########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

__author__ = 'idanmo'


from manager import get_node_instance
from manager import update_node_instance
from manager import get_blueprint_resource
from manager import download_blueprint_resource
from manager import get_provider_context
from manager import get_bootstrap_context
from logs import CloudifyPluginLoggingHandler
from logs import init_cloudify_logger
from logs import send_plugin_event


class ContextCapabilities(object):
    """
    Represents dependency nodes capabilities.

    Capabilities are actually dependency nodes runtime properties.
    For example:
        In a case where a 'db' node is contained in a 'vm' node,
         The 'vm' node can publish its ip address using ctx['ip'] = ip_addr
         in its plugins invocations.
         In order for the 'db' node to consume the 'vm' node's ip, capabilities
         would be used on 'db' node plugins invocations:
            ip_addr = ctx.capabilities['ip']
        In a case where it is needed to iterate through all available
         capabilities, the following method should be used:
            all_caps = ctx.capabilities.get_all()
            Where the returned value is a dict of node ids as keys and their
            runtime properties as values.
    """
    def __init__(self, relationships=None):
        self._relationships = relationships or []
        self._relationship_runtimes = None

    def _find_item(self, key):
        """
        Returns the capability for the provided key by iterating through all
        dependency nodes available capabilities.
        """
        ls = [caps for caps in self._capabilities.values() if key in caps]
        if len(ls) == 0:
            return False, None
        if len(ls) > 1:
            raise RuntimeError(
                "'{0}' capability ambiguity [capabilities={1}]".format(
                    key, self._capabilities))
        return True, ls[0][key]

    def __getitem__(self, key):
        found, value = self._find_item(key)
        if not found:
            raise KeyError(
                "capability '{0}' not found [capabilities={1}]".format(
                    key, self._capabilities))
        return value

    def __contains__(self, key):
        found, _ = self._find_item(key)
        return found

    def get_all(self):
        """Returns all capabilities as dict."""
        return self._capabilities

    def __str__(self):
        return ('<' + self.__class__.__name__ + ' ' +
                str(self._capabilities) + '>')

    @property
    def _capabilities(self):
        if self._relationship_runtimes is None:
            self._relationship_runtimes = {rel_id: get_node_instance(rel_id)
                                           for rel_id in self._relationships}
        return self._relationship_runtimes


class CommonContextOperations(object):

    def _get_node_instance_if_needed(self):
        if self.node_id is None:
            raise RuntimeError('Cannot get node state - invocation is not '
                               'in a context of node')
        if self._node_instance is None:
            self._node_instance = get_node_instance(self.node_id)


class CloudifyRelatedNode(CommonContextOperations):
    """
    Represents the related node of a relationship.
    """
    def __init__(self, ctx):
        self._related = ctx['related']
        self._node_instance = None

    @property
    def node_id(self):
        """The related node's id."""
        return self._related['node_id']

    @property
    def properties(self):
        """The related node's properties as dict (read-only)."""
        return self._related['node_properties']

    @property
    def runtime_properties(self):
        """The related node's in context runtime properties as a dict
        (read-only).

        Runtime properties are properties set during the node's lifecycle.
        Retrieving runtime properties involves a call to Cloudify's storage.
        """
        self._get_node_instance_if_needed()
        return self._node_instance.runtime_properties

    def __getitem__(self, key):
        """
        A syntactic sugar for getting related node's properties/runtime
        properties where the priority is for properties (properties
         specified in the blueprint).
        """
        if key in self.properties:
            return self.properties[key]
        return self.runtime_properties[key]

    def __contains__(self, key):
        return key in self.properties or key in self.runtime_properties


class BootstrapContext(object):

    class CloudifyAgent(object):

        def __init__(self, cloudify_agent):
            self._cloudify_agent = cloudify_agent

        @property
        def min_workers(self):
            return self._cloudify_agent.get('min_workers')

        @property
        def max_workers(self):
            return self._cloudify_agent.get('min_workers')

        @property
        def user(self):
            return self._cloudify_agent.get('user')

        @property
        def remote_execution_port(self):
            return self._cloudify_agent.get('remote_execution_port')

        @property
        def agent_key_path(self):
            return self._cloudify_agent.get('agent_key_path')

    def __init__(self, bootstrap_context):
        self._bootstrap_context = bootstrap_context

        cloudify_agent = bootstrap_context.get('cloudify_agent', {})
        self._cloudify_agent = self.CloudifyAgent(cloudify_agent)

    @property
    def cloudify_agent(self):
        return self._cloudify_agent


class CloudifyContext(CommonContextOperations):
    """
    A context object passed to plugins tasks invocations.
    Using the context object, plugin writers can:
        - Get node in context information
        - Update node runtime properties.
        - Use a context aware logger.
        - Get related node info (relationships).
        and more...
    """

    def __init__(self, ctx=None):
        self._context = ctx or {}
        context_capabilities = self._context.get('relationships')
        self._capabilities = ContextCapabilities(context_capabilities)
        self._logger = None
        self._node_instance = None
        self._node_properties = \
            ImmutableProperties(self._context.get('node_properties') or {})
        if 'related' in self._context:
            self._related = CloudifyRelatedNode(self._context)
        else:
            self._related = None
        self._provider_contexts = {}
        self._bootstrap_context = None

    @property
    def node_id(self):
        """The node's in context id."""
        return self._context.get('node_id')

    @property
    def node_name(self):
        """The node's in context name."""
        return self._context.get('node_name')

    @property
    def properties(self):
        """
        The node's in context properties as dict (read-only).
        These properties are the properties specified in the blueprint.
        """
        return self._node_properties

    @property
    def runtime_properties(self):
        """The node's in context runtime properties as a dict (read-only).

        Runtime properties are properties set during the node's lifecycle.
        Retrieving runtime properties involves a call to Cloudify's storage.

        In order to set runtime properties for the node in context use the
        __setitem__(key, value) method (square brackets notation).
        """
        self._get_node_instance_if_needed()
        return self._node_instance.runtime_properties

    @property
    def node_state(self):
        """The node's state."""
        self._get_node_instance_if_needed()
        return self._node_instance.state

    @property
    def blueprint_id(self):
        """The blueprint id the plugin invocation belongs to."""
        return self._context.get('blueprint_id')

    @property
    def deployment_id(self):
        """The deployment id the plugin invocation belongs to."""
        return self._context.get('deployment_id')

    @property
    def execution_id(self):
        """
        The workflow execution id the plugin invocation was requested from.
        This is a unique value which identifies a specific workflow execution.
        """
        return self._context.get('execution_id')

    @property
    def workflow_id(self):
        """
        The workflow id the plugin invocation was requested from.
        For example:
            'install', 'uninstall' etc...
        """
        return self._context.get('workflow_id')

    @property
    def task_id(self):
        """The plugin's task invocation unique id."""
        return self._context.get('task_id')

    @property
    def task_name(self):
        """The full task name of the invoked task."""
        return self._context.get('task_name')

    @property
    def task_target(self):
        """The task target (RabbitMQ queue name)."""
        return self._context.get('task_target')

    @property
    def plugin(self):
        """The plugin name of the invoked task."""
        return self._context.get('plugin')

    @property
    def operation(self):
        """
        The node operation name which is mapped to this task invocation.
        For example: cloudify.interfaces.lifecycle.start
        """
        return self._context.get('operation')

    @property
    def capabilities(self):
        """
        Capabilities of nodes this node depends.
        The capabilities are actually dependency nodes runtime properties.

        For example:

            - Getting a specific capability:
                conn_str = ctx.capabilities['connection_string']
                This actually attempts to locate the provided key in
                 ctx.capabilities.get_all() (described below).

            - Getting all capabilities:
                all_caps = ctx.capabilities.get_all()
                The result is a dict of node ids as keys and the values are
                the dependency node's runtime properties.

        """
        return self._capabilities

    @property
    def related(self):
        """
        The related node in a relationship.

        When using relationship interfaces, if relationship hook is executed
        at source node, the node in context is the source node and the related
        node is the target node.
        If relationship hook is executed at target
        node, the node in context is the target node and the related node is
        the source node.

        Returns:
            CloudifyRelatedNode instance.
        """
        return self._related

    @property
    def logger(self):
        """
        A Cloudify context aware logger.

        Use this logger in order to index logged messages in ElasticSearch
        using logstash.
        """
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    @property
    def bootstrap_context(self):
        """
        System context provided during the bootstrap process
        """
        if self._bootstrap_context is None:
            context = get_bootstrap_context()
            self._bootstrap_context = BootstrapContext(context)
        return self._bootstrap_context

    def send_event(self, event):
        """
        Send an event to rabbitmq

        :param event: the event message
        """
        send_plugin_event(ctx=self, message=event)

    def get_provider_context(self, name):
        """
        Provider context provided during the bootstrap process
        """
        if self._provider_contexts.get(name) is None:
            provider_context = get_provider_context(name)
            self._provider_contexts[name] = provider_context
        return self._provider_contexts.get(name)

    def _verify_node_in_context(self):
        if self.node_id is None:
            raise RuntimeError('Invocation requires a node in context')

    def __getitem__(self, key):
        """
        Gets node in context's static/runtime property.

        If key is not found in node's static properties, an attempt to
        get the property from the node's runtime properties will be made
         (a call to Cloudify's storage).
        """
        if self.properties is not None and key in self.properties:
            return self.properties[key]
        self._get_node_instance_if_needed()
        return self._node_instance[key]

    def __setitem__(self, key, value):
        """
        Sets a runtime property for the node in context.

        New or updated properties will be saved to Cloudify's storage as soon
        as the task execution is over or if ctx.update() was
        explicitly invoked.
        """
        self._get_node_instance_if_needed()
        self._node_instance[key] = value

    def __contains__(self, key):
        if self.properties is not None and key in self.properties:
            return True
        self._get_node_instance_if_needed()
        return key in self._node_instance

    def get_resource(self, resource_path):
        """
        Retrieves a resource bundled with the blueprint.

        Parameters:
            resource_path - the path to the resource. Note that this path is
            relative to the blueprint file which was uploaded.

        Returns:
            The resource's content.

        """
        return get_blueprint_resource(self.blueprint_id, resource_path)

    def download_resource(self, resource_path, target_path=None):
        """
        Retrieves a resource bundled with the blueprint and saves it under a
        local file.

        Parameters:
            resource_path - the path to the resource. Note that this path is
            relative to the blueprint file which was uploaded.

            target_path - optional local path (including filename) to store
            the resource at on the local file system. If missing, the location
            will be a tempfile with a generated name.

        Returns:
            The path to the resource on the local file system (identical to
            target_path parameter if used).

            raises an cloudify.exceptions.HttpException
            on any kind of Http Error.

            raises an IOError if the resource
            failed to be written to the local file system.

        """
        return download_blueprint_resource(self.blueprint_id, resource_path,
                                           self.logger, target_path)

    def update(self):
        """
        Stores new/updated runtime properties for the node in context in
        Cloudify's storage.

        This method should be invoked only if its necessary to immediately
        update Cloudify's storage with changes. Otherwise, the method is
        automatically invoked as soon as the task execution is over.
        """
        if self._node_instance is not None and self._node_instance.dirty:
            update_node_instance(self._node_instance)
            self._node_instance = None

    def _init_cloudify_logger(self):
        logger_name = self.task_name if self.task_name is not None \
            else 'cloudify_plugin'
        handler = CloudifyPluginLoggingHandler(self)
        return init_cloudify_logger(handler, logger_name)

    def __str__(self):
        attrs = ('node_id', 'properties', 'runtime_properties', 'capabilities')
        info = ' '.join(["{0}={1}".format(a, getattr(self, a)) for a in attrs])
        return '<' + self.__class__.__name__ + ' ' + info + '>'


class ImmutableProperties(dict):
    """
    Of course this is not actually immutable, but it is good enough to provide
    an API that will tell you you're doing something wrong if you try updating
    the static node properties in the normal way.
    """

    def __setitem__(self, key, value):
        raise RuntimeError('Cannot override read only properties')
