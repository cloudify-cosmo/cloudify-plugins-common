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

from logging import getLogger
from manager import get_node_state
from manager import update_node_state


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
    def __init__(self, capabilities=None):
        if capabilities is None:
            capabilities = {}
        self._capabilities = capabilities

    def __getitem__(self, key):
        """
        Returns the capability for the provided key by iterating through all
        dependency nodes available capabilities.
        """
        value = None
        for caps in self._capabilities:
            if key in caps:
                if value is not None:
                    raise RuntimeError(
                        "'{0}' capability ambiguity [capabilities={1}]".format(
                            key, self._capabilities))
                value = caps[key]
        return value

    def get_all(self):
        """Returns all capabilities as dict."""
        return self._capabilities


class CloudifyRelatedNode(object):
    """
    Represents the related node of a relationship.
    """
    def __init__(self, ctx):
        self._related = ctx['related']
        if 'capabilities' in ctx and self.node_id in ctx['capabilities']:
            self._runtime_properties = ctx['capabilities'][self.node_id]
        else:
            self._runtime_properties = {}

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
        """The related node's runtime properties as dict (read-only)."""
        return self._runtime_properties

    def __getitem__(self, key):
        """
        A syntactic sugar for getting related node's properties/runtime
        properties where the priority is for properties (properties
         specified in the blueprint).
        """
        if key in self.properties:
            return self.properties[key]
        return self._runtime_properties[key]

    def __contains__(self, key):
        return key in self.properties or key in self._runtime_properties


class CloudifyContext(object):
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
        if ctx is None:
            ctx = {}
        self._context = ctx
        if 'capabilities' in self._context:
            context_capabilities = self._context['capabilities']
        else:
            context_capabilities = {}
        self._capabilities = ContextCapabilities(context_capabilities)
        if 'task_name' in self._context:
            logger_name = self.task_name
        else:
            logger_name = 'plugin'
        self._logger = getLogger(logger_name)
        self._node_state = None
        self._set_started = False
        if 'related' in self._context:
            self._related = CloudifyRelatedNode(self._context)
        else:
            self._related = None

    @property
    def node_id(self):
        """The node's in context id."""
        return self._context['node_id']

    @property
    def node_name(self):
        """The node's in context name."""
        return self._context['node_name']

    @property
    def properties(self):
        """
        The node's in context properties as dict (read-only).
        These properties are the properties specified in the blueprint.
        """
        return self._context['node_properties']

    @property
    def runtime_properties(self):
        """The node's in context runtime properties as a dict (read-only).

        Runtime properties are properties set during the node's lifecycle.
        Retrieving runtime properties involves a call to Cloudify's storage.

        In order to set runtime properties for the node in context use the
        __setitem__(key, value) method (square brackets notation).
        """
        self._get_node_state_if_needed()
        return self._node_state.runtime_properties

    @property
    def blueprint_id(self):
        """The blueprint id the plugin invocation belongs to."""
        return self._context['blueprint_id']

    @property
    def deployment_id(self):
        """The deployment id the plugin invocation belongs to."""
        return self._context['deployment_id']

    @property
    def execution_id(self):
        """The workflow execution id the plugin invocation belongs to."""
        return self._context['execution_id']

    @property
    def task_id(self):
        """The plugin's task invocation unique id."""
        return self._context['task_id']

    @property
    def task_name(self):
        """The full task name of the invoked task."""
        return self._context['task_name']

    @property
    def plugin(self):
        """The plugin name of the invoked task."""
        return self._context['plugin']

    @property
    def operation(self):
        """
        The node operation name which is mapped to this task invocation.
        For example: cloudify.interfaces.lifecycle.start
        """
        return self._context['operation']

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
        return self._logger

    def is_set_started(self):
        return self._set_started

    def set_started(self):
        self._set_started = True

    def _get_node_state_if_needed(self):
        if self.node_id is None:
            raise RuntimeError('Cannot get node state - invocation is not '
                               'in a context of node')
        if self._node_state is None:
            self._node_state = get_node_state(self.node_id)

    def __getitem__(self, key):
        """
        Gets node in context's static/runtime property.

        If key is not found in node's static properties, an attempt to
        get the property from the node's runtime properties will be made
         (a call to Cloudify's storage).
        """
        if self.properties is not None and key in self.properties:
            return self.properties[key]
        self._get_node_state_if_needed()
        return self._node_state[key]

    def __setitem__(self, key, value):
        """
        Sets a runtime property for the node in context.

        New or updated properties will be saved to Cloudify's storage as soon
        as the task execution is over or if ctx.update() was
        explicitly invoked.
        """
        self._get_node_state_if_needed()
        self._node_state[key] = value

    def __contains__(self, key):
        if self.properties is not None and key in self.properties:
            return True
        self._get_node_state_if_needed()
        return key in self._node_state

    def update(self):
        """
        Stores new/updated runtime properties for the node in context in
        Cloudify's storage.

        This method should be invoked only if its necessary to immediately
        update Cloudify's storage with changes. Otherwise, the method is
        automatically invoked as soon as the task execution is over.
        """
        if self._node_state is not None:
            update_node_state(self._node_state)
            self._node_state = None
