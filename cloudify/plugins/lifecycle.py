########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

import itertools
import importlib

import yaml

from cloudify import utils
from cloudify import constants
from cloudify import resources
from cloudify.workflows.tasks_graph import forkjoin
from cloudify.workflows import tasks as workflow_tasks


DEFAULT_LIFECYCLE = 'default_lifecycle.yaml'


def install_node_instances(graph, node_instances, related_nodes=None):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes)
    processor.install()


def uninstall_node_instances(graph, node_instances, related_nodes=None):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes)
    processor.uninstall()


def reinstall_node_instances(graph, node_instances, related_nodes=None):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes)
    processor.uninstall()
    processor.install()


def execute_establish_relationships(graph,
                                    node_instances,
                                    related_nodes=None,
                                    modified_relationship_ids=None):
    processor = LifecycleProcessor(
            graph=graph,
            related_nodes=node_instances,
            modified_relationship_ids=modified_relationship_ids)
    processor.install()


def execute_unlink_relationships(graph,
                                 node_instances,
                                 related_nodes=None,
                                 modified_relationship_ids=None):
    processor = LifecycleProcessor(
            graph=graph,
            related_nodes=node_instances,
            modified_relationship_ids=modified_relationship_ids)
    processor.uninstall()


class LifecycleProcessor(object):

    def __init__(self,
                 graph,
                 node_instances=None,
                 related_nodes=None,
                 modified_relationship_ids=None):
        self.graph = graph
        self.node_instances = node_instances or set()
        self.intact_nodes = related_nodes or set()
        self.modified_relationship_ids = modified_relationship_ids or {}
        self.lifecycle_config = yaml.load(resources.get(DEFAULT_LIFECYCLE))

    def install(self):
        self._process_node_instances(
            node_instance_subgraph_func=install_node_instance_subgraph,
            graph_finisher_func=self._finish_install)

    def uninstall(self):
        self._process_node_instances(
            node_instance_subgraph_func=uninstall_node_instance_subgraph,
            graph_finisher_func=self._finish_uninstall)

    def _process_node_instances(self,
                                node_instance_subgraph_func,
                                graph_finisher_func):
        subgraphs = {}
        for instance in self.node_instances:
            subgraphs[instance.id] = node_instance_subgraph_func(
                instance,
                self.graph,
                self.lifecycle_config)
        for instance in self.intact_nodes:
            subgraphs[instance.id] = self.graph.subgraph(
                'stub_{0}'.format(instance.id))

        graph_finisher_func(subgraphs)
        self.graph.execute()

    def _finish_install(self, subgraphs):
        self._finish_subgraphs(
            subgraphs=subgraphs,
            intact_op='cloudify.interfaces.relationship_lifecycle.establish',
            install=True)

    def _finish_uninstall(self, subgraphs):
        self._finish_subgraphs(
            subgraphs=subgraphs,
            intact_op='cloudify.interfaces.relationship_lifecycle.unlink',
            install=False)

    def _finish_subgraphs(self, subgraphs, intact_op, install):
        self._add_hook_dependencies(subgraphs=subgraphs, install=install)

        # Create task dependencies based on node relationships
        self._add_dependencies(subgraphs=subgraphs,
                               instances=self.node_instances,
                               install=install)

        def intact_on_dependency_added(instance, rel, source_task_sequence):
            if (rel.target_node_instance in self.node_instances or
                    rel.target_node_instance.node_id in
                    self.modified_relationship_ids.get(instance.node_id, {})):
                intact_tasks = _relationship_operations(rel, intact_op)
                for intact_task in intact_tasks:
                    if not install:
                        set_send_node_event_on_error_handler(
                            intact_task, instance)
                    source_task_sequence.add(intact_task)
        # Add operations for intact nodes depending on a node instance
        # belonging to node_instances
        self._add_dependencies(subgraphs=subgraphs,
                               instances=self.intact_nodes,
                               install=install,
                               on_dependency_added=intact_on_dependency_added)

    def _add_hook_dependencies(self, subgraphs, install):
        suffix = 'install' if install else 'uninstall'
        for prefix in ['before', 'after']:
            hook_name = '{0}_{1}'.format(prefix, suffix)
            subgraph_func_name = self.lifecycle_config.get(hook_name)
            if subgraph_func_name:
                split = subgraph_func_name.split('.')
                module = split[:-1]
                func = split[-1]
                module = importlib.import_module(module)
                func = getattr(module, func)
                subgraph = self.graph.subgraph(hook_name)
                func(subgraph)
                for node_instance_subgraph in subgraphs:
                    if prefix == 'before':
                        self.graph.add_dependency(node_instance_subgraph,
                                                  subgraph)
                    else:
                        self.graph.add_dependency(subgraph,
                                                  node_instance_subgraph)

    def _add_dependencies(self, subgraphs, instances, install,
                          on_dependency_added=None):
        subgraph_sequences = dict(
            (instance_id, subgraph.sequence())
            for instance_id, subgraph in subgraphs.items())
        for instance in instances:
            relationships = list(instance.relationships)
            if not install:
                relationships = reversed(relationships)
            for rel in relationships:
                if (rel.target_node_instance in self.node_instances or
                        rel.target_node_instance in self.intact_nodes):
                    source_subgraph = subgraphs[instance.id]
                    target_subgraph = subgraphs[rel.target_id]
                    if install:
                        self.graph.add_dependency(source_subgraph,
                                                  target_subgraph)
                    else:
                        self.graph.add_dependency(target_subgraph,
                                                  source_subgraph)
                    if on_dependency_added:
                        task_sequence = subgraph_sequences[instance.id]
                        on_dependency_added(instance, rel, task_sequence)


def set_send_node_event_on_error_handler(task, instance):
    def send_node_event_error_handler(tsk):
        instance.send_event('Ignoring task {0} failure'.format(tsk.name))
        return workflow_tasks.HandlerResult.ignore()
    task.on_failure = send_node_event_error_handler


def install_node_instance_subgraph(instance, graph, config):
    """This function is used to create a tasks sequence installing one node
    instance.
    Considering the order of tasks executions, it enforces the proper
    dependencies only in context of this particular node instance.

    :param instance: node instance to generate the installation tasks for
    """
    subgraph = graph.subgraph('install_{0}'.format(instance.id))
    _build_node_instance_subgraph_from_config(
        subgraph=subgraph,
        instance=instance,
        node_instance_config=config['node_instance_install'])
    subgraph.on_failure = get_install_subgraph_on_failure_handler(instance,
                                                                  config)
    return subgraph


def uninstall_node_instance_subgraph(instance, graph, config):
    subgraph = graph.subgraph('uninstall_{0}'.format(instance.id))
    _build_node_instance_subgraph_from_config(
        subgraph=subgraph,
        instance=instance,
        node_instance_config=config['node_instance_uninstall'])

    def set_ignore_handlers(_subgraph):
        for task in _subgraph.tasks.itervalues():
            if task.is_subgraph:
                set_ignore_handlers(task)
            else:
                set_send_node_event_on_error_handler(task, instance)
    set_ignore_handlers(subgraph)
    return subgraph


def _build_node_instance_subgraph_from_config(subgraph,
                                              instance,
                                              node_instance_config):
    def raise_unhandled(entry):
        raise ValueError('Unhandled entry: {0}'.format(entry))

    def handle_entry(entry):
        if isinstance(entry, dict):
            handler = handle_dict
        elif isinstance(entry, list):
            handler = handle_list
        elif isinstance(entry, basestring):
            handler = handle_string
        else:
            raise raise_unhandled(entry)
        return handler(entry)

    def handle_dict(entry):
        if 'state' in entry:
            return instance.set_state(entry['state'])
        elif 'event' in entry:
            return instance.send_event(entry['event'])
        elif 'operation' in entry:
            return instance.execute_operation(entry['operation'])
        elif 'relationship_operation' in entry:
            return _relationships_operations(
                graph=subgraph,
                node_instance=instance,
                operation=entry['relationship_operation'],
                reverse=entry.get('reverse'))
        else:
            raise_unhandled(entry)

    def handle_list(entry):
        tasks = []
        for t in entry:
            result = handle_entry(t)
            if isinstance(result, list):
                raise ValueError('Unsupported entry in list: {0}'.format(
                    entry))
            elif isinstance(result, forkjoin):
                tasks += result.tasks
            else:
                tasks.append(result)
        return forkjoin(*tasks)

    def handle_string(entry):
        if entry == 'compute_install':
            if is_host_node(instance):
                return _host_post_start(instance)
        elif entry == 'compute_uninstall':
            if is_host_node(instance):
                return _host_pre_stop(instance)
        else:
            raise_unhandled(entry)

    sequence = subgraph.sequence()
    for e in node_instance_config:
        result = handle_entry(e)
        if not result:
            continue
        if not isinstance(result, list):
            result = [result]
        sequence.add(*result)


def reinstall_node_instance_subgraph(instance, graph, config):
    reinstall_subgraph = graph.subgraph('reinstall_{0}'.format(instance.id))
    uninstall_subgraph = uninstall_node_instance_subgraph(instance,
                                                          reinstall_subgraph,
                                                          config)
    install_subgraph = install_node_instance_subgraph(instance,
                                                      reinstall_subgraph,
                                                      config)
    reinstall_sequence = reinstall_subgraph.sequence()
    reinstall_sequence.add(
        instance.send_event('Node lifecycle failed. '
                            'Attempting to re-run node lifecycle'),
        uninstall_subgraph,
        install_subgraph)
    reinstall_subgraph.on_failure = get_install_subgraph_on_failure_handler(
        instance, config)
    return reinstall_subgraph


def get_install_subgraph_on_failure_handler(instance, config):
    def install_subgraph_on_failure_handler(subgraph):
        graph = subgraph.graph
        for task in subgraph.tasks.itervalues():
            subgraph.remove_task(task)
        if not subgraph.containing_subgraph:
            result = workflow_tasks.HandlerResult.retry()
            result.retried_task = reinstall_node_instance_subgraph(
                instance, graph, config)
            result.retried_task.current_retries = subgraph.current_retries + 1
        else:
            result = workflow_tasks.HandlerResult.ignore()
            subgraph.containing_subgraph.failed_task = subgraph.failed_task
            subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
        return result
    return install_subgraph_on_failure_handler


def _relationships_operations(graph,
                              node_instance,
                              operation,
                              reverse):
    def on_failure(subgraph):
        for task in subgraph.tasks.itervalues():
            subgraph.remove_task(task)
        handler_result = workflow_tasks.HandlerResult.ignore()
        subgraph.containing_subgraph.failed_task = subgraph.failed_task
        subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
        return handler_result
    result = graph.subgraph('{0}_subgraph'.format(operation))
    result.on_failure = on_failure
    sequence = result.sequence()
    relationships_groups = itertools.groupby(
        node_instance.relationships,
        key=lambda r: r.relationship.target_id)
    tasks = []
    for _, relationship_group in relationships_groups:
        group_tasks = []
        for relationship in relationship_group:
            group_tasks += _relationship_operations(relationship, operation)
        tasks.append(forkjoin(*group_tasks))
    if reverse:
        tasks = reversed(tasks)
    sequence.add(*tasks)
    return result


def _relationship_operations(relationship, operation):
    return [relationship.execute_source_operation(operation),
            relationship.execute_target_operation(operation)]


def is_host_node(node_instance):
    return constants.COMPUTE_NODE_TYPE in node_instance.node.type_hierarchy


def _wait_for_host_to_start(host_node_instance):
    task = host_node_instance.execute_operation(
        'cloudify.interfaces.host.get_state')

    # handler returns True if if get_state returns False,
    # this means, that get_state will be re-executed until
    # get_state returns True
    def node_get_state_handler(tsk):
        host_started = tsk.async_result.get()
        if host_started:
            return workflow_tasks.HandlerResult.cont()
        else:
            return workflow_tasks.HandlerResult.retry(
                ignore_total_retries=True)
    if not task.is_nop():
        task.on_success = node_get_state_handler
    return task


def prepare_running_agent(host_node_instance):
    tasks = []
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)

    plugins_to_install = filter(lambda plugin: plugin['install'],
                                host_node_instance.node.plugins_to_install)
    if (plugins_to_install and
            install_method != constants.AGENT_INSTALL_METHOD_NONE):
        node_operations = host_node_instance.node.operations
        tasks += [host_node_instance.send_event('Installing plugins')]
        if 'cloudify.interfaces.plugin_installer.install' in \
                node_operations:
            # 3.2 Compute Node
            tasks += [host_node_instance.execute_operation(
                'cloudify.interfaces.plugin_installer.install',
                kwargs={'plugins': plugins_to_install})
            ]
        else:
            tasks += [host_node_instance.execute_operation(
                'cloudify.interfaces.cloudify_agent.install_plugins',
                kwargs={'plugins': plugins_to_install})
            ]

    tasks += [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.install'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.start'),
    ]
    return tasks


def _host_post_start(host_node_instance):
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    tasks = [_wait_for_host_to_start(host_node_instance)]
    if install_method != constants.AGENT_INSTALL_METHOD_NONE:
        node_operations = host_node_instance.node.operations
        if 'cloudify.interfaces.worker_installer.install' in node_operations:
            # 3.2 Compute Node
            tasks += [
                host_node_instance.send_event('Installing Agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.worker_installer.install'),
                host_node_instance.send_event('Starting Agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.worker_installer.start')
            ]
        else:
            tasks += [
                host_node_instance.send_event('Creating Agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.create'),
                host_node_instance.send_event('Configuring Agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.configure'),
                host_node_instance.send_event('Starting Agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.start')
            ]

    tasks.extend(prepare_running_agent(host_node_instance))
    return tasks


def _host_pre_stop(host_node_instance):
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    tasks = []
    tasks += [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.stop'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.uninstall'),
    ]
    if install_method != constants.AGENT_INSTALL_METHOD_NONE:
        tasks.append(host_node_instance.send_event('Stopping agent'))
        if install_method in constants.AGENT_INSTALL_METHODS_SCRIPTS:
            # this option is only available since 3.3 so no need to
            # handle 3.2 version here.
            tasks += [
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.stop_amqp'),
                host_node_instance.send_event('Deleting agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.delete')
            ]
        else:
            node_operations = host_node_instance.node.operations
            if 'cloudify.interfaces.worker_installer.stop' in node_operations:
                tasks += [
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.worker_installer.stop'),
                    host_node_instance.send_event('Deleting agent'),
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.worker_installer.uninstall')
                ]
            else:
                tasks += [
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.cloudify_agent.stop'),
                    host_node_instance.send_event('Deleting agent'),
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.cloudify_agent.delete')
                ]
    return tasks
