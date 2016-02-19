########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

from cloudify.workflows.tasks_graph import forkjoin
from cloudify.workflows import tasks as workflow_tasks


def upgrade_node_instances(graph, node_instances, intact_nodes=None):
    processor = UpgradeProcessor(graph=graph,
                                 node_instances=node_instances,
                                 intact_nodes=intact_nodes)
    processor.upgrade()


class UpgradeProcessor(object):

    def __init__(self,
                 graph,
                 node_instances,
                 intact_nodes=None):
        self.graph = graph
        self.node_instances = node_instances
        self.intact_nodes = intact_nodes or set()

    def upgrade(self):
        self._process_node_instances(
            node_instance_subgraph_func=upgrade_node_subgraph)

    def _process_node_instances(self, node_instance_subgraph_func):
        subgraphs = {}
        for instance in self.node_instances:
            subgraphs[instance.id] = node_instance_subgraph_func(instance,
                                                                 self.graph)
        for instance in self.intact_nodes:
            subgraphs[instance.id] = self.graph.subgraph(
                'stub_{0}'.format(instance.id))

        # Create task dependencies based on node relationships
        self._add_dependencies(subgraphs=subgraphs,
                               instances=self.node_instances)

        def intact_on_dependency_added(rel, source_subgraph):
            if rel.target_node_instance in self.node_instances:
                intact_tasks = _relationship_operations(
                    rel, 'cloudify.interfaces.relationship_upgrade.establish')
                for intact_task in intact_tasks:
                    source_subgraph.add_task(intact_task)
        # Add operations for intact nodes depending on a node instance
        # belonging to node_instances
        self._add_dependencies(subgraphs=subgraphs,
                               instances=self.intact_nodes,
                               on_dependency_added=intact_on_dependency_added)
        self.graph.execute()

    def _add_dependencies(self, subgraphs, instances,
                          on_dependency_added=None):
        for instance in instances:
            for rel in instance.relationships:
                if (rel.target_node_instance in self.node_instances or
                            rel.target_node_instance in self.intact_nodes):
                    source_subgraph = subgraphs[instance.id]
                    target_subgraph = subgraphs[rel.target_id]
                    self.graph.add_dependency(source_subgraph,
                                              target_subgraph)
                    if on_dependency_added:
                        on_dependency_added(instance, rel, source_subgraph)


def set_send_node_event_on_error_handler(task, instance):
    def send_node_event_error_handler(tsk):
        instance.send_event('Ignoring task {0} failure'.format(tsk.name))
        return workflow_tasks.HandlerResult.ignore()
    task.on_failure = send_node_event_error_handler


def upgrade_node_subgraph(instance, graph):
    """This function is used to create a tasks sequence upgrading one node
    instance.

    :param instance: node instance to generate the upgrade tasks for
    """
    subgraph = graph.subgraph('upgrade_{0}'.format(instance.id))
    sequence = subgraph.sequence()
    sequence.add(
        forkjoin(instance.send_event('Pre-upgrade node')),
        instance.execute_operation('cloudify.interfaces.upgrade.preupgrade'),
        forkjoin(*_relationships_operations(
            instance,
            'cloudify.interfaces.relationship_upgrade.preconfigure'
        )),
        forkjoin(instance.set_state('upgrading'),
                 instance.send_event('Upgrading node')),
        instance.execute_operation('cloudify.interfaces.upgrade.upgrade'),
        instance.set_state('upgraded'),
        forkjoin(*_relationships_operations(
            instance,
            'cloudify.interfaces.relationship_upgrade.postconfigure'
        )),
        forkjoin(instance.set_state('postupgreade'),
                 instance.send_event('Post-upgrade node')),
        instance.execute_operation('cloudify.interfaces.upgrade.postupgrade'),
        instance.set_state('started'),
        *_relationships_operations(
            instance,
            'cloudify.interfaces.relationship_upgrade.establish'
        ))

    # If this is a host node, we need to add specific host start
    # tasks such as waiting for it to start and installing the agent
    # worker (if necessary)
    # if is_host_node(instance):
    #     sequence.add(*_host_post_start(instance))
    subgraph.on_failure = get_upgrade_subgraph_on_failure_handler(instance)
    return subgraph


def reupgrade_node_instance_subgraph(instance, graph):
    reinstall_subgraph = graph.subgraph('reinstall_{0}'.format(instance.id))
    upgrade_subgraph = upgrade_node_subgraph(instance, reinstall_subgraph)
    reinstall_sequence = reinstall_subgraph.sequence()
    reinstall_sequence.add(
        instance.send_event('Node upgrade failed. '
                            'Attempting to re-run node upgrade'),
        upgrade_subgraph)
    reinstall_subgraph.on_failure = get_upgrade_subgraph_on_failure_handler(
        instance)
    return reinstall_subgraph


def get_upgrade_subgraph_on_failure_handler(instance):
    def reinstall_subgraph_on_failure_handler(subgraph):
        graph = subgraph.graph
        for task in subgraph.tasks.itervalues():
            graph.remove_task(task)
        if not subgraph.containing_subgraph:
            result = workflow_tasks.HandlerResult.retry()
            result.retried_task = reupgrade_node_instance_subgraph(instance,
                                                                   graph)
            result.retried_task.current_retries = subgraph.current_retries + 1
        else:
            result = workflow_tasks.HandlerResult.ignore()
            subgraph.containing_subgraph.failed_task = subgraph.failed_task
            subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
        return result
    return reinstall_subgraph_on_failure_handler


def _relationships_operations(node_instance, operation):
    tasks = []
    for relationship in node_instance.relationships:
        tasks += _relationship_operations(relationship, operation)
    return tasks


def _relationship_operations(relationship, operation):
    return [relationship.execute_source_operation(operation),
            relationship.execute_target_operation(operation)]
