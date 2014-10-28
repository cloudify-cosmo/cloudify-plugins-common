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


from cloudify.decorators import workflow
from cloudify.workflows.tasks_graph import forkjoin
from cloudify.workflows import tasks as workflow_tasks


@workflow
def install(ctx, **kwargs):
    """Default install workflow"""

    # switch to graph mode (operations on the context return tasks instead of
    # result instances)
    graph = ctx.graph_mode()

    # We need reference to the create event/state tasks and the started
    # task so we can later create a proper dependency between nodes and
    # their relationships. We use the below tasks as part of a single node
    # workflow, and to create the dependency (at the bottom)
    send_event_creating_tasks = {}
    set_state_creating_tasks = {}
    set_state_started_tasks = {}
    for node in ctx.nodes:
        for instance in node.instances:
            send_event_creating_tasks[instance.id] = instance.send_event(
                'Creating node')
            set_state_creating_tasks[instance.id] = instance.set_state(
                'creating')
            set_state_started_tasks[instance.id] = instance.set_state(
                'started')

    # Create node linear task sequences
    # For each node, we create a "task sequence" in which all tasks
    # added to it will be executed in a sequential manner
    for node in ctx.nodes:
        for instance in node.instances:
            sequence = graph.sequence()

            sequence.add(
                instance.set_state('initializing'),
                forkjoin(
                    set_state_creating_tasks[instance.id],
                    send_event_creating_tasks[instance.id]
                ),
                instance.execute_operation(
                    'cloudify.interfaces.lifecycle.create'),
                instance.set_state('created'),
                forkjoin(*_relationship_operations(
                    instance,
                    'cloudify.interfaces.relationship_lifecycle.preconfigure'
                )),
                forkjoin(
                    instance.set_state('configuring'),
                    instance.send_event('Configuring node')),
                instance.execute_operation(
                    'cloudify.interfaces.lifecycle.configure'),
                instance.set_state('configured'),
                forkjoin(*_relationship_operations(
                    instance,
                    'cloudify.interfaces.relationship_lifecycle.postconfigure'
                )),
                forkjoin(
                    instance.set_state('starting'),
                    instance.send_event('Starting node')),
                instance.execute_operation(
                    'cloudify.interfaces.lifecycle.start'))

            # If this is a host node, we need to add specific host start
            # tasks such as waiting for it to start and installing the agent
            # worker (if necessary)
            if _is_host_node(instance):
                sequence.add(*_host_post_start(instance))

            sequence.add(
                set_state_started_tasks[instance.id],
                forkjoin(
                    instance.execute_operation(
                        'cloudify.interfaces.monitoring.start'),
                    *_relationship_operations(
                        instance,
                        'cloudify.interfaces.relationship_lifecycle.establish'
                    )))

    # Create task dependencies based on node relationships
    # for each node, make a dependency between the create tasks (event, state)
    # and the started state task of the target
    for node in ctx.nodes:
        for instance in node.instances:
            for rel in instance.relationships:
                node_set_creating = set_state_creating_tasks[instance.id]
                node_event_creating = send_event_creating_tasks[instance.id]
                target_set_started = set_state_started_tasks[rel.target_id]
                graph.add_dependency(node_set_creating, target_set_started)
                graph.add_dependency(node_event_creating, target_set_started)

    return graph.execute()


@workflow
def uninstall(ctx, **kwargs):
    """Default uninstall workflow"""

    # switch to graph mode (operations on the context return tasks instead of
    # result instances)
    graph = ctx.graph_mode()

    set_state_stopping_tasks = {}
    set_state_deleted_tasks = {}
    stop_node_tasks = {}
    stop_monitor_tasks = {}
    delete_node_tasks = {}
    for node in ctx.nodes:
        for instance in node.instances:
            # We need reference to the set deleted state tasks and the set
            # stopping state tasks so we can later create a proper dependency
            # between nodes and their relationships. We use the below tasks as
            # part of a single node workflow, and to create the dependency
            # (at the bottom)
            set_state_stopping_tasks[instance.id] = instance.set_state(
                'stopping')
            set_state_deleted_tasks[instance.id] = instance.set_state(
                'deleted')

            # We need reference to the stop node tasks, stop monitor tasks and
            # delete node tasks as we augment them with on_failure error
            # handlers # later on
            stop_node_tasks[instance.id] = instance.execute_operation(
                'cloudify.interfaces.lifecycle.stop')
            stop_monitor_tasks[instance.id] = instance.execute_operation(
                'cloudify.interfaces.monitoring.stop')
            delete_node_tasks[instance.id] = instance.execute_operation(
                'cloudify.interfaces.lifecycle.delete')

    # Create node linear task sequences
    # For each node, we create a "task sequence" in which all tasks
    # added to it will be executed in a sequential manner
    for node in ctx.nodes:
        for instance in node.instances:
            unlink_tasks = _relationship_operations_with_targets(
                instance, 'cloudify.interfaces.relationship_lifecycle.unlink')

            sequence = graph.sequence()

            sequence.add(set_state_stopping_tasks[instance.id],
                         instance.send_event('Stopping node'))
            if _is_host_node(instance):
                sequence.add(*_host_pre_stop(instance))
            sequence.add(stop_node_tasks[instance.id],
                         instance.set_state('stopped'),
                         forkjoin(*[task for task, _ in unlink_tasks]),
                         instance.set_state('deleting'),
                         instance.send_event('Deleting node'),
                         delete_node_tasks[instance.id],
                         set_state_deleted_tasks[instance.id])

            # adding the stop monitor task not as a part of the sequence,
            # as it can happen in parallel with any other task, and is only
            # dependent on the set node state 'stopping' task
            graph.add_task(stop_monitor_tasks[instance.id])
            graph.add_dependency(stop_monitor_tasks[instance.id],
                                 set_state_stopping_tasks[instance.id])

            # augmenting the stop node, stop monitor and delete node tasks with
            # error handlers
            _set_send_node_event_on_error_handler(
                stop_node_tasks[instance.id],
                instance,
                "Error occurred while stopping node - ignoring...")
            _set_send_node_event_on_error_handler(
                stop_monitor_tasks[instance.id],
                instance,
                "Error occurred while stopping monitor - ignoring...")
            _set_send_node_event_on_error_handler(
                delete_node_tasks[instance.id],
                instance,
                "Error occurred while deleting node - ignoring...")

            for unlink_task, target_id in unlink_tasks:
                _set_send_node_event_on_error_handler(
                    unlink_task,
                    instance,
                    "Error occurred while unlinking node from node {0} - "
                    "ignoring...".format(target_id))

    # Create task dependencies based on node relationships
    # for each node, make a dependency between the target's stopping task
    # and the deleted state task of the current node
    for node in ctx.nodes:
        for instance in node.instances:
            for rel in instance.relationships:
                target_set_stopping = set_state_stopping_tasks[rel.target_id]
                node_set_deleted = set_state_deleted_tasks[instance.id]
                graph.add_dependency(target_set_stopping, node_set_deleted)

    return graph.execute()


def _set_send_node_event_on_error_handler(task, node_instance, error_message):
    def send_node_event_error_handler(tsk):
        node_instance.send_event(error_message)
        return workflow_tasks.HandlerResult.ignore()
    task.on_failure = send_node_event_error_handler


def _relationship_operations(node_instance, operation):
    tasks_with_targets = _relationship_operations_with_targets(
        node_instance, operation)
    return [task for task, _ in tasks_with_targets]


def _relationship_operations_with_targets(node_instance, operation):
    tasks = []
    for relationship in node_instance.relationships:
        tasks.append(
            (relationship.execute_source_operation(operation),
             relationship.target_id))
        tasks.append(
            (relationship.execute_target_operation(operation),
             relationship.target_id))
    return tasks


def _is_host_node(node_instance):
    return 'cloudify.nodes.Compute' in node_instance.node.type_hierarchy


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


def _host_post_start(host_node_instance):

    plugins_to_install = filter(lambda plugin: plugin['install'],
                                host_node_instance.node.plugins_to_install)

    tasks = [_wait_for_host_to_start(host_node_instance)]
    if host_node_instance.node.properties['install_agent'] is True:
        tasks += [
            host_node_instance.send_event('Installing worker'),
            host_node_instance.execute_operation(
                'cloudify.interfaces.worker_installer.install'),
            host_node_instance.execute_operation(
                'cloudify.interfaces.worker_installer.start'),
        ]
        if plugins_to_install:
            tasks += [
                host_node_instance.send_event('Installing host plugins'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.plugin_installer.install',
                    kwargs={
                        'plugins': plugins_to_install}),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.worker_installer.restart',
                    send_task_events=False)
            ]
    tasks += [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.install'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.start'),
    ]
    return tasks


def _host_pre_stop(host_node_instance):
    tasks = []
    tasks += [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.stop'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.uninstall'),
    ]
    if host_node_instance.node.properties['install_agent'] is True:
        tasks += [
            host_node_instance.send_event('Uninstalling worker'),
            host_node_instance.execute_operation(
                'cloudify.interfaces.worker_installer.stop'),
            host_node_instance.execute_operation(
                'cloudify.interfaces.worker_installer.uninstall')
        ]

    for task in tasks:
        if task.is_remote():
            _set_send_node_event_on_error_handler(
                task, host_node_instance,
                'Error occurred while uninstalling worker - ignoring...')

    return tasks


@workflow
def execute_operation(ctx, operation, operation_kwargs,
                      run_by_dependency_order, type_names, node_ids,
                      node_instance_ids, **kwargs):
    """ A generic workflow for executing arbitrary operations on nodes """

    graph = ctx.graph_mode()

    send_event_starting_tasks = {}
    send_event_done_tasks = {}

    # filtering node instances
    filtered_node_instances = []
    for node in ctx.nodes:
        if node_ids and node.id not in node_ids:
            continue
        if type_names and not next((type_name for type_name in type_names if
                                    type_name in node.type_hierarchy), None):
            continue

        for instance in node.instances:
            if node_instance_ids and instance.id not in node_instance_ids:
                continue
            filtered_node_instances.append(instance)

    # pre-preparing events tasks
    for instance in filtered_node_instances:
        start_event_message = 'Starting operation {0}'.format(operation)
        if operation_kwargs:
            start_event_message += ' (Operation parameters: {0})'.format(
                operation_kwargs)

        send_event_starting_tasks[instance.id] = \
            instance.send_event(start_event_message)
        send_event_done_tasks[instance.id] = \
            instance.send_event('Finished operation {0}'.format(operation))

    if run_by_dependency_order:
        # if run by dependency order is set, then create NOP tasks for the
        # rest of the instances. This is done to support indirect
        # dependencies, i.e. when instance A is dependent on instance B
        # which is dependent on instance C, where A and C are to be executed
        # with the operation on (i.e. they're in filtered_node_instances)
        # yet B isn't.
        # We add the NOP tasks rather than creating dependencies between A
        # and C themselves since even though it may sometimes increase the
        # number of dependency relationships in the execution graph, it also
        # ensures their number is linear to the number of relationships in
        # the deployment (e.g. consider if A and C are one out of N instances
        # of their respective nodes yet there's a single instance of B -
        # using NOP tasks we'll have 2N relationships instead of N^2).
        filtered_node_instances_ids = set(inst.id for inst in
                                          filtered_node_instances)
        for node in ctx.nodes:
            for instance in node.instances:
                if instance.id not in filtered_node_instances_ids:
                    nop_task = workflow_tasks.NOPLocalWorkflowTask(ctx)
                    send_event_starting_tasks[instance.id] = nop_task
                    send_event_done_tasks[instance.id] = nop_task
                    graph.add_task(nop_task)

    # registering actual tasks to sequences
    for instance in filtered_node_instances:
        sequence = graph.sequence()
        sequence.add(
            send_event_starting_tasks[instance.id],
            instance.execute_operation(operation, kwargs=operation_kwargs),
            send_event_done_tasks[instance.id])

    # adding tasks dependencies if required
    if run_by_dependency_order:
        for node in ctx.nodes:
            for instance in node.instances:
                for rel in instance.relationships:
                    instance_starting_task = \
                        send_event_starting_tasks[instance.id]
                    target_done_task = \
                        send_event_done_tasks.get(rel.target_id)

                    graph.add_dependency(instance_starting_task,
                                         target_done_task)

    return graph.execute()
