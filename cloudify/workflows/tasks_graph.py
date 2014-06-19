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

__author__ = 'dank'

import time

import networkx as nx

from cloudify.workflows.events import start_event_monitor
from cloudify.workflows import api
from cloudify.workflows import tasks as tasks_api


class TaskDependencyGraph(object):
    """A task graph builder"""

    done_states = [tasks_api.TASK_FAILED, tasks_api.TASK_SUCCEEDED]

    def __init__(self, workflow_context):
        """
        :param workflow_context: A WorkflowContext instance (used for logging)
        """
        self.ctx = workflow_context
        self.graph = nx.DiGraph()

    def add_task(self, task):
        """A a WorkflowTask to this graph

        :param task: The task
        """
        self.ctx.logger.debug('adding task: {}'.format(task))
        self.graph.add_node(task.id, task=task)

    def get_task(self, task_id):
        """Get a task instance that was inserted to this graph by its id

        :param task_id: the task id
        :return: a WorkflowTask instance for the requested task if found.
                 None, otherwise.
        """
        data = self.graph.node.get(task_id)
        return data['task'] if data is not None else None

    # src depends on dst
    def add_dependency(self, src_task, dst_task):
        """
        Add a dependency between tasks.
        The source task will only be executed after the target task terminates.
        A task may depend on several tasks, in which case it will only be
        executed after all its 'destination' tasks terminate

        :param src_task: The source task
        :param dst_task: The target task
        """

        self.ctx.logger.debug('adding dependency: {} -> {}'.format(src_task,
                                                                   dst_task))
        if not self.graph.has_node(src_task.id):
            raise RuntimeError('source task {0} is not in graph (task id: '
                               '{1})'.format(src_task, src_task.id))
        if not self.graph.has_node(dst_task.id):
            raise RuntimeError('destination task {0} is not in graph (task '
                               'id: {1})'.format(dst_task, dst_task.id))
        self.graph.add_edge(src_task.id, dst_task.id)

    def sequence(self):
        """
        :return: a new TaskSequence for this graph
        """
        return TaskSequence(self)

    def execute(self):
        """
        Start executing the graph based on tasks and dependencies between
        them.
        Calling this method will block until one of the following occurs:
        1. all tasks terminated
        2. a task failed
        3. an unhandled exception is raised
        4. the execution is cancelled

        Note: This method will return None unless the execution has been
        cancelled, in which case the return value will be
        api.EXECUTION_CANCELLED_RESULT. Callers of this method should check
        the return value and propagate the result in the latter case.
        """

        # start the celery event monitor for receiving task sent/started/
        # failed/succeeded events for remote workflow tasks
        start_event_monitor(self)

        while not self._is_execution_cancelled():

            # execute all tasks that are executable at the moment
            for task in self._executable_tasks():
                task.apply_async()

            # for each terminated task
            # 1. if it failed, call its handler. if the handler returns
            #    false, fail the workflow. otherwise continue normally.
            # 2. if it succeeded remove it and its dependencies
            #    from the graph. if its handler returned true,
            #    duplicate the task and reinsert it to the graph
            #    with its original dependents
            for task in self._terminated_tasks():
                retry = False
                if task.get_state() == tasks_api.TASK_FAILED:
                    ignore_fail = task.handle_task_failed()
                    if not ignore_fail:
                        raise RuntimeError(
                            "Workflow failed: Task failed '{}' -> {}"
                            .format(task.name, task.error))
                else:
                    retry = task.handle_task_succeeded()

                dependents = self.graph.predecessors(task.id)
                removed_edges = [(dependent, task.id)
                                 for dependent in dependents]
                self.graph.remove_edges_from(removed_edges)
                self.graph.remove_node(task.id)
                if retry:
                    new_task = task.duplicate()
                    self.add_task(new_task)
                    added_edges = [(dependent, new_task.id)
                                   for dependent in dependents]
                    self.graph.add_edges_from(added_edges)

            # no more tasks to process, time to move on
            if len(self.graph.node) == 0:
                return
            # sleep some and do it all over again
            else:
                time.sleep(0.1)

        return api.EXECUTION_CANCELLED_RESULT

    def _is_execution_cancelled(self):
        return api.has_cancel_request()

    def _executable_tasks(self, ):
        """
        A task is executable if it is in pending state
        and it has no dependencies at the moment (i.e. all of its dependencies
        already terminated)

        :return: An iterator for executable tasks
        """

        return (task for task in self._tasks_iter()
                if task.get_state() == tasks_api.TASK_PENDING
                and not self._task_has_dependencies(task.id))

    def _terminated_tasks(self):
        """
        A task is terminated if it is in 'succeeded' or 'failed' state

        :return: An iterator for terminated tasks
        """

        return (task for task in self._tasks_iter()
                if task.get_state() in self.done_states)

    def _task_has_dependencies(self, task_id):
        """
        :param task_id: The task id
        :return: Does this task have any dependencies
        """
        successors = self.graph.succ.get(task_id)
        return successors is not None and len(successors) > 0

    def _tasks_iter(self):
        return (data['task'] for _, data in self.graph.nodes_iter(data=True))


class forkjoin(object):
    """
    A simple wrapper for tasks. Used in conjunction with TaskSequence.
    Defined to make the code easier to read (instead of passing a list)
    see TaskSequence.add for more details
    """

    def __init__(self, *tasks):
        self.tasks = tasks


class TaskSequence(object):
    """
    Helper class to add tasks in a sequential manner to a task dependency
    graph
    """

    def __init__(self, graph):
        """
        :param graph: The TaskDependencyGraph instance
        """
        self.graph = graph
        self.last_fork_join_tasks = None

    def add(self, *tasks):
        """
        Add tasks to the sequence.

        :param tasks: Each task might be:
            1) A WorkflowTask instance, in which case, it will be added to the
               graph with a dependency between it and the task previously
               inserted into the sequence
            2) A forkjoin of tasks, in which case it will be treated
               as a "fork-join" task in the sequence, i.e. all the fork-join
               tasks will depend on the last task in the sequence (could be
               fork join) and the next added task will depend on all tasks
               in this fork-join task
        """
        for fork_join_tasks in tasks:
            if isinstance(fork_join_tasks, forkjoin):
                fork_join_tasks = fork_join_tasks.tasks
            else:
                fork_join_tasks = [fork_join_tasks]
            for task in fork_join_tasks:
                self.graph.add_task(task)
                if self.last_fork_join_tasks is not None:
                    # TODO: consider batch insertion of edges (see
                    # TODO: digraph.add_edges_from method)
                    for last_fork_join_task in self.last_fork_join_tasks:
                        self.graph.add_dependency(task, last_fork_join_task)
            if fork_join_tasks:
                self.last_fork_join_tasks = fork_join_tasks
