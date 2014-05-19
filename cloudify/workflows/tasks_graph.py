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


import time

import networkx as nx

from cloudify.workflows.events import start_event_monitor
from cloudify.workflows import tasks as tasks_api


class TaskDependencyGraph(object):

    done_states = [tasks_api.TASK_FAILED, tasks_api.TASK_SUCCEEDED]

    def __init__(self, workflow_context):
        self.ctx = workflow_context
        self.graph = nx.DiGraph()

    def add_task(self, task):
        self.ctx.logger.debug('adding task: {}'.format(task))
        self.graph.add_node(task.id, task=task)

    def get_task(self, task_id):
        data = self.graph.node.get(task_id)
        return data['task'] if data is not None else None

    # src depends on dst
    def add_dependency(self, src_task, dst_task):
        self.ctx.logger.debug('adding dependency: {} -> {}'.format(src_task,
                                                                   dst_task))
        self.graph.add_edge(src_task.id, dst_task.id)

    def sequence(self):
        return TaskSequence(self)

    def execute(self):
        start_event_monitor(self)

        while True:

            for task in self._executable_tasks():
                task.apply_async()

            for task in self._terminated_tasks():
                retry = task.handle_task_terminated()
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

            if len(self.graph.node) == 0:
                break
            else:
                time.sleep(0.1)

    def _executable_tasks(self, ):
        return (task for task in self._tasks_iter()
                if task.get_state() == tasks_api.TASK_PENDING
                and not self._task_has_dependencies(task.id))

    def _terminated_tasks(self):
        return (task for task in self._tasks_iter()
                if task.get_state() in self.done_states)

    def _task_has_dependencies(self, task_id):
        successors = self.graph.succ.get(task_id)
        return successors is not None and len(successors) > 0

    def _tasks_iter(self):
        return (data['task'] for _, data in self.graph.nodes_iter(data=True))


class TaskSequence(object):

    def __init__(self, graph):
        self.graph = graph
        self.last_fork_join_tasks = None

    def add(self, *tasks):
        """
        Adding tasks to the sequence. any iterable item in `tasks`
        will be considered a "fork-join"
        :param tasks: A list of elements where each element might be:
            1) A task, in which case, it will be added to the sequence
            2) An iterable of tasks, in which case it will be treated
               as a "fork-join" task in the sequence, i.e. all the fork-join
               tasks will depend on the last task in the sequence (could be
               fork join) and the next added task will depend on all tasks
               in this fork-join task
        """
        for fork_join_tasks in tasks:
            if fork_join_tasks is tasks_api.NOP:
                continue
            if not hasattr(fork_join_tasks, '__iter__'):
                fork_join_tasks = [fork_join_tasks]

            fork_join_tasks = [t for t in fork_join_tasks
                               if t is not tasks_api.NOP]
            for task in fork_join_tasks:
                self.graph.add_task(task)
                if self.last_fork_join_tasks is not None:
                    for last_fork_join_task in self.last_fork_join_tasks:
                        self.graph.add_dependency(task, last_fork_join_task)
            if fork_join_tasks:
                self.last_fork_join_tasks = fork_join_tasks

    def add_dependency_to_last(self, task):
        if self.last_fork_join_tasks is not None:
            for last_fork_join_task in self.last_fork_join_tasks:
                self.graph.add_dependency(task, last_fork_join_task)
