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


import testtools

from cloudify import decorators
from cloudify.workflows import tasks

from cloudify.test_utils import workflow_test


@decorators.operation
def operation(ctx, arg, total_retries=0, **_):
    runtime_properties = ctx.instance.runtime_properties
    invocations = runtime_properties.get('invocations', [])
    invocations.append(arg)
    ctx.instance.runtime_properties['invocations'] = invocations

    current_retries = runtime_properties.get('current_retries', {})
    invocation_current_retries = current_retries.get(arg, 0)
    if invocation_current_retries < total_retries:
        current_retries[arg] = invocation_current_retries + 1
        runtime_properties['current_retries'] = current_retries
        return ctx.operation.retry()


@decorators.workflow
def workflow(ctx, test, **_):
    instance = next(next(ctx.nodes).instances)
    graph = ctx.graph_mode()
    tests = {
        'subgraph': _test_subgraph,
        'nested_subgraph': _test_nested_subgraph,
        'empty_subgraph': _test_empty_subgrap,
        'task_in_subgraph_retry': _test_task_in_subgraph_retry,
        'subgraph_retry': _test_subgraph_retry,
        'subgraph_retry_failure': _test_subgraph_retry_failure,
        'task_in_two_subgraphs': _test_task_in_two_subgraphs
    }
    tests[test](ctx, graph, instance)
    graph.execute()


def _test_subgraph(ctx, graph, instance):
    seq = graph.sequence()
    for i in range(2):
        subgraph = graph.subgraph('sub{0}'.format(i))
        subseq = subgraph.sequence()
        for j in range(2):
            subseq.add(instance.execute_operation(
                'interface.operation',
                kwargs={'arg': (i, j)}))
        seq.add(subgraph)


def _test_nested_subgraph(ctx, graph, instance):
    seq = graph.sequence()
    for i in range(2):
        subgraph = graph.subgraph('sub{0}'.format(i))
        subseq = subgraph.sequence()
        for j in range(2):
            subsubgraph = subgraph.subgraph('subsub{0}_{1}'.format(i, j))
            subsubseq = subsubgraph.sequence()
            for k in range(2):
                subsubseq.add(instance.execute_operation(
                    'interface.operation',
                    kwargs={'arg': (i, j, k)}))
            subseq.add(subsubgraph)
        seq.add(subgraph)


def _test_empty_subgrap(ctx, graph, instance):
    graph.subgraph('empty')


def _test_task_in_subgraph_retry(ctx, graph, instance):
    seq = graph.sequence()
    for i in range(2):
        subgraph = graph.subgraph('sub{0}'.format(i))
        subseq = subgraph.sequence()
        for j in range(2):
            subseq.add(instance.execute_operation(
                'interface.operation',
                kwargs={'arg': (i, j),
                        'total_retries': 1}))
        seq.add(subgraph)


def _test_subgraph_retry(ctx, graph, instance):

    def build_graph(total_retries):
        result = graph.subgraph('retried')
        result.add_task(instance.execute_operation(
            'interface.operation', kwargs={'arg': '',
                                           'total_retries': total_retries}))
        return result

    subgraph = build_graph(total_retries=2)

    def retry_handler(subgraph2):
        if subgraph2.failed_task.name != ('cloudify.tests.test_task_subgraph.'
                                          'operation'):
            return tasks.HandlerResult.fail()
        result = tasks.HandlerResult.retry()
        result.retried_task = build_graph(total_retries=0)
        result.retried_task.current_retries = subgraph2.current_retries + 1
        return result
    subgraph.on_failure = retry_handler


def _test_subgraph_retry_failure(ctx, graph, instance):

    def build_graph():
        result = graph.subgraph('retried')
        result.add_task(instance.execute_operation(
            'interface.operation', kwargs={'arg': '',
                                           'total_retries': 20}))
        return result

    subgraph = build_graph()

    def retry_handler(subgraph2):
        result = tasks.HandlerResult.retry()
        result.retried_task = build_graph()
        result.retried_task.on_failure = retry_handler
        result.retried_task.current_retries = subgraph2.current_retries + 1
        return result
    subgraph.on_failure = retry_handler


def _test_task_in_two_subgraphs(ctx, graph, instance):
    sub1 = graph.subgraph('sub1')
    sub2 = graph.subgraph('sub2')
    task = instance.execute_operation('interface.operation')
    sub1.add_task(task)
    sub2.add_task(task)


class TaskSubgraphWorkflowTests(testtools.TestCase):

    @workflow_test('resources/blueprints/test-task-subgraph-blueprint.yaml')
    def setUp(self, env=None):
        super(TaskSubgraphWorkflowTests, self).setUp()
        self.env = env

    def _run(self, test, subgraph_retries=0):
        self.env.execute('workflow',
                         parameters={'test': test},
                         task_retries=1,
                         task_retry_interval=0,
                         subgraph_retries=subgraph_retries)

    @property
    def invocations(self):
        return self.env.storage.get_node_instances()[0].runtime_properties[
            'invocations']

    def test_task_subgraph(self):
        self._run('subgraph')
        invocations = self.invocations
        self.assertEqual(len(invocations), 4)
        self.assertEqual(invocations, sorted(invocations))

    def test_nested_task_subgraph(self):
        self._run('nested_subgraph')
        invocations = self.invocations
        self.assertEqual(len(invocations), 8)
        self.assertEqual(invocations, sorted(invocations))

    def test_empty_subgraph(self):
        self._run('empty_subgraph')

    def test_task_in_subgraph_retry(self):
        self._run('task_in_subgraph_retry')
        invocations = self.invocations
        self.assertEqual(len(invocations), 8)
        self.assertEqual(invocations, sorted(invocations))
        for i in range(4):
            self.assertEqual(invocations[2*i], invocations[2*i+1])

    def test_subgraph_retry_sanity(self):
        self.assertRaises(RuntimeError,
                          self._run, 'subgraph_retry', subgraph_retries=0)
        self.assertEqual(len(self.invocations), 2)

    def test_subgraph_retry(self):
        self._run('subgraph_retry', subgraph_retries=1)
        self.assertEqual(len(self.invocations), 3)

    def test_subgraph_retry_failure(self):
        self.assertRaises(RuntimeError,
                          self._run, 'subgraph_retry_failure',
                          subgraph_retries=2)
        self.assertEqual(len(self.invocations), 6)

    def test_invalid_task_in_two_subgraphs(self):
        self.assertRaises(RuntimeError,
                          self._run, 'task_in_two_subgraphs')
