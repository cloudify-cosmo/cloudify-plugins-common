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
from cloudify import exceptions
from cloudify import constants
from cloudify import ctx

from cloudify.test_utils import workflow_test


invocations = []


@decorators.operation
def operation(descriptor, **_):
    current_op = ctx.operation.name.split('.')[-1]
    if ctx.type == constants.NODE_INSTANCE:
        node = ctx.node
        instance = ctx.instance
    else:
        node = ctx.source.node
        instance = ctx.target.instance
    runtime_properties = instance.runtime_properties
    invocations.append((node.id, current_op))

    state = runtime_properties.get('state', {})
    if node.id not in state:
        state[node.id] = {}
    node_state = state[node.id]
    if current_op not in node_state:
        node_state[current_op] = 0
    op_state = node_state[current_op]

    node_descriptor = descriptor.get(node.id, {})
    op_descriptor = node_descriptor.get(current_op, 0)

    if op_state < op_descriptor:
        ctx.operation.retry()
        op_state += 1
        node_state[current_op] = op_state
        runtime_properties['state'] = state


@decorators.operation
def operation_failing_stop(*_, **__):
    invocations.append((ctx.node.id, ctx.operation.name.split('.')[-1]))
    raise exceptions.NonRecoverableError('')


@decorators.operation
def operation_delete(*_, **__):
    invocations.append((ctx.node.id, ctx.operation.name.split('.')[-1]))


def inputs(node, op, count):
    return {'descriptor': {node: {op: count}}}


class TaskLifecycleRetryTests(testtools.TestCase):

    blueprint_path = 'resources/blueprints/test-lifecycle-retry-blueprint.yaml'
    non_ignore_blueprint = \
        'resources/blueprints/' \
        'test-uninstall-ignore-failure-parameter-blueprint.yaml'

    def setUp(self):
        super(TaskLifecycleRetryTests, self).setUp()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        global invocations
        invocations = []

    def _run(self,
             env,
             subgraph_retries=0,
             workflow='install',
             parameters=None):
        env.execute(workflow,
                    parameters=parameters,
                    task_retries=1,
                    task_retry_interval=0,
                    subgraph_retries=subgraph_retries)

    @workflow_test(blueprint_path, inputs=inputs('node1', 'configure', 2))
    def test_retry_lifecycle(self, env):
        self._run(env, subgraph_retries=1)
        self.assertEqual(invocations, [
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
            ('node1', 'stop'),
            ('node1', 'delete'),
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'start'),
            ('node2', 'create'),
            ('node2', 'preconfigure'),
            ('node2', 'configure'),
            ('node2', 'postconfigure'),
            ('node2', 'start'),
            ('node2', 'establish'),
        ])

    @workflow_test(blueprint_path, inputs=inputs('node2', 'postconfigure', 2))
    def test_retry_lifecycle_2(self, env):
        self._run(env, subgraph_retries=1)
        self.assertEqual(invocations, [
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'start'),
            ('node2', 'create'),
            ('node2', 'preconfigure'),
            ('node2', 'configure'),
            ('node2', 'postconfigure'),
            ('node2', 'postconfigure'),
            ('node2', 'stop'),
            ('node2', 'unlink'),
            ('node2', 'delete'),
            ('node2', 'create'),
            ('node2', 'preconfigure'),
            ('node2', 'configure'),
            ('node2', 'postconfigure'),
            ('node2', 'start'),
            ('node2', 'establish'),
        ])

    @workflow_test(blueprint_path, inputs=inputs('node1', 'configure', 4))
    def test_retry_lifecycle3(self, env):
        self._run(env, subgraph_retries=2)
        self.assertEqual(invocations, [
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
            ('node1', 'stop'),
            ('node1', 'delete'),
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
            ('node1', 'stop'),
            ('node1', 'delete'),
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'start'),
            ('node2', 'create'),
            ('node2', 'preconfigure'),
            ('node2', 'configure'),
            ('node2', 'postconfigure'),
            ('node2', 'start'),
            ('node2', 'establish'),
        ])

    @workflow_test(blueprint_path, inputs=inputs('node1', 'configure', 4))
    def test_retry_lifecycle_failure(self, env):
        e = self.assertRaises(RuntimeError, self._run, env, subgraph_retries=1)
        self.assertIn('test_lifecycle_retry.operation', str(e))
        self.assertEqual(invocations, [
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
            ('node1', 'stop'),
            ('node1', 'delete'),
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
        ])

    @workflow_test(blueprint_path, inputs=inputs('node1', 'configure', 6))
    def test_retry_lifecycle_failure2(self, env):
        e = self.assertRaises(RuntimeError, self._run, env, subgraph_retries=2)
        self.assertIn('test_lifecycle_retry.operation', str(e))
        self.assertEqual(invocations, [
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
            ('node1', 'stop'),
            ('node1', 'delete'),
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
            ('node1', 'stop'),
            ('node1', 'delete'),
            ('node1', 'create'),
            ('node1', 'configure'),
            ('node1', 'configure'),
        ])

    @workflow_test(blueprint_path, inputs=inputs('node2', 'stop', 6))
    def test_retry_lifecycle_in_uninstall(self, env):
        e = self.assertRaises(RuntimeError, self._run, env,
                              subgraph_retries=0, workflow='uninstall')
        self.assertIn('test_lifecycle_retry.operation', str(e))
        self.assertEqual(invocations, [
            ('node2', 'stop'),
            ('node2', 'stop'),
        ])

    @workflow_test(blueprint_path, inputs=inputs('node2', 'stop', 6))
    def test_retry_lifecycle_in_uninstall_2(self, env):
        parameters = {
            'ignore_failure': True
        }
        e = self.assertRaises(RuntimeError, self._run, env, subgraph_retries=1,
                              workflow='uninstall', parameters=parameters)
        self.assertIn('test_lifecycle_retry.operation', str(e))
        self.assertEqual(invocations, [
            ('node2', 'stop'),
            ('node2', 'stop'),
        ])

    @workflow_test(non_ignore_blueprint)
    def test_retry_lifecycle_in_uninstall_ignore_failure_false(self, env):
        parameters = {
            'ignore_failure': False
        }
        e = self.assertRaises(RuntimeError, self._run, env, subgraph_retries=0,
                              workflow='uninstall', parameters=parameters)
        self.assertIn('test_lifecycle_retry.operation_failing_stop', str(e))
        self.assertEqual(invocations, [('node1', 'stop')])

    @workflow_test(non_ignore_blueprint)
    def test_retry_lifecycle_in_uninstall_ignore_failure_true(self, env):
        parameters = {
            'ignore_failure': True
        }
        self._run(env=env, subgraph_retries=0, workflow='uninstall',
                  parameters=parameters)
        self.assertEqual(invocations, [('node1', 'stop'), ('node1', 'delete')])
