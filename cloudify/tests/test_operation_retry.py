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


import os

import testtools

from cloudify import context
from cloudify import decorators
from cloudify import exceptions
from cloudify.workflows import local


RETRY_MESSAGE = 'operation will be retried'
RETRY_AFTER = 10


@decorators.operation
def retry_operation(ctx, **_):
    return ctx.operation.retry(message=RETRY_MESSAGE, retry_after=RETRY_AFTER)


class OperationRetryTests(testtools.TestCase):

    def test_operation_retry_api(self):
        op_name = 'operation'
        ctx = context.CloudifyContext({
            'operation': {
                'name': op_name,
                'retry_number': 0,
                'max_retries': 10
            }
        })
        self.assertEqual(op_name, ctx.operation.name)
        self.assertEqual(0, ctx.operation.retry_number)
        self.assertEqual(10, ctx.operation.max_retries)

    def test_operation_retry(self):
        ctx = context.CloudifyContext({})
        e = self.assertRaises(exceptions.OperationRetry,
                              retry_operation,
                              ctx)
        self.assertEqual(RETRY_AFTER, e.retry_after)
        self.assertIn(RETRY_MESSAGE, str(e))


@decorators.operation
def node_operation_retry(ctx, **kwargs):
    if 'counter' not in ctx.instance.runtime_properties:
        ctx.instance.runtime_properties['counter'] = 0
    counter = ctx.instance.runtime_properties['counter']
    if counter != ctx.operation.retry_number:
        raise exceptions.NonRecoverableError(
            'counter({0}) != ctx.operation.retry_number({1})'.format(
                counter, ctx.operation.retry_number))
    expected_max_retries = 3
    if ctx.operation.max_retries != expected_max_retries:
        raise exceptions.NonRecoverableError(
            'ctx.operation.max_retries is expected to be {0} but '
            'is {1}'.format(expected_max_retries, ctx.operation.max_retries))
    ctx.instance.runtime_properties['counter'] = counter + 1
    if ctx.operation.retry_number < ctx.operation.max_retries:
        return ctx.operation.retry(message='Operation will be retried',
                                   retry_after=3)


@decorators.workflow
def execute_operation(ctx, **kwargs):
    graph = ctx.graph_mode()
    for instance in next(ctx.nodes).instances:
        graph.add_task(instance.execute_operation('lifecycle.start'))
    graph.execute()


class OperationRetryWorkflowTests(testtools.TestCase):

    def setUp(self):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-operation-retry-blueprint.yaml")
        self.env = local.init_env(blueprint_path)
        super(OperationRetryWorkflowTests, self).setUp()

    def test_operation_retry(self):
        self.env.execute('execute_operation',
                         task_retries=3,
                         task_retry_interval=1)
        instance = self.env.storage.get_node_instances()[0]
        self.assertEqual(1, len(instance['runtime_properties']))
