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


from StringIO import StringIO
from os import path

import testtools
from mock import patch

from cloudify import context
from cloudify import decorators
from cloudify import exceptions
from cloudify import logs
from cloudify.workflows import tasks as workflow_tasks
from cloudify.test_utils import workflow_test
from cloudify.test_utils import dispatch_helper

RETRY_MESSAGE = 'operation will be retried'
RETRY_AFTER = 10


@decorators.operation
def retry_operation(ctx, **_):
    return dispatch_helper.run(retry_operation_impl, **_)


def retry_operation_impl(ctx, **_):
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
def execute_operation(ctx, operation, **kwargs):
    ignore_operations = ['lifecycle.stop']

    graph = ctx.graph_mode()
    for instance in next(ctx.nodes).instances:
        task = instance.execute_operation(operation)
        if operation in ignore_operations:
            def ignore(tsk):
                return workflow_tasks.HandlerResult.ignore()

            task.on_failure = ignore
        graph.add_task(task)
    graph.execute()


class OperationRetryWorkflowTests(testtools.TestCase):

    blueprint_path = path.join('resources', 'blueprints',
                               'test-operation-retry-blueprint.yaml')

    @workflow_test(blueprint_path)
    def test_operation_retry(self, cfy_local):
        cfy_local.execute('execute_operation',
                          task_retries=3,
                          task_retry_interval=1,
                          parameters={
                              'operation': 'lifecycle.start'
                          })
        instance = cfy_local.storage.get_node_instances()[0]
        self.assertEqual(4, instance['runtime_properties']['counter'])

    def test_operation_retry_task_message(self):
        output_buffer = StringIO()
        original_event_out = logs.stdout_event_out

        # Provide same interface for all event output
        def event_output(log, ctx=None):
            original_event_out(log)
            output_buffer.write('{0}\n'.format(log['message']['text']))

        with patch('cloudify.logs.stdout_event_out', event_output):
            self.test_operation_retry()
            self.assertIn('Task rescheduled', output_buffer.getvalue())
            self.assertIn('Operation will be retried',
                          output_buffer.getvalue())

    @workflow_test(blueprint_path)
    def test_ignore_operation_retry(self, cfy_local):
        cfy_local.execute('execute_operation',
                          task_retries=3,
                          task_retry_interval=1,
                          parameters={
                              'operation': 'lifecycle.stop'
                          })
        instance = cfy_local.storage.get_node_instances()[0]
        self.assertEqual(4, instance['runtime_properties']['counter'])
