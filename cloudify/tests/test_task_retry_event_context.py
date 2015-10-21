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

from os import path

import testtools
from mock import patch

from cloudify import decorators
from cloudify import exceptions
from cloudify import logs
from cloudify.test_utils import workflow_test


@decorators.operation
def op(ctx, retry_type, **_):
    if 0 <= ctx.operation.max_retries <= ctx.operation.retry_number:
        return
    if retry_type == 'retry':
        ctx.operation.retry()
    elif retry_type == 'error':
        raise RuntimeError()
    elif retry_type == 'non-recoverable':
        raise exceptions.NonRecoverableError()


@decorators.workflow
def execute_operation(ctx, retry_type, **_):
    instance = next(next(ctx.nodes).instances)
    instance.execute_operation('test.op', kwargs={
        'retry_type': retry_type,
    })


class TaskRetryEventContextTests(testtools.TestCase):

    blueprint_path = path.join('resources', 'blueprints',
                               'test-task-retry-event-context-blueprint.yaml')

    @workflow_test(blueprint_path)
    def test_operation_retry(self, cfy_local):
        self._test_impl(cfy_local, 'retry', task_retries=2)

    @workflow_test(blueprint_path)
    def test_recoverable_retry(self, cfy_local):
        self._test_impl(cfy_local, 'error', task_retries=2)

    @workflow_test(blueprint_path)
    def test_infinite_retries(self, cfy_local):
        self._test_impl(cfy_local, 'non-recoverable', task_retries=-1)

    def _test_impl(self, cfy_local, retry_type, task_retries):
        events = []
        original_event_out = logs.stdout_event_out

        # Provide same interface for all event outputs
        def event_output(event, ctx=None):
            original_event_out(event)
            events.append(event)
        with patch('cloudify.logs.stdout_event_out', event_output):
            try:
                cfy_local.execute('execute_operation',
                                  task_retries=task_retries,
                                  task_retry_interval=0,
                                  parameters={
                                      'retry_type': retry_type
                                  })
            except exceptions.NonRecoverableError:
                pass
        events = [e for e in events if
                  e.get('event_type', '').startswith('task_') or
                  e.get('event_type', '').startswith('sending_task')]
        range_size = task_retries if task_retries > 0 else 1
        for i in range(range_size):
            # The following assertions are mostly here for sanity.
            # we generally want to make sure all task event contain
            # current_retries and total_retries so all tests try to go though
            # all event types.
            self.assertEqual('sending_task', events[i*3 + 0]['event_type'])
            self.assertEqual('task_started', events[i*3 + 1]['event_type'])
            if retry_type == 'retry':
                if i < task_retries:
                    expected_type = 'task_rescheduled'
                else:
                    expected_type = 'task_succeeded'
            elif retry_type == 'error':
                if i < task_retries:
                    expected_type = 'task_failed'
                else:
                    expected_type = 'task_succeeded'
            elif retry_type == 'non-recoverable':
                expected_type = 'task_failed'
            else:
                raise RuntimeError('We should not have arrived here')
            self.assertEqual(expected_type, events[i*3 + 2]['event_type'])

            # The following are the actual test assertions
            for j in range(3):
                context = events[i*3 + j]['context']
                self.assertEqual(i, context['task_current_retries'])
                self.assertEqual(task_retries, context['task_total_retries'])
