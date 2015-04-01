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

from cloudify import decorators
from cloudify import exceptions
from cloudify.workflows import local

@decorators.operation
def fail_operation(**_):
    raise ExpectedException('TEST_EXPECTED_FAIL')

@decorators.workflow
def fail_execute_task(ctx, **kwargs):
    try:
        ctx.execute_task(
            task_name='cloudify.tests.test_task_retry.fail_operation').get()
    except ExpectedException:
        pass
    else:
        raise AssertionError()


class TaskRetryWorkflowTests(testtools.TestCase):

    def setUp(self):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-task-retry-blueprint.yaml")
        self.env = local.init_env(blueprint_path)
        super(TaskRetryWorkflowTests, self).setUp()

    def test_task_retry(self):
        self.env.execute('fail_execute_task',
                         task_retries=1,
                         task_retry_interval=0)


class ExpectedException(exceptions.RecoverableError):
    pass