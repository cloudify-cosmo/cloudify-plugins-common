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

import random

import mock
import testtools

from cloudify.celery import gate_keeper


class GateKeeperTest(testtools.TestCase):

    def test_gate_keeper(self):
        # This tests emulates the existence of 100 deployments
        # and 100 tasks for each (once for workflows and once
        # for operations). Total of 100 * 100 * 2 tasks that are "waiting"
        # in the gate_keeper queues.
        # Then, until no more tasks are left, each iteration, a different
        # workflows or operations queue is selected and one task execution
        # is simulated. After that, it verifies the internal state is
        # valid.
        # This test exists mostly to "stress test" this component and make
        # sure it does not introduce any noticeable bottleneck.

        num_deployments = 100
        num_operations = 100
        requests = []
        for i in range(num_deployments):
            op_deployment_requests = Requests(self, i, op)
            workflow_deployment_requests = Requests(self, i, workflow)
            for j in range(num_operations):
                op_deployment_requests.begin_new_request()
                workflow_deployment_requests.begin_new_request()
            requests.append(op_deployment_requests)
            requests.append(workflow_deployment_requests)
        while requests:
            deployment_requests = requests[random.randint(0,
                                                          len(requests) - 1)]
            if not deployment_requests.end_first_running_request():
                requests.remove(deployment_requests)

    def setUp(self):
        super(GateKeeperTest, self).setUp()
        self.bucket_size = 5
        self.current = set()
        self.keeper = gate_keeper.GateKeeper(
            with_gate_keeper=True,
            gate_keeper_bucket_size=self.bucket_size,
            worker=mock.Mock())

    def tearDown(self):
        for q in self.keeper._current.values():
            self.assertTrue(q.empty())
        for q in self.keeper._on_hold.values():
            self.assertTrue(q.empty())
        super(GateKeeperTest, self).tearDown()

    def begin(self, task):
        request = RequestMock(task, self)
        self.keeper.task_received(request, request.handler)
        return request


class Requests(object):

    def __init__(self, test, deployment_id, task_func):
        self.deployment_id = str(deployment_id)
        self.task_func = task_func
        self.test = test
        self.requests = []
        self.running_requests = []
        self.next_index = 0

    def begin_new_request(self):
        request = self.test.begin(self.task_func(self.deployment_id))
        self.requests.append(request)
        self._update_running()
        self.validate_expected_state()

    def end_first_running_request(self):
        if self.running_requests:
            request = self.running_requests.pop(0)
            request.end()
        self._update_running()
        self.validate_expected_state()
        return bool(self.running_requests)

    def _update_running(self):
        if len(self.running_requests) < self.test.bucket_size:
            if self.next_index < len(self.requests):
                request = self.requests[self.next_index]
                self.next_index += 1
                self.running_requests.append(request)

    def validate_expected_state(self):
        for request in self.requests:
            if request in self.running_requests:
                request.assert_running()
            else:
                request.assert_not_running()


class RequestMock(object):

    counter = 0

    def __init__(self, task, test):
        self.id = RequestMock.counter
        RequestMock.counter += 1
        deployment_id = task['deployment_id']
        task_type = task['type']
        self.kwargs = {
            '__cloudify_context': {
                'deployment_id': deployment_id,
                'type': task_type
            }
        }
        self.bucket_key = deployment_id
        if task_type == 'workflow':
            self.bucket_key = '{0}_workflows'.format(self.bucket_key)
        self.test = test

    on_success = mock.Mock()

    @property
    def running(self):
        return self in self.test.current

    def assert_running(self):
        self.test.assertIn(self, self.test.current)

    def assert_not_running(self):
        self.test.assertNotIn(self, self.test.current)

    def handler(self):
        self.assert_not_running()
        self.test.current.add(self)

    def end(self):
        self.assert_running()
        self.test.current.remove(self)
        self.test.keeper.task_ended(self.bucket_key)

    def __str__(self):
        return '({0} @ {1})'.format(self.id, self.bucket_key)

    __repr__ = __str__


def op(deployment_id):
    return {'deployment_id': deployment_id, 'type': 'operation'}


def workflow(deployment_id):
    return {'deployment_id': deployment_id, 'type': 'workflow'}
