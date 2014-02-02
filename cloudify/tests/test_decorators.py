########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

__author__ = 'idanmo'


import unittest
import cloudify.decorators as decorators
from cloudify.decorators import with_node_state
from cloudify.decorators import with_logger
from cloudify.decorators import operation
from cloudify.manager import NodeState


@operation
@with_node_state
def node_state_1(__cloudify_id, node_state):
    return node_state is not None


@operation
@with_node_state
def node_state_2(**kwargs):
    return 'node_state' in kwargs and kwargs['node_state'] is not None


@operation
@with_node_state(arg='custom_name')
def node_state_3(__cloudify_id, custom_name):
    return custom_name is not None


@operation
@with_logger
@with_node_state
def method_with_two_decorators(__cloudify_id, node_state, logger):
    return node_state is not None and logger is not None


class WithNodeStateTest(unittest.TestCase):

    def get_state_mock(self, node_id):
        self._get_counter += 1
        return NodeState(node_id)

    def update_state_mock(self, *args):
        self._update_counter += 1

    def setUp(self):
        self._get_counter = 0
        self._update_counter = 0
        decorators.get_node_state = self.get_state_mock
        decorators.update_node_state = self.update_state_mock

    def test_node_state_injection(self):
        kwargs = {'__cloudify_id': 'id'}
        self.assertTrue(node_state_1(**kwargs))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    def test_node_state_injection_kwargs(self):
        kwargs = {'__cloudify_id': 'id'}
        self.assertTrue(node_state_2(**kwargs))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    def test_node_state_arg_name(self):
        kwargs = {'__cloudify_id': 'id'}
        self.assertTrue(node_state_3(**kwargs))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    def test_two_decorators(self):
        kwargs = {'__cloudify_id': 'id'}
        self.assertTrue(method_with_two_decorators(**kwargs))


@operation
@with_logger
def logger_1(logger):
    logger.info('hello!')
    return logger is not None


@operation
@with_logger(arg='custom_logger')
def logger_2(custom_logger):
    custom_logger.info('hello!')
    return custom_logger is not None


@operation
@with_logger(arg='custom_logger')
def logger_3(**kwargs):
    kwargs['custom_logger'].info('hello!')
    return 'custom_logger' in kwargs


@operation
@with_logger
def logger_4(x, logger):
    return logger is not None


class WithLoggerTest(unittest.TestCase):

    def test_with_logger(self):
        self.assertTrue(logger_1())
        self.assertTrue(logger_2())
        self.assertTrue(logger_3())

    def test_without_kwargs(self):
        self.assertTrue(logger_4(1000))
