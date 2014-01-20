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
from functools import wraps
from cloudify.decorators import inject_argument
from cloudify.decorators import with_node_state
from cloudify.decorators import with_logger
from cloudify.manager import DeploymentNode


def inject(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        a, b = inject_argument('z', 'Z', method, args, kwargs)
        return method(*a, **b)
    return wrapper


class MethodArgumentInjectionTest(unittest.TestCase):

    @inject
    def method_0(self, x, y, z):
        return "{0}{1}{2}".format(x, y, z)

    @inject
    def method_1(self, z, x, y):
        return "{0}{1}{2}".format(x, y, z)

    @inject
    def method_2(self, x, z, y):
        return "{0}{1}{2}".format(x, y, z)

    @inject
    def method_3(self, x, z, y):
        return "{0}{1}{2}".format(x, y, z)

    @inject
    def no_z_arg(self, x, y):
        return "{0}{1}".format(x, y)

    @inject
    def no_z_arg_kwargs(self, x, y, **kwargs):
        return "{0}{1}{2}".format(x, y, kwargs['z'])

    @inject
    def z_named_arg(self, x, y, z=None):
        return "{0}{1}{2}".format(x, y, z)

    def test_arg_injection(self):
        methods = [self.method_0,
                   self.method_1,
                   self.method_2,
                   self.method_3,
                   self.no_z_arg_kwargs,
                   self.z_named_arg]

        for m in methods:
            self.assertEqual("XYZ", m("X", "Y"))

    def test_arg_doesnt_exist(self):
        self.assertRaises(TypeError, self.no_z_arg, [1, 2])

    @inject
    def method_named_z_arg(self, x, y="y", z="z"):
        return "{0}{1}{2}".format(x, y, z)

    @inject
    def method_z_arg(self, x, z, y="y"):
        return "{0}{1}{2}".format(x, y, z)

    @inject
    def method_kwargs(self, x, **kwargs):
        return "{0}{1}".format(x, kwargs['z'])

    @inject
    def method_kwargs_only(self, **kwargs):
        return "{0}".format(kwargs['z'])

    def test_method_kwargs_only(self):
        result = self.method_kwargs_only()
        self.assertEquals("Z", result)
        result = self.method_kwargs_only(z="A")
        self.assertEquals("Z", result)

    def test_method_kwargs(self):
        result = self.method_kwargs("X")
        self.assertEquals("XZ", result)
        result = self.method_kwargs("X", z="A")
        self.assertEquals("XZ", result)

    def test_method_z_arg(self):
        result = self.method_z_arg("X")
        self.assertEquals("XyZ", result)
        result = self.method_z_arg("X", "z")
        self.assertEquals("XyZ", result)
        result = self.method_z_arg("X", "z", y="Y")
        self.assertEquals("XYZ", result)

    def test_method_named_z_arg(self):
        result = self.method_named_z_arg("X", y="Y")
        self.assertEquals("XYZ", result)
        result = self.method_named_z_arg("X")
        self.assertEquals("XyZ", result)
        result = self.method_named_z_arg("X", z="z")
        self.assertEquals("XyZ", result)
        result = self.method_named_z_arg("X", y="y", z="z")
        self.assertEquals("XyZ", result)
        result = self.method_named_z_arg("X", "Y")
        self.assertEquals("XYZ", result)
        result = self.method_named_z_arg("X", "Y", "Z")
        self.assertEquals("XYZ", result)


class WithNodeStateTest(unittest.TestCase):

    def get_state_mock(self, node_id):
        self._get_counter += 1
        return DeploymentNode(node_id)

    def update_state_mock(self, *args):
        self._update_counter += 1

    def setUp(self):
        self._get_counter = 0
        self._update_counter = 0
        decorators.get_node_state = self.get_state_mock
        decorators.update_node_state = self.update_state_mock

    @with_node_state
    def method_1(self, __cloudify_id, node_state):
        return node_state is not None

    @with_node_state
    def method_2(self, **kwargs):
        return 'node_state' in kwargs and kwargs['node_state'] is not None

    @with_node_state(arg='custom_name')
    def method_3(self, __cloudify_id, custom_name):
        return custom_name is not None

    def test_node_state_injection(self):
        node_id = 'id'
        self.assertTrue(self.method_1(node_id))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    def test_node_state_injection_kwargs(self):
        node_id = 'id'
        self.assertTrue(self.method_2(__cloudify_id=node_id))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    def test_node_state_injection_named(self, node_state=None):
        node_id = 'id'
        self.assertTrue(self.method_2(__cloudify_id=node_id))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    def test_node_state_arg_name(self):
        node_id = 'id'
        self.assertTrue(self.method_3(node_id))
        self.assertEqual(1, self._get_counter)
        self.assertEqual(1, self._update_counter)

    @with_logger
    @with_node_state
    def method_with_two_decorators(self, __cloudify_id, node_state, logger):
        return node_state is not None and logger is not None

    def test_with_logger(self):
        self.assertTrue(self.method_with_two_decorators('id'))


class WithLoggerTest(unittest.TestCase):

    @with_logger
    def some_method_logger(self, logger):
        logger.info('hello!')
        return logger is not None

    @with_logger(arg='custom_logger')
    def some_method_custom_logger(self, custom_logger):
        custom_logger.info('hello!')
        return custom_logger is not None

    @with_logger(arg='custom_logger')
    def some_method_kwargs_logger(self, **kwargs):
        kwargs['custom_logger'].info('hello!')
        return 'custom_logger' in kwargs

    def test_with_logger(self):
        self.assertTrue(self.some_method_logger())
        self.assertTrue(self.some_method_custom_logger())
        self.assertTrue(self.some_method_kwargs_logger())
