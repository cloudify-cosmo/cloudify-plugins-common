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
from functools import wraps
from cloudify.decorators import inject_argument


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



