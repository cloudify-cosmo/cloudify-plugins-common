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
from cloudify.decorators import operation
from cloudify.context import CloudifyContext


@operation
def acquire_context(a, b, ctx, **kwargs):
    return ctx


class OperationTest(unittest.TestCase):

    def test_empty_ctx(self):
        ctx = acquire_context(0, 0)
        self.assertIsInstance(ctx, CloudifyContext)

    def test_provided_ctx(self):
        ctx = {'node_id': '1234'}
        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertIsInstance(ctx, CloudifyContext)
        self.assertEquals('1234', getattr(ctx, 'node_id'))

    def test_provided_capabilities(self):
        ctx = {
            'node_id': '5678',
            'capabilities': {
                'some_node': {
                    'k': 'v'
                }
            }
        }
        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertIn('k', ctx.capabilities)
        self.assertEquals('v', ctx.capabilities['k'])

    def test_capabilities_clash(self):
        ctx = {
            'node_id': '5678',
            'capabilities': {
                'node1': {'k': 'v1'},
                'node2': {'k': 'v2'},
            }
        }
        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertRaises(RuntimeError, ctx.capabilities.__contains__, 'k')

    def test_invalid_properties_update(self):
        kwargs = {'__cloudify_context': {
            'node_id': '5678'
        }}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertRaises(RuntimeError, ctx.properties.__setitem__, 'k', 'v')
