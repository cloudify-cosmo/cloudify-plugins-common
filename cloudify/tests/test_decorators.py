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
from cloudify import manager
from cloudify.decorators import operation
from cloudify.context import CloudifyContext
from cloudify.exceptions import NonRecoverableError
import cloudify.tests.mocks.mock_rest_client as rest_client_mock


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
            'relationships': {
                'some_node': {
                    'k': 'v'
                }
            }
        }

        # using a mock rest client
        manager.get_rest_client = \
            lambda: rest_client_mock.MockRestclient()

        rest_client_mock.put_node_instance('some_node',
                                           runtime_properties={'k': 'v'})

        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertIn('k', ctx.capabilities)
        self.assertEquals('v', ctx.capabilities['k'])

    def test_capabilities_clash(self):
        capabilities = {
            'node1': {'k': 'v1'},
            'node2': {'k': 'v2'},
        }

        ctx = {
            'node_id': '5678',
            'relationships': capabilities
        }

        # using a mock rest client
        manager.get_rest_client = \
            lambda: rest_client_mock.MockRestclient()

        rest_client_mock.put_node_instance('node1',
                                           runtime_properties={'k': 'v1'})
        rest_client_mock.put_node_instance('node2',
                                           runtime_properties={'k': 'v2'})

        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertRaises(NonRecoverableError, ctx.capabilities.__contains__,
                          'k')

    def test_invalid_properties_update(self):
        kwargs = {'__cloudify_context': {
            'node_id': '5678'
        }}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertRaises(NonRecoverableError, ctx.properties.__setitem__,
                          'k', 'v')
