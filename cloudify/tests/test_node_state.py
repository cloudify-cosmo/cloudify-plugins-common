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
from cloudify.manager import NodeState


class NodeStateTest(unittest.TestCase):

    def test_put_get(self):
        node = NodeState('id', {})
        node['key'] = 'value'
        self.assertEqual('value', node['key'])
        updated = node.get_updated_properties()
        self.assertEqual(1, len(updated))
        self.assertEqual(1, len(updated['key']))
        self.assertEqual('value', updated['key'][0])

    def test_updated(self):
        props = {'key': 'value', 'not_updated': 'value'}
        node = NodeState('id', props)
        node['key'] = 'new_value'
        self.assertEqual('new_value', node['key'])
        self.assertEqual('value', node['not_updated'])
        updated = node.get_updated_properties()
        self.assertEqual(1, len(updated))
        self.assertEqual(2, len(updated['key']))
        self.assertEqual('new_value', updated['key'][0])
        self.assertEqual('value', updated['key'][1])

    def test_no_updates_to_empty_node(self):
        node = NodeState('id')
        self.assertEqual(0, len(node.get_updated_properties()))

    def test_no_updates(self):
        node = NodeState('id', {'key': 'value'})
        self.assertEqual(0, len(node.get_updated_properties()))

    def test_put_new_property(self):
        node = NodeState('id')
        node.put('key', 'value')
        self.assertEqual('value', node.get('key'))
        updated = node.get_updated_properties()
        self.assertEqual(1, len(updated))
        self.assertEqual(1, len(updated['key']))
        self.assertEqual('value', updated['key'][0])

    def test_put_several_properties(self):
        node = NodeState('id', {'key0': 'value0'})
        node.put('key1', 'value1')
        node.put('key2', 'value2')
        updated = node.get_updated_properties()
        self.assertEqual(2, len(updated))
        self.assertEqual(1, len(updated['key1']))
        self.assertEqual(1, len(updated['key2']))
        self.assertEqual('value1', updated['key1'][0])
        self.assertEqual('value2', updated['key2'][0])

    def test_put_new_property_twice(self):
        node = NodeState('id')
        node.put('key', 'value')
        node.put('key', 'v')
        self.assertEqual('v', node.get('key'))
        updated = node.get_updated_properties()
        self.assertEqual(1, len(updated))
        self.assertEqual(2, len(updated['key']))
        self.assertEqual('v', updated['key'][0])
        self.assertEqual('value', updated['key'][1])
