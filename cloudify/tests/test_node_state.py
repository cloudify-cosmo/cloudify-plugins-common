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


import testtools

from cloudify import exceptions
from cloudify.manager import NodeInstance


class NodeStateTest(testtools.TestCase):

    def test_put_get(self):
        node = NodeInstance('instance_id', 'node_id', {})
        node['key'] = 'value'
        self.assertEqual('value', node['key'])
        props = node.runtime_properties
        self.assertEqual(1, len(props))
        self.assertEqual('value', props['key'])

    def test_no_updates_to_empty_node(self):
        node = NodeInstance('instance_id', 'node_id')
        self.assertEqual(0, len(node.runtime_properties))

    def test_put_new_property(self):
        node = NodeInstance('instance_id', 'node_id')
        node.put('key', 'value')
        self.assertEqual('value', node.get('key'))
        props = node.runtime_properties
        self.assertEqual(1, len(props))
        self.assertEqual('value', props['key'])

    def test_put_several_properties(self):
        node = NodeInstance('instance_id', 'node_id', {'key0': 'value0'})
        node.put('key1', 'value1')
        node.put('key2', 'value2')
        props = node.runtime_properties
        self.assertEqual(3, len(props))
        self.assertEqual('value0', props['key0'])
        self.assertEqual('value1', props['key1'])
        self.assertEqual('value2', props['key2'])

    def test_update_property(self):
        node = NodeInstance('instance_id', 'node_id')
        node.put('key', 'value')
        self.assertEqual('value', node.get('key'))
        props = node.runtime_properties
        self.assertEqual(1, len(props))
        self.assertEqual('value', props['key'])

    def test_put_new_property_twice(self):
        node = NodeInstance('instance_id', 'node_id')
        node.put('key', 'value')
        node.put('key', 'v')
        self.assertEqual('v', node.get('key'))
        props = node.runtime_properties
        self.assertEqual(1, len(props))
        self.assertEqual('v', props['key'])

    def test_delete_property(self):
        node = NodeInstance('instance_id', 'node_id')
        node.put('key', 'value')
        self.assertEquals('value', node.get('key'))
        node.delete('key')
        self.assertNotIn('key', node)

    def test_delete_property_sugared_syntax(self):
        node = NodeInstance('instance_id', 'node_id')
        node.put('key', 'value')
        self.assertEquals('value', node.get('key'))
        del(node['key'])
        self.assertNotIn('key', node)

    def test_delete_nonexistent_property(self):
        node = NodeInstance('instance_id', 'node_id')
        self.assertRaises(KeyError, node.delete, 'key')

    def test_delete_makes_properties_dirty(self):
        node = NodeInstance('instance_id', 'node_id',
                            runtime_properties={'preexisting-key': 'val'})
        self.assertFalse(node.dirty)
        del(node['preexisting-key'])
        self.assertTrue(node.dirty)

    def test_setting_runtime_properties(self):
        """Assignment to .runtime_properties is possible and stores them."""
        node = NodeInstance('instance_id', 'node_id',
                            runtime_properties={'preexisting-key': 'val'})
        node.runtime_properties = {'other key': 'other val'}
        self.assertEqual({'other key': 'other val'}, node.runtime_properties)

    def test_setting_runtime_properties_sets_dirty(self):
        """Assignment to .runtime_properties sets the dirty flag."""
        node = NodeInstance('instance_id', 'node_id',
                            runtime_properties={'preexisting-key': 'val'})
        node.runtime_properties = {'other key': 'other val'}
        self.assertTrue(node.runtime_properties.dirty)

    def test_setting_runtime_properties_checks_modifiable(self):
        """Cannot assign to .runtime_properties if modifiable is false."""
        node = NodeInstance('instance_id', 'node_id',
                            runtime_properties={'preexisting-key': 'val'})
        node.runtime_properties.modifiable = False
        try:
            node.runtime_properties = {'other key': 'other val'}
        except exceptions.NonRecoverableError:
            pass
        else:
            self.fail(
                'Error should be raised when assigning runtime_properties '
                'with the modifiable flag set to False')
