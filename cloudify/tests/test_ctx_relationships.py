#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.


import os
import copy

import testtools

from cloudify import context
from cloudify.workflows import local
from cloudify.decorators import operation
from cloudify.decorators import workflow
from cloudify import ctx as operation_ctx
from cloudify.workflows import ctx as workflow_ctx
from cloudify import exceptions


class TestContextRelationship(testtools.TestCase):

    def setUp(self):
        super(TestContextRelationship, self).setUp()
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources/blueprints/relationship_context.yaml')
        self.env = local.init_env(blueprint_path)

    def test_instance_relationships(self):
        self._update_runtime_properties()
        result = self._assert_relationships('')
        self._test_relationships(result, '')

    def test_source_relationships(self):
        self._update_runtime_properties()
        result = self._assert_relationships('source')
        self._test_relationships(result, 'source')

    def test_target_relationships(self):
        self._update_runtime_properties()
        result = self._assert_relationships('target')
        self._test_relationships(result, 'target')

    def _test_relationships(self, result, rel):
        node1 = result['node1']
        node2 = result['node2']
        node3 = result['node3']

        self.assertEqual(len(node3), 0)

        if rel == 'target':
            self.assertEqual(len(node1), 0)
        else:
            self.assertEqual(len(node1), 1)
            rel1 = node1[0]
            self.assertEqual(rel1['type'],
                             'cloudify.relationships.contained_in2')
            self.assertEqual(rel1['type_hierarchy'],
                             ['cloudify.relationships.contained_in',
                              'cloudify.relationships.contained_in2'])
            self.assertEqual(rel1['target_node']['id'], 'node2')
            self.assertEqual(rel1['target_node']['prop'],
                             'node2_static_prop_value')
            self.assertIn('node2_', rel1['target_instance']['id'])
            self.assertEqual(rel1['target_instance']['runtime_properties'],
                             {'node2_prop': 'node2_value'})
        self._assert_node2_rel(node2)

    def _assert_node2_rel(self, relationships):
        self.assertEqual(len(relationships), 1)
        rel2 = relationships[0]
        self.assertEqual(rel2['type'], 'cloudify.relationships.contained_in3')
        self.assertEqual(rel2['type_hierarchy'],
                         ['cloudify.relationships.contained_in',
                          'cloudify.relationships.contained_in2',
                          'cloudify.relationships.contained_in3'])
        self.assertEqual(rel2['target_node']['id'], 'node3')
        self.assertEqual(rel2['target_node']['prop'],
                         'node3_static_prop_value')
        self.assertIn('node3_', rel2['target_instance']['id'])
        self.assertEqual(rel2['target_instance']['runtime_properties'],
                         {'node3_prop': 'node3_value'})

    def _update_runtime_properties(self):
        for node in ['node1', 'node2', 'node3']:
            self._run(
                'update_runtime_properties', '',
                node=node,
                kwargs={
                    'runtime_properties': {
                        'prop': {
                            '{0}_prop'.format(node):
                            '{0}_value'.format(node)}}
                })

    def _assert_relationships(self, rel):
        for node in ['node1', 'node2', 'node3']:
            self._run('assert_relationships', rel, node=node)

        instances = self.env.storage.get_node_instances()
        instance1 = [i for i in instances if i.node_id == 'node1'][0]
        instance2 = [i for i in instances if i.node_id == 'node2'][0]
        instance3 = [i for i in instances if i.node_id == 'node3'][0]
        rel = rel or 'self'
        return {
            'node1': instance1.runtime_properties.get(rel, []),
            'node2': instance2.runtime_properties.get(rel, []),
            'node3': instance3.runtime_properties.get(rel, [])
        }

    def _assert_capabilities(self, rel):
        self._run('assert_capabilities', rel)

        instances = self.env.storage.get_node_instances()
        instance1 = [i for i in instances if i.node_id == 'node1'][0]
        instance2 = [i for i in instances if i.node_id == 'node2'][0]
        rel = rel or 'self'
        return {
            'node1': instance1.runtime_properties.get(rel, {}),
            'node2': instance2.runtime_properties.get(rel, {}),
        }

    def test_modifiable_instance(self):
        self._run('assert_modifiable', '')
        instances = self.env.storage.get_node_instances()
        instance = [i for i in instances if i.node_id == 'node1'][0]
        self.assertEqual(instance.runtime_properties['new_prop'], 'new_value')

    def test_modifiable_source(self):
        self._test_modifiable_relationship('source')

    def test_modifiable_target(self):
        self._test_modifiable_relationship('target')

    def test_not_modifiable_instance(self):
        with testtools.ExpectedException(exceptions.NonRecoverableError,
                                         '.*Cannot modify.*'):
            self._run('assert_not_modifiable', '')

    def test_not_modifiable_source(self):
        with testtools.ExpectedException(exceptions.NonRecoverableError,
                                         '.*Cannot modify.*'):
            self._run('assert_not_modifiable', 'source')

    def test_not_modifiable_target(self):
        with testtools.ExpectedException(exceptions.NonRecoverableError,
                                         '.*Cannot modify.*'):
            self._run('assert_not_modifiable', 'target')

    def _test_modifiable_relationship(self, rel):
        self._run('assert_modifiable', rel)
        instances = self.env.storage.get_node_instances()
        instance1 = [i for i in instances if i.node_id == 'node1'][0]
        instance2 = [i for i in instances if i.node_id == 'node2'][0]
        self.assertEqual(instance1.runtime_properties['new_source_prop'],
                         'new_source_value')
        self.assertEqual(instance2.runtime_properties['new_target_prop'],
                         'new_target_value')

    def test_immutable_properties(self):
        with testtools.ExpectedException(exceptions.NonRecoverableError,
                                         '.*read only properties.*'):
            self._run('assert_immutable_properties', '')

    def test_instance_capabilities(self):
        self._update_runtime_properties()
        result = self._assert_capabilities('')
        self.assertIn('node2_', result['node1']['id'])
        self.assertEquals(result['node1']['prop'],
                          {'node2_prop': 'node2_value'})

    def test_source_capabilities(self):
        self._update_runtime_properties()
        result = self._assert_capabilities('source')
        self.assertIn('node2_', result['node1']['id'])
        self.assertEquals(result['node1']['prop'],
                          {'node2_prop': 'node2_value'})

    def test_target_capabilities(self):
        self._update_runtime_properties()
        result = self._assert_capabilities('target')
        self.assertIn('node3_', result['node2']['id'])
        self.assertEquals(result['node2']['prop'],
                          {'node3_prop': 'node3_value'})

    def test_invalid_deployment_capabilities(self):
        with testtools.ExpectedException(exceptions.NonRecoverableError,
                                         '.*used in a deployment context.*'):
            self.env.execute(
                'execute_task',
                parameters={
                    'task': '{0}.{1}'.format(__name__, 'assert_capabilities')
                })

    def test_2_hops(self):
        self._update_runtime_properties()
        self._run('asset_2_hops', '')
        node_instances = self.env.storage.get_node_instances()
        instance = [i for i in node_instances if i.node_id == 'node1'][0]
        self._assert_node2_rel(instance.runtime_properties['result'])

    def _run(self, op, rel, node='node1', kwargs=None):
        kwargs = kwargs or {}
        self.env.execute('execute_operation',
                         task_retries=0,
                         parameters={'op': op, 'rel': rel, 'node': node,
                                     'kwargs': kwargs})


@workflow
def execute_operation(op, rel, node, kwargs, **_):
    node = workflow_ctx.get_node(node)
    instance = next(node.instances)
    kwargs['rel'] = rel
    if rel == 'source':
        try:
            relationship = next(instance.relationships)
            relationship.execute_source_operation(op, kwargs=kwargs)
        except StopIteration:
            return
    elif rel == 'target':
        try:
            relationship = next(instance.relationships)
            relationship.execute_target_operation(op, kwargs=kwargs)
        except StopIteration:
            return
    elif rel == '':
        instance.execute_operation(op, kwargs=kwargs)
    else:
        raise RuntimeError('not handled: {0}'.format(rel))


@workflow
def execute_task(task, **_):
    workflow_ctx.execute_task(task)


@operation
def update_runtime_properties(runtime_properties, rel, **_):
    if operation_ctx.type == context.NODE_INSTANCE:
        instance = operation_ctx.instance
    elif operation_ctx.type == context.RELATIONSHIP_INSTANCE:
        if rel == 'source':
            instance = operation_ctx.source.instance
        elif rel == 'target':
            instance = operation_ctx.target.instance
        else:
            raise RuntimeError('not handled')
    else:
        raise RuntimeError('not handled')
    instance.runtime_properties.update(runtime_properties)


@operation
def assert_relationships(rel, **_):
    if operation_ctx.type == context.NODE_INSTANCE:
        instance = operation_ctx.instance
    elif operation_ctx.type == context.RELATIONSHIP_INSTANCE:
        if rel == 'source':
            instance = operation_ctx.source.instance
        elif rel == 'target':
            instance = operation_ctx.target.instance
        else:
            raise RuntimeError('not handled')
    else:
        raise RuntimeError('not handled')
    result = []
    for relationship in instance.relationships:
        result.append(_extract_relationship(relationship))
    rel = rel or 'self'
    instance.runtime_properties[rel] = result


def _extract_relationship(relationship):
    return {
        'type': relationship.type,
        'type_hierarchy': relationship.type_hierarchy,
        'target_node': {
            'id': relationship.target.node.id,
            'prop':
                copy.deepcopy(relationship.target.node.properties['prop'])
        },
        'target_instance': {
            'id': relationship.target.instance.id,
            'runtime_properties':
                copy.deepcopy(
                    relationship.target.instance.runtime_properties['prop'])
        }
    }

@operation
def assert_capabilities(rel=None, **_):
    if rel == 'source':
        instance = operation_ctx.source.instance
    elif rel == 'target':
        instance = operation_ctx.target.instance
    elif rel == '':
        instance = operation_ctx.instance
    else:
        # testing ctx.type == deployment fails
        return operation_ctx.capabilities.get_all()
    rel = rel or 'self'
    caps = operation_ctx.capabilities.get_all()
    if len(caps) != 1:
        raise RuntimeError('unexpected count {0}'.format(caps))

    node_id, runtime_properties = next(caps.iteritems())
    instance.runtime_properties[rel] = {
        'id': node_id,
        'prop': runtime_properties['prop']
    }


@operation
def assert_modifiable(**_):
    if operation_ctx.type == context.NODE_INSTANCE:
        operation_ctx.instance.runtime_properties['new_prop'] = 'new_value'
    elif operation_ctx.type == context.RELATIONSHIP_INSTANCE:
        operation_ctx.source.instance.runtime_properties[
            'new_source_prop'] = 'new_source_value'
        operation_ctx.target.instance.runtime_properties[
            'new_target_prop'] = 'new_target_value'
    else:
        raise RuntimeError('not handled')


@operation
def assert_not_modifiable(rel=None, **_):
    if operation_ctx.type == context.NODE_INSTANCE:
        for relationship in operation_ctx.instance.relationships:
            relationship.target.instance.runtime_properties[
                'should_not'] = 'work'
    elif operation_ctx.type == context.RELATIONSHIP_INSTANCE:
        if rel == 'source':
            for relationship in operation_ctx.source.instance.relationships:
                relationship.target.instance.runtime_properties[
                    'should_not'] = 'work'
        elif rel == 'target':
            for relationship in operation_ctx.target.instance.relationships:
                relationship.target.instance.runtime_properties[
                    'should_not'] = 'work'
    else:
        raise RuntimeError('not handled')


@operation
def assert_immutable_properties(**_):
    operation_ctx.node.properties['should_not'] = 'work'


@operation
def asset_2_hops(**_):
    result = []
    for relationship in operation_ctx.instance.relationships:
        for relationship2 in relationship.target.instance.relationships:
            result.append(_extract_relationship(relationship2))
    operation_ctx.instance.runtime_properties['result'] = result
