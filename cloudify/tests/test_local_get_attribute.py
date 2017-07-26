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
import shutil
import tempfile

import testtools

from cloudify import constants
from cloudify.workflows import local
from cloudify.decorators import operation
from cloudify.decorators import workflow
from cloudify import ctx as operation_ctx
from cloudify.workflows import ctx as workflow_ctx


class TestLocalWorkflowGetAttribute(testtools.TestCase):

    def test_in_memory_storage(self):
        self._test()

    def test_file_storage(self):
        tempdir = tempfile.mkdtemp()
        storage = local.FileStorage(tempdir)
        try:
            self._test(storage)
        finally:
            shutil.rmtree(tempdir)

    def test_file_storage_payload(self):
        tempdir = tempfile.mkdtemp()
        storage = local.FileStorage(tempdir)
        try:
            self._test(storage)

            # update payload
            with storage.payload() as payload:
                payload['payload_key'] = 'payload_key_value'

            # read payload
            storage2 = local.FileStorage(tempdir)
            local.load_env(self.env.name, storage=storage2)
            with storage2.payload() as payload:
                self.assertEqual(payload['payload_key'], 'payload_key_value')
        finally:
            shutil.rmtree(tempdir)

    def test_multi_instance_relationship_ambiguity_resolution(self):
        self._test(blueprint='get_attribute_multi_instance.yaml')

    def test_multi_instance_scaling_group_ambiguity_resolution(self):
        self._test(blueprint='get_attribute_multi_instance2.yaml')

    def _test(self, storage=None, blueprint='get_attribute.yaml'):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources/blueprints/{0}'.format(blueprint))
        self.env = local.init_env(blueprint_path, storage=storage)
        self.env.execute('setup', task_retries=0)
        self.env.execute('run', task_retries=0)


@workflow
def populate_runtime_properties(**_):
    for node in workflow_ctx.nodes:
        for instance in node.instances:
            instance.execute_operation('test.setup')


@workflow
def run_all_operations(**_):
    node = workflow_ctx.get_node('node1')
    instance = next(node.instances)
    instance.execute_operation('test.op')
    relationship = next(instance.relationships)
    relationship.execute_source_operation('test.op')
    relationship.execute_target_operation('test.op')


@operation
def populate(**_):
    operation_ctx.instance.runtime_properties.update({
        'self_ref_property': 'self_ref_value',
        'node_ref_property': 'node_ref_value',
        'source_ref_property': 'source_ref_value',
        'target_ref_property': 'target_ref_value',
    })


@operation
def op(self_ref=None,
       node_ref=None,
       source_ref=None,
       target_ref=None,
       static=None,
       **_):
    if operation_ctx.type == constants.NODE_INSTANCE:
        assert self_ref == 'self_ref_value', \
            'self: {0}'.format(self_ref)
        assert node_ref == 'node_ref_value', \
            'node: {0}'.format(self_ref)
        assert source_ref is None, \
            'source: {0}'.format(source_ref)
        assert source_ref is None, \
            'target: {0}'.format(target_ref)
        assert static == 'static_property_value', \
            'static: {0}'.format(static)
    else:
        assert self_ref is None, \
            'self: {0}'.format(self_ref)
        assert node_ref is None, \
            'node: {0}'.format(self_ref)
        assert source_ref == 'source_ref_value', \
            'source: {0}'.format(source_ref)
        assert target_ref == 'target_ref_value', \
            'target: {0}'.format(target_ref)


@workflow
def run_multi(**_):
    node = workflow_ctx.get_node('node1')
    for instance in node.instances:
        instance.execute_operation('test.op')


@operation
def populate_multi(**_):
    operation_ctx.instance.runtime_properties.update({
        'node_ref_property': 'node_ref_value_{0}'.format(
            operation_ctx.instance.id),
    })


@operation
def op_multi(node_ref, **_):
    operation_ctx.logger.info(node_ref)
    assert node_ref.startswith('node_ref_value_node2_'), \
        'node: {0}'.format(operation_ctx.instance.id)
