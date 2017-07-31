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

from os import path

import testtools

from cloudify.constants import COMPUTE_NODE_TYPE
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError
from cloudify.test_utils import workflow_test


_VALIDATION_SUCCESS = {
    'agent_alive_crossbroker': True
}
_VALIDATION_FAIL = {
    'agent_alive_crossbroker': False
}


@operation
def validate_amqp(ctx, current_amqp=True, **_):
    status = ctx.node.properties['validation_result']
    if status:
        ctx.instance.runtime_properties['agent_status'] = status
        if not current_amqp and not status['agent_alive_crossbroker']:
            raise NonRecoverableError()


@operation
def create_amqp(ctx, **_):
    ctx.instance.runtime_properties['created'] = True


class TestInstallNewAgentsWorkflow(testtools.TestCase):
    blueprint_path = path.join('resources', 'blueprints',
                               'install-new-agents-blueprint.yaml')

    def _assert_all_computes_created(self, env, created):
        for node_instance in env.storage.get_node_instances():
            node = env.storage.get_node(node_instance['name'])
            is_compute = COMPUTE_NODE_TYPE in node['type_hierarchy']
            expected_created = created and is_compute
            if expected_created:
                self.assertTrue(node_instance.runtime_properties.get(
                    'created'))
            else:
                self.assertNotIn('created', node_instance.runtime_properties)

    @workflow_test(blueprint_path)
    def test_not_installed(self, cfy_local):
        with testtools.ExpectedException(RuntimeError, ".*is not started.*"):
            cfy_local.execute('install_new_agents')
        self._assert_all_computes_created(cfy_local, created=False)

    @workflow_test(blueprint_path, inputs={
        'host_a_validation_result': _VALIDATION_SUCCESS,
        'host_b_validation_result': _VALIDATION_SUCCESS})
    def test_correct(self, cfy_local):
        cfy_local.execute('install')
        cfy_local.execute('install_new_agents')
        self._assert_all_computes_created(cfy_local, created=True)

    @workflow_test(blueprint_path, inputs={
        'host_a_validation_result': _VALIDATION_SUCCESS,
        'host_b_validation_result': _VALIDATION_FAIL})
    def test_failed_validation(self, cfy_local):
        cfy_local.execute('install')
        with testtools.ExpectedException(RuntimeError,
                                         ".*Task failed.*validate_amqp.*"):
            cfy_local.execute('install_new_agents')
        self._assert_all_computes_created(cfy_local, created=False)

    @workflow_test(blueprint_path, inputs={
        'host_a_validation_result': _VALIDATION_SUCCESS,
        'host_b_validation_result': _VALIDATION_SUCCESS})
    def test_validation_only(self, cfy_local):
        cfy_local.execute('install')
        cfy_local.execute('install_new_agents', parameters={'install': False},
                          allow_custom_parameters=True)
        self._assert_all_computes_created(cfy_local, created=False)
