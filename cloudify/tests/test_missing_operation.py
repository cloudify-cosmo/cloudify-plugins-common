########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

from os import path

import testtools

from cloudify.decorators import workflow
from cloudify.test_utils import workflow_test


@workflow
def not_exist_op_workflow(ctx, **kwargs):
    for node in ctx.nodes:
        for instance in node.instances:
            instance.execute_operation(
                'cloudify.interfaces.lifecycle.op_not_exist')


@workflow
def not_exist_interface_workflow(ctx, **kwargs):
    for node in ctx.nodes:
        for instance in node.instances:
            instance.execute_operation(
                'cloudify.interfaces.interfaces_not_exist.create')


@workflow
def stop_workflow(ctx, **kwargs):
    for node in ctx.nodes:
        for instance in node.instances:
            instance.execute_operation(
                'cloudify.interfaces.lifecycle.stop')


class TestExecuteNotExistOperationWorkflow(testtools.TestCase):

    execute_blueprint_path = path.join('resources', 'blueprints',
                                       'not_exist_op_workflow.yaml')

    @workflow_test(execute_blueprint_path)
    def test_execute_not_exist_operation(self, cfy_local):
        node_id = cfy_local.plan.get('node_instances')[0].get('id')
        try:
            cfy_local.execute('not_exist_op_workflow')
            self.fail('Expected exception due to operation not exist')
        except Exception as e:
            self.assertTrue('operation of node instance {0} does not exist'
                            .format(node_id) in e.message)

    @workflow_test(execute_blueprint_path)
    def test_execute_not_exist_interface(self, cfy_local):
        node_id = cfy_local.plan.get('node_instances')[0].get('id')
        try:
            cfy_local.execute('not_exist_interface_workflow')
            self.fail('Expected exception due to operation not exist')
        except Exception as e:
            self.assertTrue('operation of node instance {0} does not exist'
                            .format(node_id) in e.message)

    @workflow_test(execute_blueprint_path)
    def test_execute_stop_operation(self, cfy_local):
        # checks that an operation that exists in a builtin interface
        # does not raise an exception if it is not declared in the blueprint
        cfy_local.execute('stop_workflow')
