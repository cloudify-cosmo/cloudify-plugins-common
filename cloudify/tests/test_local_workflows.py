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


import yaml
import sys
import tempfile
import unittest
import shutil
import os

import nose.tools

from cloudify.decorators import workflow, operation
from cloudify.workflows import local


@nose.tools.nottest
class LocalWorkflowTest(unittest.TestCase):

    def setUp(self):
        self.work_dir = tempfile.mkdtemp(prefix='cloudify-workflows-')
        self.storage_dir = os.path.join(self.work_dir, 'storage')
        os.mkdir(self.storage_dir)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        shutil.rmtree(self.work_dir)

    def test_workflow_and_operation_logging_and_events(self):
        import logging
        for h in logging.getLogger().handlers:
            logging.getLogger().removeHandler(h)
        import cloudify.logs
        cloudify.logs.stdout_event_out = lambda event: sys.stdout.write('{}\n'.format(event['message']))
        # cloudify.logs.stdout_event_out = lambda event: None
        cloudify.logs.stdout_log_out = lambda log: sys.stdout.write('{}\n'.format(log['message']))
        # cloudify.logs.stdout_log_out = lambda log: None

        def the_workflow(ctx, **_):
            instance = _instance(ctx, 'node')
            ctx.logger.info('workflow_logging')
            ctx.send_event('workflow_event')
            instance.logger.info('node_instance_logging')
            instance.send_event('node_instance_event')
            instance.execute_operation('test.op0')

        def the_operation(ctx, **_):
            ctx.logger.info('logging')
            ctx.send_event('event')

        self._execute_workflow(the_workflow, operation_methods=[
            the_operation])
        self.fail()

    def test_workflow_bootstrap_context(self):
        def bootstrap_context(ctx, **_):
            bootstrap_context = ctx.internal._get_bootstrap_context()
            self.assertDictEqual(bootstrap_context, {})
        self._execute_workflow(bootstrap_context)
        self.fail()

    def test_update_execution_status(self):
        def update_execution_status(ctx, **_):
            ctx.update_execution_status('status')
        self.assertRaises(RuntimeError,
                          self._execute_workflow,
                          update_execution_status)
        self.fail()

    def test_workflow_set_get_node_instance_state(self):
        def get_set_node_instance_state(ctx, **_):
            instance = _instance(ctx, 'node')
            self.assertIsNone(instance.get_state().get())
            instance.set_state('state').get()
            self.assertEquals('state', instance.get_state().get())
        self._execute_workflow(get_set_node_instance_state)

    def test_workflow_ctx_properties(self):
        def attributes(ctx, **_):
            self.assertEqual(self._testMethodName, ctx.blueprint_id)
            self.assertEqual(self._testMethodName, ctx.deployment_id)
            self.assertEqual('workflow', ctx.workflow_id)
            self.assertIsNotNone(ctx.execution_id)
        self._execute_workflow(attributes)

    def test_workflow_blueprint_model(self):
        self.fail()

    def test_operation_capabilities(self):
        self.fail()

    def test_operation_runtime_properties(self):
        def runtime_properties(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('test.op0').get()
            instance.execute_operation('test.op1').get()

        def op1(ctx, **_):
            ctx.runtime_properties['key'] = 'value'

        def op2(ctx, **_):
            self.assertEqual('value', ctx.runtime_properties['key'])

        self._execute_workflow(runtime_properties, operation_methods=[
            op1, op2])

    def test_operation_related_properties(self):
        self.fail()

    def test_operation_related_runtime_properties(self):
        self.fail()

    def test_operation_related_node_id(self):
        self.fail()

    def test_operation_ctx_properties_and_methods(self):
        def ctx_properties(ctx, **_):
            self.assertEqual('node', ctx.node_name)
            self.assertIn('node_', ctx.node_id)
            self.assertEqual('state', ctx.node_state)
            self.assertEqual(self._testMethodName, ctx.blueprint_id)
            self.assertEqual(self._testMethodName, ctx.deployment_id)
            self.assertIsNotNone(ctx.execution_id)
            self.assertEqual('workflow', ctx.workflow_id)
            self.assertIsNotNone(ctx.task_id)
            self.assertEqual('{}.{}'.format(__name__,
                                            'ctx_properties'),
                             ctx.task_name)
            self.assertIsNone(ctx.task_target)
            self.assertEqual('127.0.0.1', ctx.host_ip)
            self.assertEqual('127.0.0.1', ctx.host_ip)
            self.assertEqual('p', ctx.plugin)
            self.assertEqual('test.op0', ctx.operation)
            self.assertDictContainsSubset({'property': 'value'},
                                          ctx.properties)
            self.assertEqual('content', ctx.get_resource('resource'))
            target_path = ctx.download_resource('resource')
            with open(target_path) as f:
                self.assertEqual('content', f.read())
            expected_target_path = os.path.join(self.work_dir, 'resource')
            target_path = ctx.download_resource(
                'resource', target_path=expected_target_path)
            self.assertEqual(target_path, expected_target_path)
            with open(target_path) as f:
                self.assertEqual('content', f.read())
        self._execute_workflow(operation_methods=[ctx_properties])

    def test_operation_bootstrap_context(self):
        def contexts(ctx, **_):
            self.assertDictEqual({}, ctx.bootstrap_context._bootstrap_context)
            self.assertDictEqual({}, ctx.provider_context)
        self._execute_workflow(operation_methods=[contexts])
        self.fail()

    def test_install_uninstall(self):
        self.fail()

    def _load_env(self, blueprint_path, name=None):
        if name is None:
            name = self._testMethodName
        return local.Environment(blueprint_path,
                                 name=name,
                                 storage_cls=self.storage_cls,
                                 **self.storage_kwargs)

    def _execute_workflow(self, workflow_method=None, operation_methods=None):
        # same as @workflow above the method
        if workflow_method is None and len(operation_methods) == 1:
            def workflow_method(ctx, **_):
                instance = _instance(ctx, 'node')
                instance.set_state('state').get()
                instance.execute_operation('test.op0')

        workflow_method = workflow(workflow_method)
        if operation_methods is None:
            operation_methods = []
        operation_methods = [operation(m) for m in operation_methods]

        setattr(sys.modules[__name__],
                workflow_method.__name__,
                workflow_method)
        for operation_method in operation_methods:
            setattr(sys.modules[__name__],
                    operation_method.__name__,
                    operation_method)

        try:
            blueprint = {
                'plugins': {
                    'p': {
                        'derived_from': 'cloudify.plugins.manager_plugin'
                    }
                },
                'node_types': {
                    'type': {
                        'properties': {
                            'property': {}
                        }
                    }
                },
                'node_templates': {
                    'node': {
                        'type': 'type',
                        'interfaces': {
                            'test': [
                                {'op{}'.format(index):
                                 'p.{}.{}'.format(__name__,
                                                  op_method.__name__)}
                                for index, op_method in
                                enumerate(operation_methods)
                            ]
                        },
                        'properties': {'property': 'value'}
                    }
                },
                'workflows': {
                    'workflow': 'p.{}.{}'.format(__name__,
                                                 workflow_method.__name__)
                }
            }

            blueprint_dir = os.path.join(self.work_dir, 'blueprint')
            os.mkdir(blueprint_dir)
            with open(os.path.join(blueprint_dir, 'resource'), 'w') as f:
                f.write('content')
            blueprint_path = os.path.join(blueprint_dir, 'blueprint.yaml')
            with open(blueprint_path, 'w') as f:
                f.write(yaml.safe_dump(blueprint))
            env = self._load_env(blueprint_path)
            env.execute('workflow')
        finally:
            delattr(sys.modules[__name__],
                    workflow_method.__name__)
            for operation_method in operation_methods:
                delattr(sys.modules[__name__],
                        operation_method.__name__)


@nose.tools.istest
class LocalWorkflowTestInMemoryStorage(LocalWorkflowTest):

    def setUp(self):
        self.storage_cls = local.InMemoryStorage
        self.storage_kwargs = {}
        super(LocalWorkflowTestInMemoryStorage, self).setUp()


@nose.tools.istest
class LocalWorkflowTestFileStorage(LocalWorkflowTest):

    def setUp(self):
        super(LocalWorkflowTestFileStorage, self).setUp()
        self.storage_cls = local.FileStorage
        self.storage_kwargs = {'storage_dir': self.storage_dir}

    def cleanup(self):
        shutil.rmtree(self.storage_dir)


class LocalWorkflowEnvironmentTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_inputs(self):
        self.fail()

    def test_workflow_parameters(self):
        self.fail()

    def test_no_operation_module(self):
        self.fail()

    def test_no_operation_attribute(self):
        self.fail()

    def test_no_source_operation_module(self):
        self.fail()

    def test_no_source_operation_attribute(self):
        self.fail()

    def test_no_target_operation_module(self):
        self.fail()

    def test_no_target_operation_attribute(self):
        self.fail()


def _instance(ctx, node_name):
    return next(ctx.get_node(node_name).instances)
