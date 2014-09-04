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

import cloudify.logs
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

        events = []
        logs = []

        def mock_stdout_event(event):
            events.append(event)

        def mock_stdout_log(log):
            logs.append(log)

        o_stdout_event = cloudify.logs.stdout_event_out
        o_stdout_log = cloudify.logs.stdout_log_out
        cloudify.logs.stdout_event_out = mock_stdout_event
        cloudify.logs.stdout_log_out = mock_stdout_log
        try:
            def the_workflow(ctx, **_):
                instance = _instance(ctx, 'node')
                ctx.logger.info('workflow_logging')
                ctx.send_event('workflow_event').get()
                instance.logger.info('node_instance_logging')
                instance.send_event('node_instance_event').get()
                instance.execute_operation('test.op0').get()

            def the_operation(ctx, **_):
                ctx.logger.info('op_logging')
                ctx.send_event('op_event')

            self._execute_workflow(the_workflow, operation_methods=[
                the_operation])

            self.assertEqual(6, len(events))
            self.assertEqual(3, len(logs))
            self.assertEqual('workflow_event',
                             events[0]['message']['text'])
            self.assertEqual('node_instance_event',
                             events[1]['message']['text'])
            self.assertEqual('sending_task',
                             events[2]['event_type'])
            self.assertEqual('task_started',
                             events[3]['event_type'])
            self.assertEqual('op_event',
                             events[4]['message']['text'])
            self.assertEqual('task_succeeded',
                             events[5]['event_type'])
            self.assertEqual('workflow_logging',
                             logs[0]['message']['text'])
            self.assertEqual('node_instance_logging',
                             logs[1]['message']['text'])
            self.assertEqual('op_logging',
                             logs[2]['message']['text'])
        finally:
            cloudify.logs.stdout_event_out = o_stdout_event
            cloudify.logs.stdout_event_out = o_stdout_log

    def test_workflow_bootstrap_context(self):
        def bootstrap_context(ctx, **_):
            bootstrap_context = ctx.internal._get_bootstrap_context()
            self.assertDictEqual(bootstrap_context, {})
        self._execute_workflow(bootstrap_context)

    def test_update_execution_status(self):
        def update_execution_status(ctx, **_):
            ctx.update_execution_status('status')
        self.assertRaises(NotImplementedError,
                          self._execute_workflow,
                          update_execution_status)

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

        def op0(ctx, **_):
            ctx.runtime_properties['key'] = 'value'

        def op1(ctx, **_):
            self.assertEqual('value', ctx.runtime_properties['key'])

        self._execute_workflow(runtime_properties, operation_methods=[
            op0, op1])

    def test_operation_related_properties(self):
        def the_workflow(ctx, **_):
            instance = _instance(ctx, 'node')
            relationship = next(instance.relationships)
            relationship.execute_source_operation('test.op0')
            relationship.execute_target_operation('test.op0')

        def op(ctx, **_):
            if 'node2_' in ctx.related.node_id:
                self.assertDictContainsSubset({'property': 'default'},
                                              ctx.related.properties)
            elif 'node_' in ctx.related.node_id:
                self.assertDictContainsSubset({'property': 'value'},
                                              ctx.related.properties)
            else:
                self.fail('unexpected: {}'.format(ctx.related.node_id))

        self._execute_workflow(the_workflow, operation_methods=[op])

    def test_operation_related_runtime_properties(self):
        def related_runtime_properties(ctx, **_):
            instance = _instance(ctx, 'node')
            instance2 = _instance(ctx, 'node2')
            relationship = next(instance.relationships)
            instance.execute_operation('test.op0',
                                       kwargs={'value': 'instance1'}).get()
            instance2.execute_operation('test.op0',
                                        kwargs={'value': 'instance2'}).get()
            relationship.execute_source_operation(
                'test.op1', kwargs={'value': 'instance2'}).get()
            relationship.execute_target_operation(
                'test.op1', kwargs={'value': 'instance1'}).get()

        def op0(ctx, value, **_):
            ctx.runtime_properties['key'] = value

        def op1(ctx, value, **_):
            self.assertEqual(value, ctx.related.runtime_properties['key'])

        self._execute_workflow(related_runtime_properties, operation_methods=[
            op0, op1])

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
            self.assertEqual('{}.{}'.format(self._testMethodName,
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
        if workflow_method is None and len(operation_methods) == 1:
            def workflow_method(ctx, **_):
                instance = _instance(ctx, 'node')
                instance.set_state('state').get()
                instance.execute_operation('test.op0')

        # same as @workflow above the method
        workflow_method = workflow(workflow_method, force_not_celery=True)

        def stub_op(ctx, **_):
            pass
        if operation_methods is None:
            operation_methods = [stub_op]
        # same as @operation above each op method
        operation_methods = [operation(m, force_not_celery=True)
                             for m in operation_methods]

        temp_module = self._create_temp_module()

        setattr(temp_module,
                workflow_method.__name__,
                workflow_method)
        for operation_method in operation_methods:
            setattr(temp_module,
                    operation_method.__name__,
                    operation_method)

        interfaces = {
            'test': [
                {'op{}'.format(index):
                 'p.{}.{}'.format(self._testMethodName,
                                  op_method.__name__)}
                for index, op_method in
                enumerate(operation_methods)
            ]
        }

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
                            'property': {
                                'default': 'default'
                            }
                        }
                    }
                },
                'relationships': {
                    'cloudify.relationships.contained_in': {}
                },
                'node_templates': {
                    'node2': {
                        'type': 'type',
                        'interfaces': interfaces,
                    },
                    'node': {
                        'type': 'type',
                        'interfaces': interfaces,
                        'properties': {'property': 'value'},
                        'relationships': [{
                            'target': 'node2',
                            'type': 'cloudify.relationships.contained_in',
                            'source_interfaces': interfaces,
                            'target_interfaces': interfaces
                        }]
                    },
                },
                'workflows': {
                    'workflow': 'p.{}.{}'.format(self._testMethodName,
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
            self._remove_temp_module()

    def _create_temp_module(self):
        import imp
        temp_module = imp.new_module(self._testMethodName)
        sys.modules[self._testMethodName] = temp_module
        return temp_module

    def _remove_temp_module(self):
        del sys.modules[self._testMethodName]

@nose.tools.istest
class LocalWorkflowTestInMemoryStorage(LocalWorkflowTest):

    def setUp(self):
        super(LocalWorkflowTestInMemoryStorage, self).setUp()
        self.storage_cls = local.InMemoryStorage
        self.storage_kwargs = {}


@nose.tools.istest
class LocalWorkflowTestFileStorage(LocalWorkflowTest):

    def setUp(self):
        super(LocalWorkflowTestFileStorage, self).setUp()
        self.storage_cls = local.FileStorage
        self.storage_kwargs = {'storage_dir': self.storage_dir}


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
