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

import contextlib
import time
import yaml
import sys
import tempfile
import shutil
import os
import threading
import Queue

import testtools
import nose.tools
import cloudify.logs
from testtools.matchers import ContainsAll
from cloudify.decorators import workflow, operation

from cloudify.exceptions import NonRecoverableError
from cloudify.workflows import local
from cloudify.workflows import workflow_context
from cloudify.workflows.workflow_context import task_config

PLUGIN_PACKAGE_NAME = 'test-package'
PLUGIN_PACKAGE_VERSION = '1.1.1'


@nose.tools.nottest
class BaseWorkflowTest(testtools.TestCase):

    def setUp(self):
        super(BaseWorkflowTest, self).setUp()
        self.work_dir = tempfile.mkdtemp(prefix='cloudify-workflows-')
        self.blueprint_dir = os.path.join(self.work_dir, 'blueprint')
        self.storage_dir = os.path.join(self.work_dir, 'storage')
        self.storage_kwargs = {}
        self.env = None
        os.mkdir(self.storage_dir)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        shutil.rmtree(self.work_dir)
        self._remove_temp_module()

    def _init_env(self, blueprint_path,
                  inputs=None,
                  name=None,
                  ignored_modules=None,
                  provider_context=None):
        if name is None:
            name = self._testMethodName

        storage = self.storage_cls(**self.storage_kwargs)

        if isinstance(storage, local.FileStorage) \
                and (self.storage_dir != self.blueprint_dir):
            shutil.rmtree(self.storage_kwargs['storage_dir'])

        return local.init_env(blueprint_path,
                              name=name,
                              inputs=inputs,
                              storage=storage,
                              ignored_modules=ignored_modules,
                              provider_context=provider_context)

    def _load_env(self, name):
        if name is None:
            name = self._testMethodName

        storage = self.storage_cls(**self.storage_kwargs)

        return local.load_env(name=name,
                              storage=storage)

    def _setup_env(self,
                   workflow_methods=None,
                   operation_methods=None,
                   use_existing_env=True,
                   name=None,
                   inputs=None,
                   create_blueprint_func=None,
                   workflow_parameters_schema=None,
                   load_env=False,
                   ignored_modules=None,
                   operation_retries=None,
                   operation_retry_interval=None,
                   provider_context=None):
        if create_blueprint_func is None:
            create_blueprint_func = self._blueprint_1

        def stub_op(ctx, **_):
            pass
        if operation_methods is None:
            operation_methods = [stub_op]

        if workflow_methods[0] is None:
            def workflow_method(ctx, **_):
                instance = _instance(ctx, 'node')
                instance.set_state('state').get()
                instance.execute_operation('test.op0')
            workflow_methods = [workflow_method]

        # same as @workflow above the method
        workflow_methods = [workflow(m) for m in workflow_methods]

        # same as @operation above each op method
        operation_methods = [operation(m) for m in operation_methods]

        temp_module = self._create_temp_module()

        for workflow_method in workflow_methods:
            setattr(temp_module,
                    workflow_method.__name__,
                    workflow_method)
        for operation_method in operation_methods:
            setattr(temp_module,
                    operation_method.__name__,
                    operation_method)

        blueprint = create_blueprint_func(workflow_methods,
                                          operation_methods,
                                          workflow_parameters_schema,
                                          ignored_modules,
                                          operation_retries,
                                          operation_retry_interval)

        inner_dir = os.path.join(self.blueprint_dir, 'inner')
        if not os.path.isdir(self.blueprint_dir):
            os.mkdir(self.blueprint_dir)
        if not os.path.isdir(inner_dir):
            os.mkdir(inner_dir)
        with open(os.path.join(inner_dir, 'imported.yaml'), 'w') as f:
            f.write('node_types: { imported_type: {} }')
        with open(os.path.join(self.blueprint_dir, 'resource'), 'w') as f:
            f.write('content')
        blueprint_path = os.path.join(self.blueprint_dir, 'blueprint.yaml')
        with open(blueprint_path, 'w') as f:
            f.write(yaml.safe_dump(blueprint))
        if not self.env or not use_existing_env:
            if load_env:
                self.env = self._load_env(name)
            else:
                self.env = self._init_env(blueprint_path,
                                          inputs=inputs,
                                          name=name,
                                          ignored_modules=ignored_modules,
                                          provider_context=provider_context)

    def _execute_workflow(self,
                          workflow_method=None,
                          operation_methods=None,
                          use_existing_env=True,
                          execute_kwargs=None,
                          name=None,
                          inputs=None,
                          create_blueprint_func=None,
                          workflow_parameters_schema=None,
                          workflow_name='workflow0',
                          load_env=False,
                          setup_env=True,
                          ignored_modules=None,
                          operation_retries=None,
                          operation_retry_interval=None,
                          provider_context=None):
        if setup_env:
            self._setup_env(
                workflow_methods=[workflow_method],
                operation_methods=operation_methods,
                use_existing_env=use_existing_env,
                name=name,
                inputs=inputs,
                create_blueprint_func=create_blueprint_func,
                workflow_parameters_schema=workflow_parameters_schema,
                load_env=load_env,
                ignored_modules=ignored_modules,
                operation_retries=operation_retries,
                operation_retry_interval=operation_retry_interval,
                provider_context=provider_context)
        elif load_env:
            self.env = self._load_env(name)

        execute_kwargs = execute_kwargs or {}
        final_execute_kwargs = {
            'task_retries': 0,
            'task_retry_interval': 1
        }
        final_execute_kwargs.update(execute_kwargs)

        return self.env.execute(workflow_name, **final_execute_kwargs)

    def _blueprint_1(self, workflow_methods, operation_methods,
                     workflow_parameters_schema, ignored_modules,
                     operation_retries, operation_retry_interval):
        interfaces = {
            'test': dict(
                ('op{0}'.format(index),
                 {'implementation': 'p.{0}.{1}'.format(self._testMethodName,
                                                       op_method.__name__),
                  'max_retries': operation_retries,
                  'retry_interval': operation_retry_interval})
                for index, op_method in
                enumerate(operation_methods)
            )
        }

        if ignored_modules:
            interfaces['test'].update({'ignored_op': 'p.{0}.ignored'
                                       .format(ignored_modules[0])})

        workflows = dict((
            ('workflow{0}'.format(index), {
                'mapping': 'p.{0}.{1}'.format(self._testMethodName,
                                              w_method.__name__),
                'parameters': workflow_parameters_schema or {}
            }) for index, w_method in enumerate(workflow_methods)
        ))

        blueprint = {
            'tosca_definitions_version': 'cloudify_dsl_1_3',
            'imports': ['inner/imported.yaml'],
            'inputs': {
                'from_input': {
                    'default': 'from_input_default_value'
                }
            },
            'outputs': {
                'some_output': {
                    'value': {'get_attribute': ['node', 'some_output']},
                },
                'static': {
                    'value': {'get_attribute': ['node', 'property']}
                }
            },
            'plugins': {
                'p': {
                    'executor': 'central_deployment_agent',
                    'install': False,
                    'package_name': PLUGIN_PACKAGE_NAME,
                    'package_version': PLUGIN_PACKAGE_VERSION
                }
            },
            'node_types': {
                'type': {
                    'properties': {
                        'property': {
                            'default': 'default'
                        },
                        'from_input': {
                            'default': 'from_input_default_value'
                        }
                    }
                },
                'cloudify.nodes.Compute': {
                    'derived_from': 'type',
                    'properties': {
                        'ip': {
                            'default': ''
                        }
                    }
                }
            },
            'relationships': {
                'cloudify.relationships.contained_in': {}
            },
            'node_templates': {
                'node4': {
                    'type': 'type',
                    'interfaces': interfaces,
                    'relationships': [{
                        'target': 'node3',
                        'type': 'cloudify.relationships.contained_in',
                    }]
                },
                'node3': {
                    'type': 'cloudify.nodes.Compute',
                    'interfaces': interfaces,
                    'properties': {
                        'ip': '1.1.1.1'
                    }
                },
                'node2': {
                    'type': 'cloudify.nodes.Compute',
                    'interfaces': interfaces,
                },
                'node': {
                    'type': 'type',
                    'interfaces': interfaces,
                    'properties': {
                        'property': 'value',
                        'from_input': {'get_input': 'from_input'}
                    },
                    'relationships': [{
                        'target': 'node2',
                        'type': 'cloudify.relationships.contained_in',
                        'source_interfaces': interfaces,
                        'target_interfaces': interfaces
                    }]
                },
                'node5': {
                    'type': 'imported_type'
                }
            },
            'workflows': workflows,
            'groups': {
                'group1': {
                    'members': ['node']
                }
            },
            'policies': {
                'policy1': {
                    'type': 'cloudify.policies.scaling',
                    'targets': ['group1']
                }
            }
        }
        return blueprint

    def _create_temp_module(self):
        import imp
        temp_module = imp.new_module(self._testMethodName)
        sys.modules[self._testMethodName] = temp_module
        return temp_module

    def _remove_temp_module(self):
        if self._testMethodName in sys.modules:
            del sys.modules[self._testMethodName]

    @contextlib.contextmanager
    def _mock_stdout_event_and_log(self):
        events = []
        logs = []

        # Provide same interface as other log/event functions
        def mock_stdout_event(event):
            events.append(event)

        # Provide same interface as other log/event functions
        def mock_stdout_log(log):
            logs.append(log)

        o_stdout_event = cloudify.logs.stdout_event_out
        o_stdout_log = cloudify.logs.stdout_log_out
        cloudify.logs.stdout_event_out = mock_stdout_event
        cloudify.logs.stdout_log_out = mock_stdout_log

        try:
            yield events, logs
        finally:
            cloudify.logs.stdout_event_out = o_stdout_log
            cloudify.logs.stdout_event_out = o_stdout_event

    def _test_retry_configuration_impl(self,
                                       global_retries,
                                       global_retry_interval,
                                       operation_retries,
                                       operation_retry_interval):

        expected_retries = global_retries
        if operation_retries is not None:
            expected_retries = operation_retries
        expected_retry_interval = global_retry_interval
        if operation_retry_interval is not None:
            expected_retry_interval = operation_retry_interval

        def flow(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('test.op0', kwargs={
                'props': {'key': 'initial_value'}
            }).get()
            instance.execute_operation('test.op1').get()

        def op0(ctx, props, **_):
            self.assertIsNotNone(ctx.instance.id)
            current_retry = ctx.instance.runtime_properties.get('retry', 0)
            last_timestamp = ctx.instance.runtime_properties.get('timestamp')
            current_timestamp = time.time()

            ctx.instance.runtime_properties['retry'] = current_retry + 1
            ctx.instance.runtime_properties['timestamp'] = current_timestamp

            self.assertEqual('initial_value', props['key'])
            props['key'] = 'new_value'

            if current_retry > 0:
                duration = current_timestamp - last_timestamp
                self.assertTrue(expected_retry_interval <= duration <=
                                expected_retry_interval + 0.5)
            if current_retry < expected_retries:
                self.fail()

        def op1(ctx, **_):
            self.assertEqual(
                expected_retries + 1, ctx.instance.runtime_properties['retry'])

        self._execute_workflow(
            flow,
            operation_methods=[op0, op1],
            operation_retries=operation_retries,
            operation_retry_interval=operation_retry_interval,
            execute_kwargs={
                'task_retry_interval': global_retry_interval,
                'task_retries': global_retries})


@nose.tools.nottest
class LocalWorkflowTest(BaseWorkflowTest):
    def test_workflow_and_operation_logging_and_events(self):

        def assert_task_events(indexes, events):
            self.assertEqual('sending_task',
                             events[indexes[0]]['event_type'])
            self.assertEqual('task_started',
                             events[indexes[1]]['event_type'])
            self.assertEqual('task_succeeded',
                             events[indexes[2]]['event_type'])

        def the_workflow(ctx, **_):
            def local_task():
                pass
            instance = _instance(ctx, 'node')
            ctx.logger.info('workflow_logging')
            ctx.send_event('workflow_event').get()
            instance.logger.info('node_instance_logging')
            instance.send_event('node_instance_event').get()
            instance.execute_operation('test.op0').get()
            ctx.local_task(local_task).get()

        def the_operation(ctx, **_):
            ctx.logger.info('op_logging')
            ctx.send_event('op_event')

        with self._mock_stdout_event_and_log() as (events, logs):
            self._execute_workflow(the_workflow, operation_methods=[
                the_operation])

            self.assertEqual(11, len(events))
            self.assertEqual(3, len(logs))
            self.assertEqual('workflow_started',
                             events[0]['event_type'])
            self.assertEqual('workflow_event',
                             events[1]['message']['text'])
            self.assertEqual('node_instance_event',
                             events[2]['message']['text'])
            assert_task_events([3, 4, 6], events)
            self.assertEqual('op_event',
                             events[5]['message']['text'])
            assert_task_events([7, 8, 9], events)
            self.assertEqual('workflow_succeeded',
                             events[10]['event_type'])
            self.assertEqual('workflow_logging',
                             logs[0]['message']['text'])
            self.assertEqual('node_instance_logging',
                             logs[1]['message']['text'])
            self.assertEqual('op_logging',
                             logs[2]['message']['text'])

    def test_task_event_filtering(self):

        def flow1(ctx, **_):
            def task():
                pass
            ctx.local_task(task)

        with self._mock_stdout_event_and_log() as (events, _):
            self._execute_workflow(flow1, use_existing_env=False)
            self.assertEqual(5, len(events))

        def flow2(ctx, **_):
            def task():
                pass
            ctx.local_task(task, send_task_events=False)

        with self._mock_stdout_event_and_log() as (events, _):
            self._execute_workflow(flow2,
                                   use_existing_env=False)
            self.assertEqual(2, len(events))

        def flow3(ctx, **_):
            @task_config(send_task_events=False)
            def task():
                pass
            ctx.local_task(task)

        with self._mock_stdout_event_and_log() as (events, _):
            self._execute_workflow(flow3, use_existing_env=False)
            self.assertEqual(2, len(events))

        def flow4(ctx, **_):
            @task_config(send_task_events=True)
            def task():
                pass
            ctx.local_task(task)

        with self._mock_stdout_event_and_log() as (events, _):
            self._execute_workflow(flow4, use_existing_env=False)
            self.assertEqual(5, len(events))

        def flow5(ctx, **_):
            def task():
                self.fail()
            ctx.local_task(task, send_task_events=False)

        with self._mock_stdout_event_and_log() as (events, _):
            self.assertRaises(AssertionError,
                              self._execute_workflow,
                              flow5, use_existing_env=False)
            self.assertEqual(3, len(events))
            self.assertEqual('task_failed', events[1]['event_type'])
            self.assertEqual('workflow_failed', events[2]['event_type'])

    def test_task_config_decorator(self):
        def flow(ctx, **_):
            task_config_kwargs = {'key': 'task_config'}
            invocation_kwargs = {'key': 'invocation'}

            @task_config(kwargs=task_config_kwargs)
            def task1(**kwargs):
                self.assertEqual(kwargs, task_config_kwargs)
            ctx.local_task(task1).get()

            @task_config(kwargs=task_config_kwargs)
            def task2(**kwargs):
                self.assertEqual(kwargs, task_config_kwargs)
            ctx.local_task(task2, kwargs=invocation_kwargs).get()

            @task_config(kwargs=task_config_kwargs)
            def task3(**kwargs):
                self.assertEqual(kwargs, invocation_kwargs)
                ctx.local_task(task3,
                               kwargs=invocation_kwargs,
                               override_task_config=True).get()
        self._execute_workflow(flow)

    def test_workflow_bootstrap_context(self):
        def bootstrap_context(ctx, **_):
            bootstrap_context = ctx.internal._get_bootstrap_context()
            self.assertEqual(bootstrap_context, {})
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
            self.assertEqual(self._testMethodName, ctx.blueprint.id)
            self.assertEqual(self._testMethodName, ctx.deployment.id)
            self.assertEqual(
                ['node'], ctx.deployment.scaling_groups['group1']['members'])
            node_instance = next(ctx.get_node('node').instances)
            scaling_groups = node_instance.scaling_groups
            self.assertEqual(1, len(scaling_groups))
            self.assertEqual('group1', scaling_groups[0]['name'])
            self.assertEqual('workflow0', ctx.workflow_id)
            self.assertIsNotNone(ctx.execution_id)
        self._execute_workflow(attributes)

    def test_workflow_blueprint_model(self):
        def blueprint_model(ctx, **_):
            nodes = list(ctx.nodes)
            node1 = ctx.get_node('node')
            node2 = ctx.get_node('node2')
            node1_instances = list(node1.instances)
            node2_instances = list(node2.instances)
            instance1 = node1_instances[0]
            instance2 = node2_instances[0]
            node1_relationships = list(node1.relationships)
            node2_relationships = list(node2.relationships)
            instance1_relationships = list(instance1.relationships)
            instance2_relationships = list(instance2.relationships)
            relationship = node1_relationships[0]
            relationship_instance = instance1_relationships[0]

            self.assertEqual(5, len(nodes))
            self.assertEqual(1, len(node1_instances))
            self.assertEqual(1, len(node2_instances))
            self.assertEqual(1, len(node1_relationships))
            self.assertEqual(0, len(node2_relationships))
            self.assertEqual(1, len(instance1_relationships))
            self.assertEqual(0, len(instance2_relationships))

            sorted_ops = ['op0', 'test.op0']

            self.assertEqual(1, node1.number_of_instances)
            self.assertEqual(1, node2.number_of_instances)
            self.assertEqual('node', node1.id)
            self.assertEqual('node2', node2.id)
            self.assertEqual('type', node1.type)
            self.assertEqual('type', node1.type)
            self.assertEqual('cloudify.nodes.Compute', node2.type)
            self.assertEqual(['type'], node1.type_hierarchy)
            self.assertEqual(['type', 'cloudify.nodes.Compute'],
                             node2.type_hierarchy)
            self.assertThat(node1.properties.items(),
                            ContainsAll({'property': 'value'}.items()))
            self.assertThat(node2.properties.items(),
                            ContainsAll({'property': 'default'}.items()))
            self.assertEqual(sorted_ops, sorted(node1.operations.keys()))
            self.assertEqual(sorted_ops, sorted(node2.operations.keys()))
            self.assertIs(relationship, node1.get_relationship('node2'))

            self.assertIn('node_', instance1.id)
            self.assertIn('node2_', instance2.id)
            self.assertEqual('node', instance1.node_id)
            self.assertEqual('node2', instance2.node_id)
            self.assertIs(node1, instance1.node)
            self.assertIs(node2, instance2.node)

            self.assertEqual(node2.id, relationship.target_id)
            self.assertTrue(relationship.is_derived_from(
                "cloudify.relationships.contained_in"
            ))
            self.assertEqual(node2, relationship.target_node)
            self.assertEqual(sorted_ops,
                             sorted(relationship.source_operations.keys()))
            self.assertEqual(sorted_ops,
                             sorted(relationship.target_operations.keys()))

            self.assertEqual(instance2.id, relationship_instance.target_id)
            self.assertEqual(instance2,
                             relationship_instance.target_node_instance)
            self.assertIs(relationship, relationship_instance.relationship)

        self._execute_workflow(blueprint_model)

    def test_operation_capabilities(self):
        def the_workflow(ctx, **_):
            instance = _instance(ctx, 'node')
            instance2 = _instance(ctx, 'node2')
            instance2.execute_operation('test.op0').get()
            instance.execute_operation('test.op1').get()

        def op0(ctx, **_):
            ctx.instance.runtime_properties['key'] = 'value'

        def op1(ctx, **_):
            caps = ctx.capabilities.get_all()
            self.assertEqual(1, len(caps))
            key, value = next(caps.iteritems())
            self.assertIn('node2_', key)
            self.assertEqual(value, {'key': 'value'})

        self._execute_workflow(the_workflow, operation_methods=[op0, op1])

    def test_operation_runtime_properties(self):
        def runtime_properties(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('test.op0').get()
            instance.execute_operation('test.op1').get()

        def op0(ctx, **_):
            ctx.instance.runtime_properties['key'] = 'value'

        def op1(ctx, **_):
            self.assertEqual('value', ctx.instance.runtime_properties['key'])

        self._execute_workflow(runtime_properties, operation_methods=[
            op0, op1])

    def test_operation_related_properties(self):
        def the_workflow(ctx, **_):
            instance = _instance(ctx, 'node')
            relationship = next(instance.relationships)
            relationship.execute_source_operation('test.op0')
            relationship.execute_target_operation('test.op0')

        def op(ctx, **_):
            if 'node2_' in ctx.target.instance.id:
                self.assertThat(ctx.target.node.properties.items(),
                                ContainsAll({'property': 'default'}.items()))
            elif 'node_' in ctx.target.instance.id:
                self.assertThat(ctx.target.node.properties.items(),
                                ContainsAll({'property': 'value'}.items()))
            else:
                self.fail('unexpected: {0}'.format(ctx.target.instance.id))
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
                'test.op1', kwargs={
                    'source': 'instance1',
                    'target': 'instance2'
                }).get()
            relationship.execute_target_operation(
                'test.op1', kwargs={
                    'source': 'instance1',
                    'target': 'instance2'
                }).get()

        def op0(ctx, value, **_):
            ctx.instance.runtime_properties['key'] = value

        def op1(ctx, source, target, **_):
            self.assertEqual(source,
                             ctx.source.instance.runtime_properties['key'])
            self.assertEqual(target,
                             ctx.target.instance.runtime_properties['key'])

        self._execute_workflow(related_runtime_properties, operation_methods=[
            op0, op1])

    def test_operation_ctx_properties_and_methods(self):
        def flow(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.set_state('state').get()
            instance.execute_operation('test.op0').get()
            target_path = ctx.internal.handler.download_deployment_resource(
                'resource')
            with open(target_path) as f:
                self.assertEqual('content', f.read())

        def ctx_properties(ctx, **_):
            self.assertEqual('node', ctx.node.name)
            self.assertIn('node_', ctx.instance.id)
            self.assertEqual(self._testMethodName, ctx.blueprint.id)
            self.assertEqual(self._testMethodName, ctx.deployment.id)
            self.assertIsNotNone(ctx.execution_id)
            self.assertEqual('workflow0', ctx.workflow_id)
            self.assertIsNotNone(ctx.task_id)
            self.assertEqual('{0}.{1}'.format(self._testMethodName,
                                              'ctx_properties'),
                             ctx.task_name)
            self.assertIsNone(ctx.task_target)
            self.assertEqual('p', ctx.plugin)
            self.assertEqual('p', ctx.plugin.name)
            self.assertEqual(PLUGIN_PACKAGE_NAME, ctx.plugin.package_name)
            self.assertEqual(PLUGIN_PACKAGE_VERSION,
                             ctx.plugin.package_version)
            self.assertEqual(sys.prefix, ctx.plugin.prefix)
            self.assertEqual('test.op0', ctx.operation.name)
            self.assertThat(ctx.node.properties.items(),
                            ContainsAll({'property': 'value'}.items()))
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
        self._execute_workflow(flow, operation_methods=[ctx_properties])

    def test_ctx_host_ip(self):
        def op0(ctx, **_):
            ctx.instance.runtime_properties['ip'] = '2.2.2.2'

        def op1(ctx, expected_ip, **_):
            self.assertEqual(ctx.instance.host_ip, expected_ip)

        def flow(ctx, **_):
            instance1 = _instance(ctx, 'node')
            instance4 = _instance(ctx, 'node4')
            # these are hosts
            # in this one will will set a runtime_property of ip
            instance2 = _instance(ctx, 'node2')
            # this one has ip as static properties
            instance3 = _instance(ctx, 'node3')

            instance2.execute_operation('test.op0').get()
            instance1.execute_operation('test.op1', kwargs={
                'expected_ip': '2.2.2.2'
            }).get()
            instance2.execute_operation('test.op1', kwargs={
                'expected_ip': '2.2.2.2'
            }).get()
            instance3.execute_operation('test.op1', kwargs={
                'expected_ip': '1.1.1.1'
            }).get()
            instance4.execute_operation('test.op1', kwargs={
                'expected_ip': '1.1.1.1'
            }).get()

        self._execute_workflow(flow, operation_methods=[op0, op1])

    def test_operation_bootstrap_context(self):
        bootstrap_context = {'stub': 'prop'}
        provider_context = {
            'cloudify': bootstrap_context
        }

        def contexts(ctx, **_):
            self.assertEqual(bootstrap_context,
                             ctx.bootstrap_context._bootstrap_context)
            self.assertEqual(provider_context, ctx.provider_context)
        self._execute_workflow(operation_methods=[contexts],
                               provider_context=provider_context)

    def test_workflow_graph_mode(self):
        def flow(ctx, **_):
            instance = _instance(ctx, 'node')
            graph = ctx.graph_mode()
            sequence = graph.sequence()
            sequence.add(instance.execute_operation('test.op2'))
            sequence.add(instance.execute_operation('test.op1'))
            sequence.add(instance.execute_operation('test.op0'))
            graph.execute()

        def op0(ctx, **_):
            invocation = ctx.instance.runtime_properties['invocation']
            self.assertEqual(2, invocation)

        def op1(ctx, **_):
            invocation = ctx.instance.runtime_properties['invocation']
            self.assertEqual(1, invocation)
            ctx.instance.runtime_properties['invocation'] += 1

        def op2(ctx, **_):
            invocation = ctx.instance.runtime_properties.get('invocation')
            self.assertIsNone(invocation)
            ctx.instance.runtime_properties['invocation'] = 1

        self._execute_workflow(flow, operation_methods=[op0, op1, op2])

    def test_node_instance_version_conflict(self):
        def flow(ctx, **_):
            pass
        # stub to get a properly initialized storage instance
        self._execute_workflow(flow)
        storage = self.env.storage
        instance = storage.get_node_instances()[0]
        storage.update_node_instance(
            instance.id,
            runtime_properties={},
            state=instance.state,
            version=instance.version)
        instance_id = instance.id
        exception = Queue.Queue()
        done = Queue.Queue()

        def proceed():
            try:
                done.get_nowait()
                return False
            except Queue.Empty:
                return True

        def publisher(key, value):
            def func():
                timeout = time.time() + 5
                while time.time() < timeout and proceed():
                    p_instance = storage.get_node_instance(instance_id)
                    p_instance.runtime_properties[key] = value
                    try:
                        storage.update_node_instance(
                            p_instance.id,
                            runtime_properties=p_instance.runtime_properties,
                            state=p_instance.state,
                            version=p_instance.version)
                    except local.StorageConflictError, e:
                        exception.put(e)
                        done.put(True)
                        return
            return func

        publisher1 = publisher('publisher1', 'value1')
        publisher2 = publisher('publisher2', 'value2')

        publisher1_thread = threading.Thread(target=publisher1)
        publisher2_thread = threading.Thread(target=publisher2)

        publisher1_thread.daemon = True
        publisher2_thread.daemon = True

        publisher1_thread.start()
        publisher2_thread.start()

        publisher1_thread.join()
        publisher2_thread.join()

        conflict_error = exception.get_nowait()

        self.assertIn('does not match current', conflict_error.message)

    def test_get_node(self):
        def flow(ctx, **_):
            pass
        # stub to get a properly initialized storage instance
        self._execute_workflow(flow)
        storage = self.env.storage
        node = storage.get_node('node')
        self.assertEqual(node.properties['property'], 'value')

    def test_get_node_missing(self):
        def flow(ctx, **_):
            pass
        # stub to get a properly initialized storage instance
        self._execute_workflow(flow)
        storage = self.env.storage
        self.assertRaises(RuntimeError,
                          storage.get_node, 'node_that_does_not_exist')

    def test_execute_non_existent_operation(self):
        def flow(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('non_existent')
        with testtools.testcase.ExpectedException(RuntimeError,
                                                  ".*does not exist.*"):
            self._execute_workflow(flow)

    def test_operation_retry_configuration(self):
        self._test_retry_configuration_impl(
            global_retries=100,
            global_retry_interval=100,
            operation_retries=1,
            operation_retry_interval=1
        )


@nose.tools.istest
class LocalWorkflowTestInMemoryStorage(LocalWorkflowTest):

    def setUp(self):
        super(LocalWorkflowTestInMemoryStorage, self).setUp()
        self.storage_cls = local.InMemoryStorage


@nose.tools.istest
class LocalWorkflowTestFileStorage(LocalWorkflowTest):

    def setUp(self):
        super(LocalWorkflowTestFileStorage, self).setUp()
        self.storage_cls = local.FileStorage
        self.storage_kwargs = {'storage_dir': self.storage_dir}


@nose.tools.istest
class FileStorageTest(BaseWorkflowTest):

    def setUp(self):
        super(FileStorageTest, self).setUp()
        self.storage_cls = local.FileStorage
        self.storage_kwargs = {'storage_dir': self.storage_dir}

    def test_storage_dir(self):
        def stub_workflow(ctx, **_):
            pass
        self._execute_workflow(stub_workflow, name=self._testMethodName)
        self.assertTrue(os.path.isdir(
            os.path.join(self.storage_dir, self._testMethodName)))

    def test_persistency(self):
        bootstrap_context = {'stub': 'prop'}
        provider_context = {'cloudify': bootstrap_context}

        def persistency_1(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.set_state('persistency')
            instance.execute_operation('test.op0').get()

        def persistency_2(ctx, **_):
            instance = _instance(ctx, 'node')
            self.assertEqual('persistency', instance.get_state().get())
            instance.execute_operation('test.op0').get()

        def op(ctx, **_):
            self.assertEqual('new_input', ctx.node.properties['from_input'])
            self.assertEqual('content', ctx.get_resource('resource'))
            self.assertEqual(bootstrap_context,
                             ctx.bootstrap_context._bootstrap_context)
            self.assertEqual(provider_context, ctx.provider_context)

        self._setup_env(workflow_methods=[persistency_1, persistency_2],
                        operation_methods=[op],
                        inputs={'from_input': 'new_input'},
                        provider_context=provider_context)

        self._execute_workflow(workflow_name='workflow0',
                               setup_env=False, load_env=True)
        self._execute_workflow(workflow_name='workflow1',
                               setup_env=False, load_env=True)

    def test_path_agnostic_persistency(self):
        # tests file storage isn't dependent on the blueprint directory
        # for resources (but stores its own copies instead)
        def persistency(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('test.op0').get()

        def op(ctx, **_):
            self.assertEqual('new_input', ctx.node.properties['from_input'])
            self.assertEqual('content', ctx.get_resource('resource'))

        self._setup_env(workflow_methods=[persistency],
                        operation_methods=[op],
                        inputs={'from_input': 'new_input'})

        shutil.rmtree(self.blueprint_dir)

        self._execute_workflow(workflow_name='workflow0',
                               setup_env=False, load_env=True)

    def test_local_init_in_blueprint_dir(self):
        self.blueprint_dir = self.storage_dir

        def flow(ctx, **_):
            pass
        self._setup_env(workflow_methods=[flow])

    def test_workdir(self):
        content = 'CONTENT'

        def op0(ctx, **_):
            self.assertEquals(
                ctx.plugin.workdir,
                os.path.join(self.storage_dir, self._testMethodName,
                             'workdir', 'plugins', 'p'))

            work_file = os.path.join(ctx.plugin.workdir, 'work_file')
            self.assertFalse(os.path.exists(work_file))
            with open(work_file, 'w') as f:
                f.write(content)

        def op1(ctx, **_):
            work_file = os.path.join(ctx.plugin.workdir, 'work_file')
            with open(work_file) as f:
                print work_file
                self.assertEqual(content, f.read())

        def workflow1(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('test.op0').get()

        def workflow2(ctx, **_):
            instance = _instance(ctx, 'node')
            instance.execute_operation('test.op1').get()

        self._setup_env(workflow_methods=[workflow1, workflow2],
                        operation_methods=[op0, op1])

        self._execute_workflow(workflow_name='workflow0',
                               setup_env=False, load_env=True)
        self._execute_workflow(workflow_name='workflow1',
                               setup_env=False, load_env=True)


@nose.tools.istest
class LocalWorkflowEnvironmentTest(BaseWorkflowTest):

    def setUp(self):
        super(LocalWorkflowEnvironmentTest, self).setUp()
        self.storage_cls = local.InMemoryStorage

    def test_inputs(self):
        def op(ctx, **_):
            self.assertEqual('new_input', ctx.node.properties['from_input'])
        self._execute_workflow(operation_methods=[op],
                               inputs={'from_input': 'new_input'})

    def test_outputs(self):
        def op(ctx, **_):
            pass
        self._execute_workflow(operation_methods=[op],
                               use_existing_env=False)
        self.assertEqual(self.env.outputs(),
                         {'some_output': None, 'static': 'value'})

        def op(ctx, **_):
            ctx.instance.runtime_properties['some_output'] = 'value'
        self._execute_workflow(operation_methods=[op],
                               use_existing_env=False)
        self.assertEqual(self.env.outputs(),
                         {'some_output': 'value', 'static': 'value'})

    def test_workflow_return_value(self):
        def flow(ctx, **_):
            return 1
        self.assertEqual(1, self._execute_workflow(flow))

    def test_blueprint_imports(self):
        def flow(ctx, **_):
            node = ctx.get_node('node5')
            self.assertEqual('imported_type', node.type)
        self._execute_workflow(flow)

    def test_workflow_parameters(self):
        normal_schema = {
            'from_invocation': {},
            'from_default': {
                'default': 'from_default_default'
            },
            'invocation_overrides_default': {
                'default': 'invocation_overrides_default_default'
            }
        }

        normal_execute_kwargs = {
            'parameters': {
                'from_invocation': 'from_invocation',
                'invocation_overrides_default':
                'invocation_overrides_default_override'
            }
        }

        def normal_flow(ctx,
                        from_invocation,
                        from_default,
                        invocation_overrides_default,
                        **_):
            self.assertEqual(from_invocation, 'from_invocation')
            self.assertEqual(from_default, 'from_default_default')
            self.assertEqual(invocation_overrides_default,
                             'invocation_overrides_default_override')

        self._execute_workflow(normal_flow,
                               execute_kwargs=normal_execute_kwargs,
                               workflow_parameters_schema=normal_schema,
                               use_existing_env=False)

        # now test missing
        missing_schema = normal_schema.copy()
        missing_schema['missing_parameter'] = {}
        missing_flow = normal_flow
        missing_execute_kwargs = normal_execute_kwargs
        self.assertRaises(ValueError,
                          self._execute_workflow,
                          missing_flow,
                          execute_kwargs=missing_execute_kwargs,
                          workflow_parameters_schema=missing_schema,
                          use_existing_env=False)

        # now test invalid custom parameters
        invalid_custom_schema = normal_schema
        invalid_custom_flow = normal_flow
        invalid_custom_kwargs = normal_execute_kwargs.copy()
        invalid_custom_kwargs['parameters']['custom_parameter'] = 'custom'
        self.assertRaises(ValueError,
                          self._execute_workflow,
                          invalid_custom_flow,
                          execute_kwargs=invalid_custom_kwargs,
                          workflow_parameters_schema=invalid_custom_schema,
                          use_existing_env=False)

        # now test valid custom parameters
        def valid_custom_flow(ctx,
                              from_invocation,
                              from_default,
                              invocation_overrides_default,
                              custom_parameter,
                              **_):
            self.assertEqual(from_invocation, 'from_invocation')
            self.assertEqual(from_default, 'from_default_default')
            self.assertEqual(invocation_overrides_default,
                             'invocation_overrides_default_override')
            self.assertEqual(custom_parameter, 'custom')

        valid_custom_schema = normal_schema
        valid_custom_kwargs = normal_execute_kwargs.copy()
        valid_custom_kwargs['parameters']['custom_parameter'] = 'custom'
        valid_custom_kwargs['allow_custom_parameters'] = True
        self._execute_workflow(
            valid_custom_flow,
            execute_kwargs=valid_custom_kwargs,
            workflow_parameters_schema=valid_custom_schema,
            use_existing_env=False)

    def test_workflow_parameters_types(self):

        workflow = {
            'parameters': {
                'optional1': {'default': 7},
                'optional2': {'default': 'bla'},
                'optional_int1': {
                    'default': 1,
                    'type': 'integer'
                },
                'optional_int2': {
                    'default': 2,
                    'type': 'integer'
                },
                'optional_float1': {
                    'default': 1.5,
                    'type': 'float'
                },
                'optional_float2': {
                    'default': 2,
                    'type': 'float'
                },
                'optional_str1': {
                    'default': 'bla',
                    'type': 'string'
                },
                'optional_str2': {
                    'default': 'blabla',
                    'type': 'string'
                },
                'optional_bool1': {
                    'default': 'False',
                    'type': 'boolean'
                },
                'optional_bool2': {
                    'default': 'True',
                    'type': 'boolean'
                },
                'mandatory1': {},
                'mandatory2': {},
                'mandatory_int1': {'type': 'integer'},
                'mandatory_int2': {'type': 'integer'},
                'mandatory_float1': {'type': 'float'},
                'mandatory_float2': {'type': 'float'},
                'mandatory_str1': {'type': 'string'},
                'mandatory_str2': {'type': 'string'},
                'mandatory_bool1': {'type': 'boolean'},
                'mandatory_bool2': {'type': 'boolean'}
            }
        }

        self._test_workflow_mandatory_parameters_types(workflow)
        self._test_workflow_optional_parameters_types(workflow)
        self._test_workflow_custom_parameters_types(workflow)

    def _test_workflow_mandatory_parameters_types(self, workflow):
        parameters = {
            'mandatory1': 'bla',
            'mandatory2': 6,
            'mandatory_int1': 1,
            'mandatory_int2': 'bla',
            'mandatory_float1': 3.5,
            'mandatory_float2': True,
            'mandatory_str1': 'bla',
            'mandatory_str2': 7,
            'mandatory_bool1': False,
            'mandatory_bool2': 'boolean_that_is_not_string'
        }
        try:
            local._merge_and_validate_execution_parameters(
                workflow, 'workflow', parameters)
        except ValueError, e:
            # check which parameters are mentioned in the error message
            self.assertIn('mandatory_int2', str(e))
            self.assertIn('mandatory_float2', str(e))
            self.assertIn('mandatory_str2', str(e))
            self.assertIn('mandatory_bool2', str(e))
            self.assertNotIn('mandatory1', str(e))
            self.assertNotIn('mandatory2', str(e))
            self.assertNotIn('mandatory_int1', str(e))
            self.assertNotIn('mandatory_float1', str(e))
            self.assertNotIn('mandatory_str1', str(e))
            self.assertNotIn('mandatory_bool1', str(e))
        else:
            self.fail()

    def _test_workflow_optional_parameters_types(self, workflow):
        parameters = {
            'mandatory1': False,
            'mandatory2': [],
            'mandatory_int1': '-7',
            'mandatory_int2': 3.5,
            'mandatory_float1': '5.0',
            'mandatory_float2': [],
            'mandatory_str1': u'bla',
            'mandatory_str2': ['bla'],
            'mandatory_bool1': 'tRUe',
            'mandatory_bool2': 0,
            'optional1': 'bla',
            'optional2': 6,
            'optional_int1': 1,
            'optional_int2': 'bla',
            'optional_float1': 3.5,
            'optional_float2': True,
            'optional_str1': 'bla',
            'optional_str2': 7,
            'optional_bool1': False,
            'optional_bool2': 'bla'
        }
        try:
            local._merge_and_validate_execution_parameters(
                workflow, 'workflow', parameters)
        except ValueError, e:
            # check which parameters are mentioned in the error message
            self.assertIn('mandatory_int2', str(e))
            self.assertIn('mandatory_float2', str(e))
            self.assertIn('mandatory_str2', str(e))
            self.assertIn('mandatory_bool2', str(e))
            self.assertNotIn('mandatory1', str(e))
            self.assertNotIn('mandatory2', str(e))
            self.assertNotIn('mandatory_int1', str(e))
            self.assertNotIn('mandatory_float1', str(e))
            self.assertNotIn('mandatory_str1', str(e))
            self.assertNotIn('mandatory_bool1', str(e))

            self.assertIn('optional_int2', str(e))
            self.assertIn('optional_float2', str(e))
            self.assertIn('optional_str2', str(e))
            self.assertIn('optional_bool2', str(e))
            self.assertNotIn('optional1', str(e))
            self.assertNotIn('optional2', str(e))
            self.assertNotIn('optional_int1', str(e))
            self.assertNotIn('optional_float1', str(e))
            self.assertNotIn('optional_str1', str(e))
            self.assertNotIn('optional_bool1', str(e))
        else:
            self.fail()

    def _test_workflow_custom_parameters_types(self, workflow):
        parameters = {
            'mandatory1': False,
            'mandatory2': [],
            'mandatory_int1': -7,
            'mandatory_int2': 3,
            'mandatory_float1': 5.0,
            'mandatory_float2': 0.0,
            'mandatory_str1': u'bla',
            'mandatory_str2': 'bla',
            'mandatory_bool1': True,
            'mandatory_bool2': False,
            'optional1': 'bla',
            'optional2': 6,
            'optional_int1': 1,
            'optional_int2': 'bla',
            'optional_float1': 3.5,
            'optional_str1': 'bla',
            'optional_bool1': 'falSE',
            'custom1': 8,
            'custom2': 3.2,
            'custom3': 'bla',
            'custom4': True
        }
        try:
            local._merge_and_validate_execution_parameters(
                workflow, 'workflow', parameters, True)
        except ValueError, e:
            # check which parameters are mentioned in the error message
            self.assertNotIn('mandatory_int2', str(e))
            self.assertNotIn('mandatory_float2', str(e))
            self.assertNotIn('mandatory_str2', str(e))
            self.assertNotIn('mandatory_bool2', str(e))
            self.assertNotIn('mandatory1', str(e))
            self.assertNotIn('mandatory2', str(e))
            self.assertNotIn('mandatory_int1', str(e))
            self.assertNotIn('mandatory_float1', str(e))
            self.assertNotIn('mandatory_str1', str(e))
            self.assertNotIn('mandatory_bool1', str(e))

            self.assertIn('optional_int2', str(e))
            self.assertNotIn('optional_float2', str(e))
            self.assertNotIn('optional_str2', str(e))
            self.assertNotIn('optional_bool2', str(e))
            self.assertNotIn('optional1', str(e))
            self.assertNotIn('optional2', str(e))
            self.assertNotIn('optional_int1', str(e))
            self.assertNotIn('optional_float1', str(e))
            self.assertNotIn('optional_str1', str(e))
            self.assertNotIn('optional_bool1', str(e))

            self.assertNotIn('custom1', str(e))
            self.assertNotIn('custom2', str(e))
            self.assertNotIn('custom3', str(e))
            self.assertNotIn('custom4', str(e))
        else:
            self.fail()

    def test_global_retry_configuration(self):
        self._test_retry_configuration_impl(
            global_retries=1,
            global_retry_interval=1,
            operation_retries=None,
            operation_retry_interval=None
        )

    def test_local_task_thread_pool_size(self):
        default_size = workflow_context.DEFAULT_LOCAL_TASK_THREAD_POOL_SIZE

        def flow(ctx, **_):
            task_processor = ctx.internal.local_tasks_processor
            self.assertEqual(len(task_processor._local_task_processing_pool),
                             default_size)
        self._execute_workflow(
            flow,
            use_existing_env=False)

        def flow(ctx, **_):
            task_processor = ctx.internal.local_tasks_processor
            self.assertEqual(len(task_processor._local_task_processing_pool),
                             default_size + 1)
        self._execute_workflow(
            flow,
            execute_kwargs={'task_thread_pool_size': default_size + 1},
            use_existing_env=False)

    def test_no_operation_module(self):
        self._no_module_or_attribute_test(
            is_missing_module=True,
            test_type='operation')

    def test_no_operation_module_ignored(self):
        def op1(ctx, **_):
            pass

        self._execute_workflow(operation_methods=[op1],
                               ignored_modules=['ignored_module'])

    def test_no_operation_attribute(self):
        self._no_module_or_attribute_test(
            is_missing_module=False,
            test_type='operation')

    def test_no_source_operation_module(self):
        self._no_module_or_attribute_test(
            is_missing_module=True,
            test_type='source')

    def test_no_source_operation_attribute(self):
        self._no_module_or_attribute_test(
            is_missing_module=False,
            test_type='source')

    def test_no_target_operation_module(self):
        self._no_module_or_attribute_test(
            is_missing_module=True,
            test_type='target')

    def test_no_target_operation_attribute(self):
        self._no_module_or_attribute_test(
            is_missing_module=False,
            test_type='target')

    def test_no_workflow_module(self):
        self._no_module_or_attribute_test(
            is_missing_module=True,
            test_type='workflow')

    def test_no_workflow_attribute(self):
        self._no_module_or_attribute_test(
            is_missing_module=False,
            test_type='workflow')

    def test_no_workflow(self):
        try:
            self._execute_workflow(workflow_name='does_not_exist')
            self.fail()
        except ValueError, e:
            self.assertIn("['workflow0']", e.message)

    def test_getting_contained_elements(self):
        def check_subgraph(ctx, **_):
            node_host = _instance(ctx, 'node_host')
            node = _instance(ctx, 'node')
            node2 = _instance(ctx, 'node2')
            node3 = _instance(ctx, 'node3')
            node4 = _instance(ctx, 'node4')

            full_contained_subgraph = set([
                node_host,
                node,
                node2,
                node3,
                node4
            ])
            self.assertEqual(
                full_contained_subgraph,
                node_host.get_contained_subgraph()
            )

            half_subgraph = set([
                node,
                node2
            ])
            self.assertEqual(
                half_subgraph,
                node2.get_contained_subgraph()
            )

            host_contained_instances = set([
                node2,
                node3
            ])
            self.assertEqual(
                host_contained_instances,
                set(node_host.contained_instances)
            )

            self.assertEqual(
                [],
                node.contained_instances
            )

        self._execute_workflow(
            check_subgraph,
            create_blueprint_func=self._blueprint_3
        )

    def _no_module_or_attribute_test(self, is_missing_module, test_type):
        try:
            self._execute_workflow(
                create_blueprint_func=self._blueprint_2(is_missing_module,
                                                        test_type),
                workflow_name='workflow')
            self.fail()
        except (ImportError, AttributeError, NonRecoverableError) as e:
            if is_missing_module:
                self.assertIn('No module named zzz', e.message)
                if test_type != 'workflow':
                    self.assertIn(test_type, e.message)
                    self.assertTrue(isinstance(e, ImportError))
            else:
                if test_type == 'workflow':
                    thing1 = 'function'
                    thing2 = ' named'
                else:
                    thing1 = 'attribute'
                    thing2 = ''
                self.assertIn("has no {0}{1} 'does_not_exist'".format(thing1,
                                                                      thing2),
                              e.message)
                if test_type != 'workflow':
                    self.assertIn(test_type, e.message)
                    self.assertTrue(isinstance(e, AttributeError))

    def _blueprint_2(self,
                     is_missing_module,
                     test_type):
        def func(*_):
            module_name = 'zzz' if is_missing_module else self._testMethodName
            interfaces = {
                'test': {
                    'op': 'p.{0}.{1}'.format(module_name, 'does_not_exist')
                }
            }
            blueprint = {
                'tosca_definitions_version': 'cloudify_dsl_1_0',
                'plugins': {
                    'p': {
                        'executor': 'central_deployment_agent',
                        'install': False
                    }
                },
                'node_types': {
                    'type': {}
                },
                'relationships': {
                    'cloudify.relationships.contained_in': {}
                },
                'node_templates': {
                    'node2': {
                        'type': 'type',
                    },
                    'node': {
                        'type': 'type',
                        'relationships': [{
                            'target': 'node2',
                            'type': 'cloudify.relationships.contained_in',
                        }]
                    },
                },
                'workflows': {
                    'workflow': 'p.{0}.{1}'.format(module_name,
                                                   'does_not_exist')
                }
            }

            node = blueprint['node_templates']['node']
            relationship = node['relationships'][0]
            if test_type == 'operation':
                node['interfaces'] = interfaces
            elif test_type == 'source':
                relationship['source_interfaces'] = interfaces
            elif test_type == 'target':
                relationship['target_interfaces'] = interfaces
            elif test_type == 'workflow':
                pass
            else:
                self.fail('unsupported: {}'.format(test_type))

            return blueprint
        return func

    def _blueprint_3(self, workflow_methods, _,
                     workflow_parameters_schema, __, *args):
        workflows = dict((
            ('workflow{0}'.format(index), {
                'mapping': 'p.{0}.{1}'.format(self._testMethodName,
                                              w_method.__name__),
                'parameters': workflow_parameters_schema or {}
            }) for index, w_method in enumerate(workflow_methods)
        ))

        blueprint = {
            'tosca_definitions_version': 'cloudify_dsl_1_0',
            'plugins': {
                'p': {
                    'executor': 'central_deployment_agent',
                    'install': False
                }
            },
            'node_types': {
                'type': {},
            },
            'relationships': {
                'cloudify.relationships.contained_in': {}
            },
            'node_templates': {
                'node_host': {
                    'type': 'type'
                },
                'node4': {
                    'type': 'type',
                    'relationships': [{
                        'target': 'node3',
                        'type': 'cloudify.relationships.contained_in',
                    }]
                },
                'node3': {
                    'type': 'type',
                    'relationships': [{
                        'target': 'node_host',
                        'type': 'cloudify.relationships.contained_in',
                    }]
                },
                'node2': {
                    'type': 'type',
                    'relationships': [{
                        'target': 'node_host',
                        'type': 'cloudify.relationships.contained_in',
                    }]
                },
                'node': {
                    'type': 'type',
                    'relationships': [{
                        'target': 'node2',
                        'type': 'cloudify.relationships.contained_in',
                    }]
                },
                'outside_node': {
                    'type': 'type'
                }
            },
            'workflows': workflows
        }
        return blueprint


def _instance(ctx, node_name):
    return next(ctx.get_node(node_name).instances)
