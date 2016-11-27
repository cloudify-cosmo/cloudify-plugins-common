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


import time
from os import path

import testtools
from testtools.matchers import MatchesAny, Equals, GreaterThan
from nose.tools import nottest

from cloudify import exceptions
from cloudify.plugins import lifecycle
from cloudify.decorators import operation
from cloudify.test_utils import workflow_test


class GlobalCounter(object):
    def __init__(self):
        self.count = 0

    def get_and_increment(self):
        result = self.count
        self.count += 1
        return result


global_counter = GlobalCounter()


class TestExecuteOperationWorkflow(testtools.TestCase):
    execute_blueprint_path = path.join('resources', 'blueprints',
                                       'execute_operation.yaml')

    @workflow_test(execute_blueprint_path)
    def test_execute_operation(self, cfy_local):
        params = self._get_params()
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 4)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_default_values(self, cfy_local):
        params = {'operation': 'cloudify.interfaces.lifecycle.create'}
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 4)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_with_operation_parameters(self, cfy_local):
        self._test_execute_operation_with_op_params(cfy_local,
                                                    'cloudify.interfaces.'
                                                    'lifecycle.create')

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_with_op_params_and_kwargs_override_allowed(
            self, cfy_local):
        self._test_execute_operation_with_op_params(cfy_local,
                                                    'cloudify.interfaces.'
                                                    'lifecycle.configure',
                                                    True)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_with_op_params_and_kwargs_override_disallowed(
            self, cfy_local):
        self._test_exec_op_with_params_and_no_kwargs_override(cfy_local, False)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_with_op_params_and_default_kwargs_override(
            self, cfy_local):
        # testing kwargs override with the default value for the
        # 'allow_kwargs_override' parameter (null/None)
        self._test_exec_op_with_params_and_no_kwargs_override(cfy_local, None)

    def _test_exec_op_with_params_and_no_kwargs_override(self, cfy_local,
                                                         kw_over_val):
        try:
            self._test_execute_operation_with_op_params(cfy_local,
                                                        'cloudify.interfaces.'
                                                        'lifecycle.configure',
                                                        kw_over_val)
            self.fail('expected kwargs override to be disallowed')
        except RuntimeError, e:
            self.assertIn(
                'To allow redefinition, pass "allow_kwargs_override"', str(e))

    def _test_execute_operation_with_op_params(self, cfy_local, op,
                                               allow_kw_override=None):
        operation_param_key = 'operation_param_key'
        operation_param_value = 'operation_param_value'
        op_params = {operation_param_key: operation_param_value}

        params = self._get_params(op=op, op_params=op_params,
                                  allow_kw_override=allow_kw_override)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 4)

        instances = cfy_local.storage.get_node_instances()
        for instance in instances:
            self.assertIn('op_kwargs', instance.runtime_properties)
            op_kwargs = instance.runtime_properties['op_kwargs']
            self.assertIn(operation_param_key, op_kwargs)
            self.assertEquals(operation_param_value,
                              op_kwargs[operation_param_key])

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_by_nodes(self, cfy_local):
        node_ids = ['node2', 'node3']
        params = self._get_params(node_ids=node_ids)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 3, node_ids=node_ids)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_by_node_instances(self, cfy_local):
        instances = cfy_local.storage.get_node_instances()
        node_instance_ids = [instances[0].id, instances[3].id]
        params = self._get_params(node_instance_ids=node_instance_ids)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 2,
                                     node_instance_ids=node_instance_ids)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_by_type_names(self, cfy_local):
        type_names = ['mock_type2']
        params = self._get_params(type_names=type_names)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 3, type_names=type_names)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_by_nodes_and_types(self, cfy_local):
        node_ids = ['node1', 'node2']
        type_names = ['mock_type2']
        params = self._get_params(node_ids=node_ids, type_names=type_names)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 2, node_ids=node_ids,
                                     type_names=type_names)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_by_nodes_types_and_node_instances(self,
                                                                 cfy_local):
        node_ids = ['node2', 'node3']
        type_names = ['mock_type2', 'mock_type1']
        instances = cfy_local.storage.get_node_instances()
        node_instance_ids = [next(inst.id for inst in instances if
                                  inst.node_id == 'node2')]
        params = self._get_params(node_ids=node_ids,
                                  node_instance_ids=node_instance_ids,
                                  type_names=type_names)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 1, node_ids=node_ids,
                                     node_instance_ids=node_instance_ids,
                                     type_names=type_names)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_empty_intersection(self, cfy_local):
        node_ids = ['node1', 'node2']
        type_names = ['mock_type3']
        params = self._get_params(node_ids=node_ids, type_names=type_names)
        cfy_local.execute('execute_operation', params)
        self._make_filter_assertions(cfy_local, 0, node_ids=node_ids,
                                     type_names=type_names)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_with_dependency_order(self, cfy_local):
        time_diff_assertions_pairs = [
            (0, 1),  # node 1 instance and node 2 instance
            (0, 2),  # node 1 instance and node 2 instance
            (1, 3),  # node 2 instance and node 3 instance
            (2, 3)  # node 2 instance and node 3 instance
        ]

        self._dep_order_tests_helper(cfy_local, [],
                                     ['node1', 'node2', 'node2', 'node3'],
                                     time_diff_assertions_pairs)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_with_indirect_dependency_order(self, cfy_local):
        time_diff_assertions_pairs = [
            (0, 1),  # node 1 instance and node 3 instance
        ]

        self._dep_order_tests_helper(cfy_local,
                                     ['node1', 'node3'],
                                     ['node1', 'node3'],
                                     time_diff_assertions_pairs)

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_failure_zero_subgraph_retries(self, cfy_local):
        self._test_retries(cfy_local,
                           op='test.fail',
                           count=2,
                           subgraph_retries=0,
                           expected_str='test_builtin_workflows.fail')

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_retry_zero_subgraph_retries(self, cfy_local):
        self._test_retries(cfy_local,
                           op='test.retry',
                           count=2,
                           subgraph_retries=0,
                           expected_str='test_builtin_workflows.retry')

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_failure_one_subgraph_retry(self, cfy_local):
        # no subgraph retry logic is implemented for execute_operation
        # node instances subgraphs, so we apply the same assertions
        # like previous tests
        self._test_retries(cfy_local,
                           op='test.fail',
                           count=2,
                           subgraph_retries=1,
                           expected_str='test_builtin_workflows.fail')

    @workflow_test(execute_blueprint_path)
    def test_execute_operation_retry_one_subgraph_retry(self, cfy_local):
        # no subgraph retry logic is implemented for execute_operation
        # node instances subgraphs, so we apply the same assertions
        # like previous tests
        self._test_retries(cfy_local,
                           op='test.retry',
                           count=2,
                           subgraph_retries=1,
                           expected_str='test_builtin_workflows.retry')

    def _test_retries(self, cfy_local,
                      op, count, subgraph_retries, expected_str):
        params = self._get_params(op=op,
                                  op_params={'count': count},
                                  node_ids=['node1'])
        e = self.assertRaises(RuntimeError,
                              cfy_local.execute,
                              'execute_operation', params,
                              task_retries=1,
                              task_retry_interval=0,
                              subgraph_retries=subgraph_retries)
        instance = cfy_local.storage.get_node_instances('node1')[0]
        invocations = instance.runtime_properties.get('invocations', [])
        self.assertEqual(len(invocations), 2)
        self.assertIn(expected_str, str(e))

    def _make_filter_assertions(self, cfy_local,
                                expected_num_of_visited_instances,
                                node_ids=None, node_instance_ids=None,
                                type_names=None):
        num_of_visited_instances = 0
        instances = cfy_local.storage.get_node_instances()
        nodes_by_id = dict((node.id, node) for node in
                           cfy_local.storage.get_nodes())

        for inst in instances:
            test_op_visited = inst.runtime_properties.get('test_op_visited')

            if (not node_ids or inst.node_id in node_ids) \
                    and \
                    (not node_instance_ids or inst.id in node_instance_ids) \
                    and \
                    (not type_names or (next((type for type in nodes_by_id[
                        inst.node_id].type_hierarchy if type in type_names),
                    None))):
                self.assertTrue(test_op_visited)
                num_of_visited_instances += 1
            else:
                self.assertIsNone(test_op_visited)

        # this is actually an assertion to ensure the tests themselves are ok
        self.assertEquals(expected_num_of_visited_instances,
                          num_of_visited_instances)

    def _dep_order_tests_helper(self, cfy_local, node_ids_param,
                                ordered_node_ids_of_instances,
                                indices_pairs_for_time_diff_assertions):
        params = self._get_params(
            op='cloudify.interfaces.lifecycle.start',
            node_ids=node_ids_param,
            run_by_dep=True)
        cfy_local.execute('execute_operation', params, task_thread_pool_size=4)

        instances_and_visit_times = sorted(
            ((inst, inst.runtime_properties['visit_time']) for inst in
             cfy_local.storage.get_node_instances() if 'visit_time' in
             inst.runtime_properties),
            key=lambda inst_and_time: inst_and_time[1])

        self.assertEqual(ordered_node_ids_of_instances,
                         [inst_and_time[0].node_id for inst_and_time in
                          instances_and_visit_times])

        # asserting time difference between the operation execution for the
        # different nodes. this way if something breaks and the tasks aren't
        # dependent on one another, there's a better chance we'll catch
        # it, since even if the order of the visits happens to be correct,
        # it's less likely there'll be a significant time difference between
        # the visits
        def assert_time_difference(earlier_inst_index, later_inst_index):
            td = instances_and_visit_times[later_inst_index][1] - \
                instances_and_visit_times[earlier_inst_index][1]
            self.assertThat(td, MatchesAny(Equals(1), GreaterThan(1)))

        for index1, index2 in indices_pairs_for_time_diff_assertions:
            assert_time_difference(index1, index2)

    def _get_params(self, op='cloudify.interfaces.lifecycle.create',
                    op_params=None, run_by_dep=False,
                    allow_kw_override=None, node_ids=None,
                    node_instance_ids=None, type_names=None):
        return {
            'operation': op,
            'operation_kwargs': op_params or {},
            'run_by_dependency_order': run_by_dep,
            'allow_kwargs_override': allow_kw_override,
            'node_ids': node_ids or [],
            'node_instance_ids': node_instance_ids or [],
            'type_names': type_names or []
        }


class TestScale(testtools.TestCase):
    scale_blueprint_path = path.join('resources', 'blueprints',
                                     'test-scale-blueprint.yaml')

    @workflow_test(scale_blueprint_path)
    def test_delta_str_int_conversion(self, cfy_local):
        cfy_local.execute('scale', parameters={'scalable_entity_name': 'node',
                                               'delta': '0'})

    @workflow_test(scale_blueprint_path)
    def test_no_node(self, cfy_local):
        with testtools.ExpectedException(ValueError, ".*mock was found.*"):
            cfy_local.execute(
                'scale', parameters={'scalable_entity_name': 'mock'})
        with testtools.ExpectedException(ValueError, ".*mock was found.*"):
            cfy_local.execute('scale_old', parameters={'node_id': 'mock'})

    @workflow_test(scale_blueprint_path)
    def test_zero_delta(self, cfy_local):
        # should simply work
        cfy_local.execute('scale', parameters={'scalable_entity_name': 'node',
                                               'delta': 0})
        cfy_local.execute('scale_old', parameters={'node_id': 'node',
                                                   'delta': 0})

    @workflow_test(scale_blueprint_path)
    def test_illegal_delta(self, cfy_local):
        with testtools.ExpectedException(ValueError, ".*-2 is illegal.*"):
            cfy_local.execute('scale', parameters={
                'scalable_entity_name': 'node',
                'delta': -2})
        with testtools.ExpectedException(ValueError, ".*-2 is illegal.*"):
            cfy_local.execute('scale_old', parameters={'node_id': 'node',
                                                       'delta': -2})

    @workflow_test(scale_blueprint_path)
    def test_illegal_str_delta(self, cfy_local):
        with testtools.ExpectedException(ValueError, ".*must be a number.*"):
            cfy_local.execute('scale',
                              parameters={'scalable_entity_name': 'node',
                                          'delta': 'not a number'})


class TestSubgraphWorkflowLogic(testtools.TestCase):

    @workflow_test(path.join('resources', 'blueprints',
                             'test-subgraph-blueprint.yaml'))
    def test_heal_connected_to_relationship_operations_on_on_affected(self,
                                                                      cfy_local
                                                                      ):
        # Tests CFY-2788 fix
        # We run heal on node2 instance. node1 is connected to node2 and node3
        # we expect that the establish/unlink operations will only be called
        # for node1->node2
        node2_instance_id = [i for i in cfy_local.storage.get_node_instances()
                             if i.node_id == 'node2'][0].id
        cfy_local.execute('heal', parameters={
            'node_instance_id': node2_instance_id})
        node1_instance = [i for i in cfy_local.storage.get_node_instances()
                          if i.node_id == 'node1'][0]
        invocations = node1_instance.runtime_properties['invocations']
        self.assertEqual(4, len(invocations))
        expected_unlink = invocations[:2]
        expected_establish = invocations[2:]

        def assertion(actual_invocations, expected_op):
            has_source_op = False
            has_target_op = False
            for invocation in actual_invocations:
                if invocation['runs_on'] == 'source':
                    has_source_op = True
                elif invocation['runs_on'] == 'target':
                    has_target_op = True
                else:
                    self.fail('Unhandled runs_on: {0}'.format(
                        invocation['runs_on']))
                self.assertEqual(invocation['target_node'], 'node2')
                self.assertEqual(invocation['operation'], expected_op)
            self.assertTrue(all([has_source_op, has_target_op]))

        assertion(expected_unlink,
                  'cloudify.interfaces.relationship_lifecycle.unlink')
        assertion(expected_establish,
                  'cloudify.interfaces.relationship_lifecycle.establish')

    @workflow_test(path.join('resources', 'blueprints',
                             'test-heal-correct-order-blueprint.yaml'))
    def test_heal_correct_order(self, env):
        env.execute('heal', parameters={
            'node_instance_id': env.storage.get_node_instances(
                node_id='node1')[0].id})
        all_invocations = []
        for instance in env.storage.get_node_instances():
            invocations = instance.runtime_properties.get('invocations', [])
            all_invocations += invocations
        sorted_invocations = sorted(all_invocations,
                                    key=lambda i: i['counter'])

        def assert_op(invocation, node_id, op):
            self.assertEqual(invocation['node_id'], node_id)
            self.assertEqual(invocation['operation'].split('.')[-1], op)
        assert_op(sorted_invocations[0], 'node2', 'stop')
        assert_op(sorted_invocations[1], 'node3', 'unlink')
        assert_op(sorted_invocations[2], 'node3', 'establish')
        assert_op(sorted_invocations[3], 'node2', 'create')

    @workflow_test(path.join(
        'resources', 'blueprints', 'test-blueprint-ignore-failure.yaml'))
    def test_heal_correct_order_ignore_failure_false(self, env):
        try:
            env.execute('heal', parameters={
                'ignore_failure': False,
                'node_instance_id': env.storage.get_node_instances(
                    node_id='node1')[0].id})
        except RuntimeError:
            all_invocations = []
            for instance in env.storage.get_node_instances():
                invocations = instance.runtime_properties.get(
                    'invocations', [])
                all_invocations += invocations
            self.assertEqual(1, len(all_invocations))
        else:
            fail()

    @workflow_test(path.join(
        'resources', 'blueprints', 'test-blueprint-ignore-failure.yaml'))
    def test_heal_correct_order_ignore_failure_true(self, env):
        env.execute('heal', parameters={
            'ignore_failure': True,
            'node_instance_id': env.storage.get_node_instances(
                node_id='node1')[0].id})
        all_invocations = []
        for instance in env.storage.get_node_instances():
            invocations = instance.runtime_properties.get('invocations', [])
            all_invocations += invocations
        self.assertEqual(4, len(all_invocations))


def mock_uninstall(graph, node_instances, ignore_failure, related_nodes=None):
    raise RuntimeError('Test - ignore_failure is: ' + str(ignore_failure))


class TestUpdateIgnoreFailure(testtools.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestUpdateIgnoreFailure, self).__init__(*args, **kwargs)
        self.original_uninstall = lifecycle.uninstall_node_instances

    def setUp(self):
        super(TestUpdateIgnoreFailure, self).setUp()
        lifecycle.uninstall_node_instances = mock_uninstall

    def tearDown(self):
        lifecycle.uninstall_node_instances = self.original_uninstall
        super(TestUpdateIgnoreFailure, self).tearDown()

    @workflow_test(path.join(
        'resources', 'blueprints', 'test-blueprint-ignore-failure.yaml'))
    def test_update_ignore_failure_false(self, env):
        try:
            env.execute('update', parameters={
                'ignore_failure': False,
                'modified_entity_ids': {'relationship': ''}
            })
        except RuntimeError as e:
            self.assertEqual('Test - ignore_failure is: False', str(e))
        else:
            fail()

    @workflow_test(path.join(
        'resources', 'blueprints', 'test-blueprint-ignore-failure.yaml'))
    def test_update_ignore_failure_true(self, env):
        try:
            env.execute('update', parameters={
                'ignore_failure': True,
                'modified_entity_ids': {'relationship': ''}
            })
        except RuntimeError as e:
            self.assertEqual('Test - ignore_failure is: True', str(e))
        else:
            fail()


class TestRelationshipOrderInLifecycleWorkflows(testtools.TestCase):

    blueprint_path = path.join('resources', 'blueprints',
                               'test-relationship-order-blueprint.yaml')

    @workflow_test(blueprint_path)
    def test_install_uninstall_and_heal_relationships_order(self, env):
        self.env = env

        self._assert_invocation_order(
            workflow='install',
            expected_invocations=[
                ('main',    'establish', 'node1'),
                ('main',    'establish', 'node2'),
                ('main',    'establish', 'main_compute'),
                ('main',    'establish', 'node4'),
                ('main',    'establish', 'node5'),
                ('depends', 'establish', 'main_compute'),
                ('depends', 'establish', 'main')
            ])

        self._assert_invocation_order(
            workflow='heal',
            expected_invocations=[
                ('depends', 'unlink', 'main'),
                ('depends', 'unlink', 'main_compute'),
                ('main',    'unlink', 'node5'),
                ('main',    'unlink', 'node4'),
                ('main',    'unlink', 'main_compute'),
                ('main',    'unlink', 'node2'),
                ('main',    'unlink', 'node1'),
                ('main',    'establish', 'node1'),
                ('main',    'establish', 'node2'),
                ('main',    'establish', 'main_compute'),
                ('main',    'establish', 'node4'),
                ('main',    'establish', 'node5'),
                ('depends', 'establish', 'main_compute'),
                ('depends', 'establish', 'main')
            ])

        self._assert_invocation_order(
            workflow='uninstall',
            expected_invocations=[
                ('depends', 'unlink', 'main'),
                ('depends', 'unlink', 'main_compute'),
                ('main',    'unlink', 'node5'),
                ('main',    'unlink', 'node4'),
                ('main',    'unlink', 'main_compute'),
                ('main',    'unlink', 'node2'),
                ('main',    'unlink', 'node1'),
            ])

    def _assert_invocation_order(self, workflow, expected_invocations):
        parameters = {}
        if workflow == 'heal':
            parameters = {
                'node_instance_id': self._get_node_instance('main').id
            }
        elif workflow == 'uninstall':
            parameters = {
                'ignore_failure': True
            }
        self.env.execute(workflow, parameters=parameters)
        main_instance = self._get_node_instance('main')
        depends_on_main = self._get_node_instance('depends')
        invocations = main_instance.runtime_properties['invocations']
        invocations += depends_on_main.runtime_properties['invocations']
        invocations.sort(key=lambda i: i['counter'])
        for index, (node_id, op, target) in enumerate(expected_invocations):
            invocation = invocations[index]
            self.assertEqual(invocation['node_id'], node_id)
            self.assertEqual(invocation['operation'].split('.')[-1], op)
            self.assertEqual(invocation['target_node'], target)
        for instance in self.env.storage.get_node_instances():
            self.env.storage.update_node_instance(instance.id,
                                                  instance.version,
                                                  runtime_properties={})

    def _get_node_instance(self, node_id):
        return self.env.storage.get_node_instances(node_id=node_id)[0]


@nottest
@operation
def exec_op_test_operation(ctx, **kwargs):
    ctx.instance.runtime_properties['test_op_visited'] = True
    if kwargs:
        ctx.instance.runtime_properties['op_kwargs'] = kwargs


@nottest
@operation
def exec_op_dependency_order_test_operation(ctx, **kwargs):
    ctx.instance.runtime_properties['visit_time'] = time.time()
    time.sleep(1)


@operation
def source_operation(ctx, **_):
    _write_rel_operation(ctx, runs_on='source')


@operation
def target_operation(ctx, **_):
    _write_rel_operation(ctx, runs_on='target')


@operation
def node_operation(ctx, **_):
    _write_operation(ctx)


@operation
def fail_stop(ctx, **_):
    _write_operation(ctx)
    raise exceptions.NonRecoverableError('')


@operation
def fail(ctx, count, **_):
    _write_operation(ctx)
    current_count = ctx.instance.runtime_properties.get('current_count', 0)
    if current_count < count:
        ctx.instance.runtime_properties['current_count'] = current_count + 1
        raise RuntimeError('EXPECTED TEST FAILURE')


@operation
def retry(ctx, count, **_):
    _write_operation(ctx)
    current_count = ctx.instance.runtime_properties.get('current_count', 0)
    if current_count < count:
        ctx.instance.runtime_properties['current_count'] = current_count + 1
        return ctx.operation.retry()


def _write_operation(ctx):
    invocations = ctx.instance.runtime_properties.get('invocations', [])
    invocations.append({
        'node_id': ctx.node.id,
        'operation': ctx.operation.name,
        'counter': global_counter.get_and_increment()
    })
    ctx.instance.runtime_properties['invocations'] = invocations


def _write_rel_operation(ctx, runs_on):
    invocations = ctx.source.instance.runtime_properties.get('invocations', [])
    invocations.append({
        'node_id': ctx.source.node.id,
        'operation': ctx.operation.name,
        'target_node': ctx.target.node.name,
        'runs_on': runs_on,
        'counter': global_counter.get_and_increment()})
    ctx.source.instance.runtime_properties['invocations'] = invocations
