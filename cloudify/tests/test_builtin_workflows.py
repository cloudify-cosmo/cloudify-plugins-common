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
import time

import testtools
from testtools.matchers import MatchesAny, Equals, GreaterThan
from nose.tools import nottest

from cloudify.workflows import local
from cloudify.decorators import operation


IGNORED_LOCAL_WORKFLOW_MODULES = (
    'cloudify_agent.operations',
    'cloudify_agent.installer.operations',

    # maintained for backward compatibily with < 3.3 blueprints
    'worker_installer.tasks',
    'plugin_installer.tasks',
    'windows_agent_installer.tasks',
    'windows_plugin_installer.tasks'
)


class TestExecuteOperationWorkflow(testtools.TestCase):

    def setUp(self):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/execute_operation.yaml")
        self.env = local.init_env(blueprint_path)
        super(TestExecuteOperationWorkflow, self).setUp()

    def test_execute_operation(self):
        params = self._get_params()
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(4)

    def test_execute_operation_default_values(self):
        params = {'operation': 'cloudify.interfaces.lifecycle.create'}
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(4)

    def test_execute_operation_with_operation_parameters(self):
        self._test_execute_operation_with_op_params(
            'cloudify.interfaces.lifecycle.create')

    def test_execute_operation_with_op_params_and_kwargs_override_allowed(
            self):
        self._test_execute_operation_with_op_params(
            'cloudify.interfaces.lifecycle.configure', True)

    def test_execute_operation_with_op_params_and_kwargs_override_disallowed(
            self):
        self._test_exec_op_with_params_and_no_kwargs_override(False)

    def test_execute_operation_with_op_params_and_default_kwargs_override(
            self):
        # testing kwargs override with the default value for the
        # 'allow_kwargs_override' parameter (null/None)
        self._test_exec_op_with_params_and_no_kwargs_override(None)

    def _test_exec_op_with_params_and_no_kwargs_override(self, kw_over_val):
        try:
            self._test_execute_operation_with_op_params(
                'cloudify.interfaces.lifecycle.configure', kw_over_val)
            self.fail('expected kwargs override to be disallowed')
        except RuntimeError, e:
            self.assertIn(
                'To allow redefinition, pass "allow_kwargs_override"', str(e))

    def _test_execute_operation_with_op_params(self, op,
                                               allow_kw_override=None):
        operation_param_key = 'operation_param_key'
        operation_param_value = 'operation_param_value'
        op_params = {operation_param_key: operation_param_value}

        params = self._get_params(op=op, op_params=op_params,
                                  allow_kw_override=allow_kw_override)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(4)

        instances = self.env.storage.get_node_instances()
        for instance in instances:
            self.assertIn('op_kwargs', instance.runtime_properties)
            op_kwargs = instance.runtime_properties['op_kwargs']
            self.assertIn(operation_param_key, op_kwargs)
            self.assertEquals(operation_param_value,
                              op_kwargs[operation_param_key])

    def test_execute_operation_by_nodes(self):
        node_ids = ['node2', 'node3']
        params = self._get_params(node_ids=node_ids)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(3, node_ids=node_ids)

    def test_execute_operation_by_node_instances(self):
        instances = self.env.storage.get_node_instances()
        node_instance_ids = [instances[0].id, instances[3].id]
        params = self._get_params(node_instance_ids=node_instance_ids)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(2, node_instance_ids=node_instance_ids)

    def test_execute_operation_by_type_names(self):
        type_names = ['mock_type2']
        params = self._get_params(type_names=type_names)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(3, type_names=type_names)

    def test_execute_operation_by_nodes_and_types(self):
        node_ids = ['node1', 'node2']
        type_names = ['mock_type2']
        params = self._get_params(node_ids=node_ids, type_names=type_names)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(2, node_ids=node_ids,
                                     type_names=type_names)

    def test_execute_operation_by_nodes_types_and_node_instances(self):
        node_ids = ['node2', 'node3']
        type_names = ['mock_type2', 'mock_type1']
        instances = self.env.storage.get_node_instances()
        node_instance_ids = [next(inst.id for inst in instances if
                                  inst.node_id == 'node2')]
        params = self._get_params(node_ids=node_ids,
                                  node_instance_ids=node_instance_ids,
                                  type_names=type_names)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(1, node_ids=node_ids,
                                     node_instance_ids=node_instance_ids,
                                     type_names=type_names)

    def test_execute_operation_empty_intersection(self):
        node_ids = ['node1', 'node2']
        type_names = ['mock_type3']
        params = self._get_params(node_ids=node_ids, type_names=type_names)
        self.env.execute('execute_operation', params)
        self._make_filter_assertions(0, node_ids=node_ids,
                                     type_names=type_names)

    def test_execute_operation_with_dependency_order(self):
        time_diff_assertions_pairs = [
            (0, 1),  # node 1 instance and node 2 instance
            (0, 2),  # node 1 instance and node 2 instance
            (1, 3),  # node 2 instance and node 3 instance
            (2, 3)   # node 2 instance and node 3 instance
        ]

        self._dep_order_tests_helper([],
                                     ['node1', 'node2', 'node2', 'node3'],
                                     time_diff_assertions_pairs)

    def test_execute_operation_with_indirect_dependency_order(self):
        time_diff_assertions_pairs = [
            (0, 1),  # node 1 instance and node 3 instance
        ]

        self._dep_order_tests_helper(['node1', 'node3'],
                                     ['node1', 'node3'],
                                     time_diff_assertions_pairs)

    def _make_filter_assertions(self, expected_num_of_visited_instances,
                                node_ids=None, node_instance_ids=None,
                                type_names=None):
        num_of_visited_instances = 0
        instances = self.env.storage.get_node_instances()
        nodes_by_id = dict((node.id, node) for node in
                           self.env.storage.get_nodes())

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

    def _dep_order_tests_helper(self, node_ids_param,
                                ordered_node_ids_of_instances,
                                indices_pairs_for_time_diff_assertions):
        params = self._get_params(
            op='cloudify.interfaces.lifecycle.start',
            node_ids=node_ids_param,
            run_by_dep=True)
        self.env.execute('execute_operation', params, task_thread_pool_size=4)

        instances_and_visit_times = sorted(
            ((inst, inst.runtime_properties['visit_time']) for inst in
             self.env.storage.get_node_instances() if 'visit_time' in
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

    def setUp(self):
        super(TestScale, self).setUp()
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-scale-blueprint.yaml")
        self.env = local.init_env(blueprint_path)

    def test_no_node(self):
        with testtools.ExpectedException(ValueError, ".*mock doesn't exist.*"):
            self.env.execute('scale', parameters={'node_id': 'mock'})

    def test_zero_delta(self):
        # should simply work
        self.env.execute('scale', parameters={'node_id': 'node',
                                              'delta': 0})

    def test_illegal_delta(self):
        with testtools.ExpectedException(ValueError, ".*-1 is illegal.*"):
            self.env.execute('scale', parameters={'node_id': 'node',
                                                  'delta': -1})


class TestSubgraphWorkflowLogic(testtools.TestCase):

    def setUp(self):
        super(TestSubgraphWorkflowLogic, self).setUp()
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-subgraph-blueprint.yaml")
        self.env = local.init_env(
            blueprint_path,
            ignored_modules=IGNORED_LOCAL_WORKFLOW_MODULES)

    def test_heal_connected_to_relationship_operations_on_on_affected(self):
        # Tests CFY-2788 fix
        # We run heal on node2 instance. node1 is connected to node2 and node3
        # we expect that the establish/unlink operations will only be called
        # for node1->node2
        node2_instance_id = [i for i in self.env.storage.get_node_instances()
                             if i.node_id == 'node2'][0].id
        self.env.execute('heal', parameters={
            'node_instance_id': node2_instance_id})
        node1_instance = [i for i in self.env.storage.get_node_instances()
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
    _write_operation(ctx, runs_on='source')


@operation
def target_operation(ctx, **_):
    _write_operation(ctx, runs_on='target')


def _write_operation(ctx, runs_on):
    invocations = ctx.source.instance.runtime_properties.get('invocations', [])
    invocations.append({
        'operation': ctx.operation.name,
        'target_node': ctx.target.node.name,
        'runs_on': runs_on})
    ctx.source.instance.runtime_properties['invocations'] = invocations
