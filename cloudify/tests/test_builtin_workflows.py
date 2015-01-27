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
        operation_param_key = 'operation_param_key'
        operation_param_value = 'operation_param_value'
        op_params = {operation_param_key: operation_param_value}

        params = self._get_params(op_params=op_params)
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
                    op_params=None, run_by_dep=False, node_ids=None,
                    node_instance_ids=None, type_names=None):
        return {
            'operation': op,
            'operation_kwargs': op_params or {},
            'run_by_dependency_order': run_by_dep,
            'node_ids': node_ids or [],
            'node_instance_ids': node_instance_ids or [],
            'type_names': type_names or []
        }


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
