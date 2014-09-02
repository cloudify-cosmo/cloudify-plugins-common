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


import importlib
import argparse

from dsl_parser import parser, tasks
from cloudify_rest_client.nodes import Node
from cloudify_rest_client.node_instances import NodeInstance

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('workflow')
    arg_parser.add_argument('blueprint_path')
    args = arg_parser.parse_args()

    plan = parser.parse_from_path(args.blueprint_path)
    plan = tasks.prepare_deployment_plan(plan, inputs=None)

    workflow_name = args.workflow
    workflow = plan['workflows'][workflow_name]

    workflow_method_path = workflow['operation']
    split = workflow_method_path.split('.')
    workflow_module_name = '.'.join(split[:-1])
    workflow_method_name = split[-1]
    module = importlib.import_module(workflow_module_name)
    workflow_method = getattr(module, workflow_method_name)

    nodes = [Node(node) for node in plan['nodes']]
    node_instances = [NodeInstance(instance)
                      for instance in plan['node_instances']]

    for node in nodes:
        if 'relationships' not in node:
            node['relationships'] = []
    for node_instance in node_instances:
        node_instance['node_id'] = node_instance['name']
        if 'relationships' not in node_instance:
            node_instance['relationships'] = []

    ctx = {
        'local': True,
        'deployment_id': 'local',
        'blueprint_id': 'local',
        'execution_id': 'local',
        'workflow_id': workflow_name,
        'nodes': nodes,
        'node_instances': node_instances
    }
    workflow_method(__cloudify_context=ctx)
