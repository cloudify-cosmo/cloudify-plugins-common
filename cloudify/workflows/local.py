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


import os
import tempfile
import copy
import importlib
import uuid
import json

from cloudify_rest_client.nodes import Node
from cloudify_rest_client.node_instances import NodeInstance

try:
    from dsl_parser import parser, tasks
except ImportError:
    raise ImportError('cloudify-dsl-parser must be installed to execute local'
                      ' workflows. (e.g. "pip install cloudify-dsl-parser")')


class Environment(object):

    def __init__(self,
                 blueprint_path,
                 name='local',
                 inputs=None,
                 storage_cls=None,
                 **storage_kwargs):
        self.name = name

        self.plan = tasks.prepare_deployment_plan(
            parser.parse_from_path(blueprint_path),
            inputs=inputs)

        nodes = [Node(node) for node in self.plan['nodes']]
        node_instances = [NodeInstance(instance)
                          for instance in self.plan['node_instances']]

        for node in nodes:
            if 'relationships' not in node:
                node['relationships'] = []
        for node_instance in node_instances:
            node_instance['node_id'] = node_instance['name']
            if 'relationships' not in node_instance:
                node_instance['relationships'] = []

        storage_kwargs.update(dict(
            name=self.name,
            resources_root=os.path.dirname(blueprint_path),
            nodes=nodes,
            node_instances=node_instances
        ))

        if storage_cls is None:
            storage_cls = InMemoryStorage

        self.storage = storage_cls(**storage_kwargs)

    def execute(self, workflow):
        workflow_name = workflow
        workflow = self.plan['workflows'][workflow_name]
        workflow_method_path = workflow['operation']
        split = workflow_method_path.split('.')
        workflow_module_name = '.'.join(split[:-1])
        workflow_method_name = split[-1]
        module = importlib.import_module(workflow_module_name)
        workflow_method = getattr(module, workflow_method_name)

        execution_id = str(uuid.uuid4())
        ctx = {
            'local': True,
            'deployment_id': self.name,
            'blueprint_id': self.name,
            'execution_id': execution_id,
            'workflow_id': workflow_name,
            'storage': self.storage
        }
        workflow_method(__cloudify_context=ctx)


class Storage(object):

    def __init__(self, name, resources_root):
        self.name = name
        self.resources_root = resources_root

    def get_resource(self, resource_path):
        with open(os.path.join(self.resources_root, resource_path)) as f:
            return f.read()

    def download_resource(self, resource_path, target_path=None):
        if not target_path:
            suffix = '-{}'.format(os.path.basename(resource_path))
            target_path = tempfile.mktemp(suffix=suffix)
        resource = self.get_resource(resource_path)
        with open(target_path, 'w') as f:
            f.write(resource)
        return target_path

    def get_node_instance(self, node_instance_id):
        raise NotImplementedError()

    def update_node_instance(self,
                             node_instance_id,
                             runtime_properties=None,
                             state=None,
                             version=None):
        instance = self._get_node_instance(node_instance_id)
        if runtime_properties is not None:
            instance['runtime_properties'] = runtime_properties
        if state is not None:
            instance['state'] = state
        if version is not None:
            instance['version'] = version
        self._store_instance(instance)

    def _get_node_instance(self, node_instance_id):
        instance = self._load_instance(node_instance_id)
        if instance is None:
            raise RuntimeError('Instance {} does not exist'
                               .format(node_instance_id))
        return instance

    def _load_instance(self, node_instance_id):
        raise NotImplementedError()

    def _store_instance(self, node_instance):
        raise NotImplementedError()

    def get_nodes(self):
        raise NotImplementedError()

    def get_node_instances(self):
        raise NotImplementedError()


class InMemoryStorage(Storage):

    def __init__(self, name, resources_root, nodes, node_instances):
        super(InMemoryStorage, self).__init__(name, resources_root)
        self._nodes = nodes
        self._node_instances = {instance.id: instance
                                for instance in node_instances}

    def get_node_instance(self, node_instance_id):
        return copy.deepcopy(self._get_node_instance(node_instance_id))

    def _load_instance(self, node_instance_id):
        return self._node_instances.get(node_instance_id)

    def _store_instance(self, node_instance):
        pass

    def get_nodes(self):
        return self._nodes

    def get_node_instances(self):
        return self._node_instances.values()


class FileStorage(Storage):

    def __init__(self, name, resources_root, nodes, node_instances,
                 storage_dir):
        super(FileStorage, self).__init__(name, resources_root)
        self._storage_dir = os.path.join(storage_dir, name)
        if not os.path.isdir(self._storage_dir):
            os.mkdir(self._storage_dir)
        self._instances_dir = os.path.join(self._storage_dir, 'node-instances')
        if not os.path.isdir(self._instances_dir):
            os.mkdir(self._instances_dir)
            for instance in node_instances:
                self._store_instance(instance)
        self._nodes = nodes

    def get_node_instance(self, node_instance_id):
        return self._get_node_instance(node_instance_id)

    def _load_instance(self, node_instance_id):
        with open(self._instance_path(node_instance_id)) as f:
            return NodeInstance(json.loads(f.read()))

    def _store_instance(self, node_instance):
        with open(self._instance_path(node_instance.id), 'w') as f:
            f.write(json.dumps(node_instance))

    def _instance_path(self, node_instance_id):
        return os.path.join(self._instances_dir, node_instance_id)

    def get_nodes(self):
        return self._nodes

    def get_node_instances(self):
        return [self._get_node_instance(instance_id)
                for instance_id in os.listdir(self._instances_dir)]
