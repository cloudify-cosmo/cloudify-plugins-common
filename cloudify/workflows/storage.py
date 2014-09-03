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


def create(resources_root, node_instances):
    return InMemoryLocalWorkflowStorage(resources_root, node_instances)


class LocalWorkflowStorage(object):

    def __init__(self, resources_root):
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


class InMemoryLocalWorkflowStorage(LocalWorkflowStorage):

    def __init__(self, resources_root, node_instances):
        super(InMemoryLocalWorkflowStorage, self).__init__(resources_root)
        self.node_instances = {instance.id: instance
                               for instance in node_instances}

    def get_node_instance(self, node_instance_id):
        return copy.deepcopy(self._get_node_instance(node_instance_id))

    def _load_instance(self, node_instance_id):
        return self.node_instances.get(node_instance_id)

    def _store_instance(self, node_instance):
        pass
