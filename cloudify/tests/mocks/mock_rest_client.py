# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
############

__author__ = 'ran'


from cloudify_rest_client import CloudifyClient


node_instances = {}


def put_node_instance(node_instance_id,
                      state='started',
                      runtime_properties=None):
    node_instances[node_instance_id] = {
        'id': node_instance_id,
        'state': state,
        'version': 0,
        'runtimeProperties': runtime_properties
    }


class MockRestclient(CloudifyClient):

    def __init__(self):
        pass

    @property
    def node_instances(self):
        return MockNodeInstancesClient()


class MockNodeInstancesClient(object):

    def get(self, node_instance_id):
        if node_instance_id not in node_instances:
            raise RuntimeError(
                'No info for node with id {0}'.format(node_instance_id))
        return node_instances[node_instance_id]
