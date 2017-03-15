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

from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.node_instances import NodeInstance
from cloudify_rest_client.executions import Execution


node_instances = {}


def put_node_instance(node_instance_id,
                      state='started',
                      runtime_properties=None,
                      relationships=None):
    node_instances[node_instance_id] = NodeInstance({
        'id': node_instance_id,
        'state': state,
        'version': 0,
        'runtime_properties': runtime_properties,
        'relationships': relationships
    })


class MockRestclient(CloudifyClient):

    def __init__(self):
        pass

    @property
    def node_instances(self):
        return MockNodeInstancesClient()

    @property
    def nodes(self):
        return MockNodesClient()

    @property
    def executions(self):
        return MockExecutionsClient()

    @property
    def manager(self):
        return MockManagerClient()


class MockNodesClient(object):

    def list(self, deployment_id):
        return []


class MockNodeInstancesClient(object):

    def get(self, node_instance_id, evaluate_functions=False):
        if node_instance_id not in node_instances:
            raise RuntimeError(
                'No info for node with id {0}'.format(node_instance_id))
        return node_instances[node_instance_id]

    def list(self, deployment_id):
        return []


class MockExecutionsClient(object):

    def update(self, *args, **kwargs):
        return None

    def get(self, id):
        return Execution({
            'id': '111',
            'status': 'terminated'
        })


class MockManagerClient(object):

    def get_context(self):
        return {'context': {}}
