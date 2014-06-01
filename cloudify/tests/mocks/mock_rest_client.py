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


node_instances = {}


class MockRestclient(object):

    def get_node_instance(self, node_id):
        if node_id not in node_instances:
            raise RuntimeError('No info for node with id {0}'.format(node_id))
        return node_instances[node_id]


def put_node_instance(node_id, runtime_properties, state='started',
                      state_version=1):

    node_instances[node_id] = {
        'state': state,
        'stateVersion': state_version,
        'runtimeInfo': runtime_properties
    }
