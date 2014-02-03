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

__author__ = 'idanmo'

import notifications
import utils
from cosmo_manager_rest_client.cosmo_manager_rest_client \
    import CosmoManagerRestClient


class NodeState(object):
    """Represents a deployment node state.
    An instance of this class contains runtime information retrieved
    from Cloudify's runtime storage.
    Its API allows to set and get properties of the node's state,
     generate an updates dict to be used when requesting to save changes
     back to the storage (in an optimistic locking manner).
    """
    def __init__(self, node_id, runtime_properties={}):
        self.id = node_id
        self._runtime_properties = runtime_properties
        self._runtime_properties = {k: [v, None] for k, v
                                    in runtime_properties.iteritems()}

    def get(self, key):
        if key in self._runtime_properties:
            return self._runtime_properties[key][0]
        return None

    def put(self, key, value):
        """Put a runtime property value.
        Initial runtime properties structure:
            key: [value, None]
        New runtime properties structure:
            key: [value]
        Updated runtime properties structure:
            key: [new_value, old_value]
        """
        if key in self._runtime_properties:
            values = self._runtime_properties[key]
            if len(values) == 1 or values[1] is None:
                self._runtime_properties[key] = [value, values[0]]
            else:
                values[0] = value
        else:
            self._runtime_properties[key] = [value]

    def __setitem__(self, key, value):
        return self.put(key, value)

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        return key in self._runtime_properties

    @property
    def runtime_properties(self):
        return {k: v[0] for k, v in self._runtime_properties.iteritems()}

    def get_updated_properties(self):
        """Get new/updated runtime properties.
        Returns:
            A dict in the following structure:
            new values => key: [value]
            updated values => key: [new_value, old_value]
        """
        if self._runtime_properties is None:
            return {}
        return {k: v for k, v in self._runtime_properties.iteritems()
                if len(v) == 1 or v[1] is not None}


def get_manager_rest_client():
    return CosmoManagerRestClient(utils.get_manager_ip(),
                                  utils.get_manager_rest_service_port())


def get_node_state(node_id):
    client = get_manager_rest_client()
    node_state = client.get_node_state(node_id)
    if 'runtimeInfo' not in node_state:
        raise KeyError('runtimeInfo not found in get_node_state response')
    return NodeState(node_id, node_state['runtimeInfo'])


def update_node_state(node_state):
    updated = node_state.get_updated_properties()
    if len(updated) == 0:
        return None
    client = get_manager_rest_client()
    client.update_node_state(node_state.id, updated)


def set_node_started(node_id, host):
    notifications.send_event(host, node_id, 'state', 'running')
