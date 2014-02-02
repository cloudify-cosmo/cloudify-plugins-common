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

from collections import defaultdict
from logging import getLogger
from manager import get_node_state
from manager import update_node_state


class ContextCapabilities(object):

    def __init__(self, capabilities={}):
        self._capabilities = capabilities if capabilities is not None else {}

    def __getitem__(self, key):
        for caps in self._capabilities:
            if key in caps:
                return caps[key]
        return None


class CloudifyContext(object):

    def __init__(self, ctx={}):
        def default_value():
            return None
        dict
        self._context = defaultdict(default_value, ctx)
        self._capabilities = ContextCapabilities(self._context['capabilities'])
        self._logger = getLogger(self.task_name)
        self._node_state = None

    @property
    def node_id(self):
        return self._context['node_id']

    @property
    def blueprint_id(self):
        return self._context['blueprint_id']

    @property
    def deployment_id(self):
        return self._context['deployment_id']

    @property
    def execution_id(self):
        return self._context['execution_id']

    @property
    def node_name(self):
        return self._context['node_name']

    @property
    def task_id(self):
        return self._context['task_id']

    @property
    def task_name(self):
        return self._context['task_name']

    @property
    def plugin(self):
        return self._context['plugin']

    @property
    def operation(self):
        return self._context['operation']

    @property
    def capabilities(self):
        return self._capabilities

    @property
    def logger(self):
        return self._logger

    def _node_state(self):
        if self._node_state is None:
            self._node_state = get_node_state(self.node_id)

    @_node_state
    def __getitem__(self, key):
        return self._node_state[key]

    @_node_state
    def __setitem__(self, key, value):
        self._node_state[key] = value

    @_node_state
    def __contains__(self, key):
        return key in self._node_state

    @_node_state
    def runtime_properties(self):
        return self._node_state.runtime_properties

    def update(self):
        if self._node_state is not None:
            update_node_state(self._node_state)
            self._node_state = None
