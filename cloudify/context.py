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

from logging import getLogger
from manager import get_node_state
from manager import update_node_state
from collections import defaultdict


class ContextCapabilities(object):

    def __init__(self, capabilities={}):
        self._capabilities = capabilities if capabilities is not None else {}

    def __getitem__(self, key):
        value = None
        for caps in self._capabilities:
            if key in caps:
                if value is not None:
                    raise RuntimeError(
                        "'{0}' capability ambiguity [capabilities={1}]".format(
                            key, self._capabilities))
                value = caps[key]
        return value

    def get_all(self):
        return self._capabilities


class CloudifyRelatedNode(object):

    def __init__(self, ctx):
        self._related = ctx['related']
        if 'capabilities' in ctx and self.node_id in ctx['capabilities']:
            self._runtime_properties = ctx['capabilities'][self.node_id]
        else:
            self._runtime_properties = {}

    @property
    def node_id(self):
        return self._related['node_id']

    @property
    def properties(self):
        return self._related['node_properties']

    @property
    def runtime_properties(self):
        return self._runtime_properties

    def __getitem__(self, key):
        if key in self.properties:
            return self.properties[key]
        return self._runtime_properties[key]

    def __contains__(self, key):
        return key in self.properties or key in self._runtime_properties


class CloudifyContext(object):

    def __init__(self, ctx={}):
        def default_value():
            return None
        self._context = defaultdict(default_value, ctx)
        self._capabilities = ContextCapabilities(self._context['capabilities'])
        self._logger = getLogger(self.task_name)
        self._node_state = None
        if self._context['related'] is not None:
            self._related = CloudifyRelatedNode(self._context)
        else:
            self._related = None

    @property
    def node_id(self):
        return self._context['node_id']

    @property
    def node_name(self):
        return self._context['node_name']

    @property
    def properties(self):
        return self._context['node_properties']

    @property
    def runtime_properties(self):
        self._get_node_state_if_needed()
        return self._node_state.runtime_properties

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
    def related(self):
        return self._related

    @property
    def logger(self):
        return self._logger

    def _get_node_state_if_needed(self):
        if self.node_id is None:
            raise RuntimeError('Cannot get node state - invocation is not '
                               'in a context of node')
        if self._node_state is None:
            self._node_state = get_node_state(self.node_id)

    def __getitem__(self, key):
        if self.node_properties is not None and key in self.node_properties:
            return self.node_properties[key]
        self._get_node_state_if_needed()
        return self._node_state[key]

    def __setitem__(self, key, value):
        self._get_node_state_if_needed()
        self._node_state[key] = value

    def __contains__(self, key):
        if self.node_properties is not None and key in self.node_properties:
            return True
        self._get_node_state_if_needed()
        return key in self._node_state

    def update(self):
        if self._node_state is not None:
            update_node_state(self._node_state)
            self._node_state = None
