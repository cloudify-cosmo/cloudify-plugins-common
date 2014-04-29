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

__author__ = 'dank'


class CloudifyWorkflowNode(object):

    def __init__(self, node):
        self._node = node

    def set_state(self, state):
        pass

    def execute(self, operation, *args, **kwargs):
        pass

    @property
    def type(self):
        return self._node.get('type')

    @property
    def properties(self):
        return self._node.get('properties', {})

    @property
    def plugins_to_install(self):
        return self._node.get('plugins_to_install', [])




class CloudifyWorkflowContext(object):

    def __init__(self, ctx):
        self._context = ctx
        self._nodes = [CloudifyWorkflowNode(node) for
                       node in ctx['plan']['nodes']]

    @property
    def nodes(self):
        return self.nodes

    def send_event(self, event):
        pass
