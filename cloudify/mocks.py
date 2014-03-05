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
import sys

__author__ = 'idanmo'

import logging
from context import CloudifyContext, ContextCapabilities


class MockCloudifyContext(CloudifyContext):
    """
    Cloudify context mock to be used when testing Cloudify plugins.
    """

    def __init__(self,
                 node_id=None,
                 properties=None,
                 runtime_properties=None,
                 capabilities=None,
                 related=None,
                 operation=None):
        super(MockCloudifyContext, self).__init__({'operation': operation})
        self._node_id = node_id
        self._properties = properties or {}
        self._runtime_properties = runtime_properties or {}
        if capabilities and not isinstance(capabilities, ContextCapabilities):
            raise ValueError(
                "MockCloudifyContext(capabilities=?) must be "
                "instance of ContextCapabilities, not {0}".format(
                    capabilities))
        self._capabilities = capabilities or ContextCapabilities()
        self._related = related

        root = logging.getLogger()
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] '
                                          '[%(name)s] %(message)s',
                                      datefmt='%H:%M:%S')
        ch.setFormatter(formatter)

        # clear all other handlers
        for handler in root.handlers:
            root.removeHandler(handler)

        root.addHandler(ch)
        self._mock_context_logger = logging.getLogger('mock-context-logger')
        self._mock_context_logger.setLevel(logging.DEBUG)

    @property
    def node_id(self):
        return self._node_id

    @property
    def properties(self):
        return self._properties

    @property
    def runtime_properties(self):
        return self._runtime_properties

    @property
    def capabilities(self):
        return self._capabilities

    @property
    def logger(self):
        return self._mock_context_logger

    def __contains__(self, key):
        return key in self._properties or key in self._runtime_properties

    def __setitem__(self, key, value):
        self._runtime_properties[key] = value

    def __getitem__(self, key):
        if key in self._properties:
            return self._properties[key]
        return self._runtime_properties[key]
