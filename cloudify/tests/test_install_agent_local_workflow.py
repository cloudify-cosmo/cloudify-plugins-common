########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

from mock import patch

import testtools
from testtools.testcase import ExpectedException

from cloudify.workflows import local


class InstallAgentTest(testtools.TestCase):

    def test_install_agent_fails_in_local(self):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-install-agent-blueprint-local.yaml")

        with ExpectedException(ValueError,
                               "'install_agent': true is not supported*"):
            self.env = local.init_env(blueprint_path)

    # Patch _validate_node, to allow installing agent in local mode
    @patch('cloudify.workflows.local._validate_node')
    @patch('cloudify_agent.installer.operations.start')
    @patch('cloudify_agent.installer.operations.configure')
    @patch('cloudify_agent.installer.operations.create')
    def _test_install_agent(self,
                            blueprint,
                            create_mock,
                            config_mock,
                            start_mock, *_):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources/blueprints/{0}'.format(blueprint))

        env = local.init_env(blueprint_path)
        env.execute('install')

        create_mock.assert_has_any_calls()
        config_mock.assert_has_any_calls()
        start_mock.assert_has_any_calls()

    def test_install_agent(self):
        self._test_install_agent('test-install-agent-blueprint.yaml')

    def test_install_agent_windows(self):
        self._test_install_agent('test-install-agent-blueprint-windows.yaml')

    def test_install_agent_3_2(self):
        self._test_install_agent('test-install-agent-blueprint-3-2.yaml')

    def test_install_agent_windows_3_2(self):
        self._test_install_agent(
            'test-install-agent-blueprint-windows-3-2.yaml')
