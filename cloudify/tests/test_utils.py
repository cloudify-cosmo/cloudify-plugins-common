########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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


import unittest

from cloudify.exceptions import CommandExecutionException


class LocalCommandRunnerTest(unittest.TestCase):

    from cloudify.utils import LocalCommandRunner
    runner = LocalCommandRunner()

    def test_run_command_success(self):
        command_execution_result = self.runner.run('echo Hello')
        self.assertEqual('Hello', command_execution_result.output.strip())
        self.assertEqual(0, command_execution_result.code)

    def test_run_command_error(self):
        try:
            self.runner.run('/bin/sh -c bad')
            self.fail('Expected CommandExecutionException due to Bad command')
        except CommandExecutionException as e:
            self.assertTrue(1, e.code)
