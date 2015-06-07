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

import logging
import unittest

from cloudify.utils import setup_logger
from cloudify.utils import LocalCommandRunner
from cloudify.exceptions import CommandExecutionException


class LocalCommandRunnerTest(unittest.TestCase):

    runner = None

    @classmethod
    def setUpClass(cls):
        cls.logger = setup_logger(cls.__name__)
        cls.logger.setLevel(logging.DEBUG)
        cls.runner = LocalCommandRunner(
            logger=cls.logger)

    def test_run_command_success(self):
        response = self.runner.run('echo Hello')
        self.assertEqual('Hello', response.std_out)
        self.assertEqual(0, response.return_code)
        self.assertEqual('', response.std_err)

    def test_run_command_error(self):
        try:
            self.runner.run('/bin/sh -c bad')
            self.fail('Expected CommandExecutionException due to Bad command')
        except CommandExecutionException as e:
            self.assertTrue(1, e.code)

    def test_run_command_with_env(self):
        response = self.runner.run('env',
                                   execution_env={'TEST_KEY': 'TEST_VALUE'})
        self.assertTrue('TEST_KEY=TEST_VALUE' in response.std_out)
