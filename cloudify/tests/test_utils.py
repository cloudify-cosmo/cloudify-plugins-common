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
import os
import unittest

from cloudify import tests
from cloudify.utils import setup_logger
from cloudify.utils import LocalCommandRunner
from cloudify.tests.file_server import FileServer


class LocalRunnerTest(unittest.TestCase):

    fs = None
    runner = None

    @classmethod
    def setUpClass(cls):
        super(LocalRunnerTest, cls).setUpClass()
        cls.logger = setup_logger(cls.__name__)
        cls.logger.setLevel(logging.DEBUG)
        cls.runner = LocalCommandRunner(
            logger=cls.logger)
        resources = os.path.join(
            os.path.dirname(tests.__file__),
            'resources'
        )
        cls.fs = FileServer(resources)
        cls.fs.start()

    @classmethod
    def tearDownClass(cls):
        super(LocalRunnerTest, cls).tearDownClass()
        cls.fs.stop()

    def test_run_command(self):
        response = self.runner.run('echo hello')
        self.assertIn('hello', response.output)

    def test_run_command_with_env(self):
        response = self.runner.run('env',
                                   execution_env={'TEST_KEY': 'TEST_VALUE'})
        self.assertIn('TEST_KEY=TEST_VALUE', response.output)
