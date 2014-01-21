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

import os
import unittest
from cosmo import build_includes
from cosmo.tests import get_logger

__author__ = 'elip'

logger = get_logger("CeleryTestCase")


class CeleryTestCase(unittest.TestCase):

    def test_includes(self):

        expected = ['cosmo.cloudify.tosca.artifacts.plugin.a.tasks',
                    'cosmo.cloudify.tosca.artifacts.plugin.b.tasks']

        directory = os.path.dirname(__file__)
        app = 'cosmo'
        logger.info("scanning directory {0} and app {1} for tasks"
                    .format(directory, app))
        includes = build_includes(directory, app)
        logger.info("includes = {0}".format(includes))
        self.assertItemsEqual(expected, includes)
