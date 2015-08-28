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

import testtools

from dsl_parser import exceptions as dsl_exceptions

from cloudify.workflows import local


class LocalWorkflowInitTest(testtools.TestCase):

    def test_init_env_validate_definitions(self):
        blueprint_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "resources/blueprints/test-validate-version-blueprint.yaml")
        self.assertRaises(
            dsl_exceptions.DSLParsingException,
            local.init_env, blueprint_path,
            validate_version=True)
        local.init_env(blueprint_path, validate_version=False)
