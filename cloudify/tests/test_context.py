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

import os
from os.path import dirname

import testtools

from cloudify import constants
from cloudify import context
from cloudify import exceptions
from cloudify.utils import create_temp_folder

import cloudify.tests as tests_path


class CloudifyContextTest(testtools.TestCase):
    file_server_process = None

    @classmethod
    def setUpClass(cls):

        resources_path = os.path.join(dirname(tests_path.__file__),
                                      "resources")

        from cloudify.tests.file_server import FileServer
        from cloudify.tests.file_server import PORT

        cls.file_server_process = FileServer(resources_path)
        cls.file_server_process.start()

        os.environ[constants.MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY] \
            = "http://localhost:{0}".format(PORT)
        os.environ[constants.MANAGER_FILE_SERVER_URL_KEY] = \
            "http://localhost:{0}".format(PORT)
        cls.context = context.CloudifyContext({'blueprint_id': ''})

    @classmethod
    def tearDownClass(cls):
        cls.file_server_process.stop()

    def test_get_resource(self):
        resource = self.context.get_resource(resource_path='for_test.log')
        self.assertIsNotNone(resource)

    def test_download_resource(self):
        resource_path = self.context.download_resource(
            resource_path='for_test.log')
        self.assertIsNotNone(resource_path)
        self.assertTrue(os.path.exists(resource_path))

    def test_download_resource_to_specific_file(self):
        target_path = "{0}/for_test_custom.log".format(create_temp_folder())
        resource_path = self.context.download_resource(
            resource_path='for_test.log',
            target_path=target_path)
        self.assertEqual(target_path, resource_path)
        self.assertTrue(os.path.exists(resource_path))

    def test_download_resource_to_non_writable_location(self):
        self.assertRaises(IOError, self.context.download_resource,
                          'for_test.log',
                          '/non-existing-folder')

    def test_get_non_existing_resource(self):
        self.assertRaises(exceptions.HttpException, self.context.get_resource,
                          'non_existing.log')

    def test_ctx_instance_in_relationship(self):
        ctx = context.CloudifyContext({
            'node_id': 'node-instance-id',
            'related': {
                'node_id': 'related-instance-id',
                'is_target': True
            },
            'relationships': ['related-instance-id']
        })
        self.assertEqual('node-instance-id', ctx.source.instance.id)
        self.assertEqual('related-instance-id', ctx.target.instance.id)
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.node)
        self.assertIn('ctx.node/ctx.instance can only be used in a '
                      'node-instance context but used in a '
                      'relationship-instance context.', str(e))
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.instance)
        self.assertIn('ctx.node/ctx.instance can only be used in a '
                      'node-instance context but used in a '
                      'relationship-instance context.', str(e))

    def test_source_target_not_in_relationship(self):
        ctx = context.CloudifyContext({})
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.source)
        self.assertIn('ctx.source/ctx.target can only be used in a '
                      'relationship-instance context but used in a '
                      'deployment context.', str(e))
        e = self.assertRaises(exceptions.NonRecoverableError,
                              lambda: ctx.target)
        self.assertIn('ctx.source/ctx.target can only be used in a '
                      'relationship-instance context but used in a '
                      'deployment context.', str(e))

    def test_ctx_type(self):
        ctx = context.CloudifyContext({})
        self.assertEqual(context.DEPLOYMENT, ctx.type)
        ctx = context.CloudifyContext({'node_id': 'node-instance-id'})
        self.assertEqual(context.NODE_INSTANCE, ctx.type)
        ctx = context.CloudifyContext({
            'node_id': 'node-instance-id',
            'related': {
                'node_id': 'related-instance-id',
                'is_target': True
            },
            'relationships': ['related-instance-id']
        })
        self.assertEqual(context.RELATIONSHIP_INSTANCE, ctx.type)
