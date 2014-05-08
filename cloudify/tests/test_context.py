from os.path import dirname
import unittest
import os

from cloudify.constants import MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY, \
    MANAGER_FILE_SERVER_URL_KEY
from cloudify.context import CloudifyContext
from cloudify.exceptions import HttpException
from cloudify.utils import create_temp_folder
import cloudify.tests as tests_path


__author__ = 'elip'


class CloudifyContextTest(unittest.TestCase):
    file_server_process = None

    @classmethod
    def setUpClass(cls):

        resources_path = os.path.join(dirname(tests_path.__file__),
                                      "resources")

        from cloudify.tests.file_server import FileServer
        from cloudify.tests.file_server import PORT

        cls.file_server_process = FileServer(resources_path)
        cls.file_server_process.start()

        os.environ[MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY] \
            = "http://localhost:{0}".format(PORT)
        os.environ[MANAGER_FILE_SERVER_URL_KEY] = \
            "http://localhost:{0}".format(PORT)
        cls.context = CloudifyContext({'blueprint_id': ''})

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
        self.assertRaises(HttpException, self.context.get_resource,
                          'non_existing.log')
