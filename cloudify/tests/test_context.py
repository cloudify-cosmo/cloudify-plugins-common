from os.path import dirname
import unittest
import os

from cloudify.constants import MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY
from cloudify.context import CloudifyContext
from cloudify.exceptions import HttpException
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
        cls.context = CloudifyContext({'blueprint_id': ''})

    @classmethod
    def tearDownClass(cls):
        cls.file_server_process.stop()

    def test_get_resource(self):
        resource = self.context.get_resource(resource_path='for_test.log')
        self.assertIsNotNone(resource)

    def test_get_non_existing_resource(self):
        self.assertRaises(HttpException, self.context.get_resource,
                          'non_existing.log')

    def test_get_resource_to_non_writable_location(self):
        self.assertRaises(IOError, self.context.get_resource,
                          'for_test.log',
                          '/non-existing-folder')
