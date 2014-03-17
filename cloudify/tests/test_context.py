from os.path import dirname
import unittest
import os

from cloudify.constants import MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY
from cloudify.context import CloudifyContext
import cloudify.tests as tests_path


__author__ = 'elip'


class CloudifyContextTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        resources_path = os.path.join(dirname(tests_path.__file__), "resources")
        os.environ[MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY] \
            = 'file://{0}'.format(resources_path)
        cls.context = CloudifyContext({'blueprint_id': ''})

    def test_get_resource(self):
        resource = self.context.get_resource(resource_path='for_test.log')
        self.assertIsNotNone(resource)

    def test_get_non_existing_resource(self):
        self.assertRaises(IOError, self.context.get_resource, 'non_existing.log')
