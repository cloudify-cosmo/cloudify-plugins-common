import os
import unittest
from cosmo import build_includes
from cosmo.tests import get_logger

__author__ = 'elip'

logger = get_logger("CeleryTestCase")


class CeleryTestCase(unittest.TestCase):

    def test_includes(self):

        expected = ['cosmo.cloudify.tosca.artifacts.plugin.a.tasks', 'cosmo.cloudify.tosca.artifacts.plugin.b.tasks']

        directory = os.path.dirname(__file__)
        app = 'cosmo'
        logger.info("scanning directory {0} and app {1} for tasks".format(directory, app))
        includes = build_includes(directory, app)
        logger.info("includes = {0}".format(includes))
        assert includes == expected







