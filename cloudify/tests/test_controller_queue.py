import logging
import mock
import sys
import os
import shutil
import tempfile
import unittest
from os.path import dirname

import testtools
from mock import patch, MagicMock

from cloudify import constants
from cloudify import context
from cloudify import exceptions
from cloudify import conflict_handlers
from cloudify.utils import create_temp_folder
from cloudify.decorators import operation
from cloudify.manager import NodeInstance
from cloudify.workflows import local
from cloudify_rest_client.exceptions import CloudifyClientError

import cloudify.tests as tests_path
from cloudify.test_utils import workflow_test


class NodeContextTests(testtools.TestCase):

    test_blueprint_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/blueprints/test-controller-queue.yaml")

    @workflow_test(blueprint_path=test_blueprint_path,
                   resources_to_copy=[
                       'resources/blueprints/test-controller-queue.yaml'])
    def test_node_type(self, cfy_local):
        cfy_local.execute('install')

        instance = cfy_local.storage.get_node_instances('direct')
        self.assertEqual(instance.runtime_properties['controller_queue'], 'direct')
        instance = cfy_local.storage.get_node_instances('host')
        self.assertEqual(instance.runtime_properties['controller,queue'], '')
        instance = cfy_local.storage.get_node_instances('connected_host')
        self.assertEqual(instance.runtime_properties['controller_queue'], 'queue')
        instance = cfy_local.storage.get_node_instances('direct_override')
        self.assertEqual(instance.runtime_properties['controller_queue'], 'direct_override')
