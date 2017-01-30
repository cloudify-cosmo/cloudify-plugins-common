import os
import testtools

from cloudify.test_utils import workflow_test


class NodeContextTests(testtools.TestCase):

    test_blueprint_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/blueprints/test-controller-queue.yaml")

    @workflow_test
    def test_node_type(self, cfy_local):
        cfy_local.execute('install')

        instance = cfy_local.storage.get_node_instances('direct')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'direct')
        instance = cfy_local.storage.get_node_instances('host')
        self.assertEqual(instance.runtime_properties['controller,queue'], '')
        instance = cfy_local.storage.get_node_instances('connected_host')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'queue')
        instance = cfy_local.storage.get_node_instances('direct_override')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'direct_override')
