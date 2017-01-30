import os
import testtools

from cloudify.test_utils import workflow_test


class ControllertTests(testtools.TestCase):

    test_blueprint_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/blueprints/test-controller-queue.yaml")

    @workflow_test(test_blueprint_path)
    def test_controller_queue_property(self, cfy_local):
        cfy_local.execute('install')

        print "aaa!!!"
        instance = cfy_local.storage.get_node_instances('direct')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'direct')
        instance = cfy_local.storage.get_node_instances('host_none')
        self.assertEqual(instance.runtime_properties['controller,queue'], '')
        instance = cfy_local.storage.get_node_instances('connected_host')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'queue')
        instance = cfy_local.storage.get_node_instances('direct_override')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'direct_override')
        instance = cfy_local.storage.get_node_instances('contained_node')
        self.assertEqual(
            instance.runtime_properties['controller_queue'], 'father_host')
