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

        direct = cfy_local.storage.get_node_instances('direct')[0]
        print direct
        host_none = cfy_local.storage.get_node_instances('host_none')[0]
        print host_none
        connected_host = cfy_local.storage.get_node_instances('connected_host')[0]
        print connected_host
        direct_override = cfy_local.storage.get_node_instances('direct_override')[0]
        print direct_override
        contained_node = cfy_local.storage.get_node_instances('contained_node')[0]
        print contained_node

        self.assertEqual(direct.runtime_properties['controller_queue'], 'direct')
        self.assertEqual(host_none.runtime_properties['controller,queue'], '')
        self.assertEqual(connected_host.runtime_properties['controller_queue'], 'queue')
        self.assertEqual(direct_override.runtime_properties['controller_queue'], 'direct_override')
        self.assertEqual(contained_node.runtime_properties['controller_queue'], 'father_host')
