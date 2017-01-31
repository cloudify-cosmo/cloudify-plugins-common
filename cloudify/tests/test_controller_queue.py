import os
import testtools

from cloudify.test_utils import workflow_test


class ControllertTests(testtools.TestCase):

    test_blueprint_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/blueprints/test-controller-queue.yaml")

    @workflow_test(blueprint_path=test_blueprint_path)
    def test_controller_queue_property(self, cfy_local):
        cfy_local.execute('execute_operation')

        instance = cfy_local.storage.get_node_instances(node_id='direct')[0]
        self.assertEqual(
            instance.properties['controller_queue'], 'direct')
        instance = cfy_local.storage.get_node_instances(node_id='host_none')[0]
        self.assertEqual(instance.properties['controller,queue'], '')
        instance = cfy_local.storage.get_node_instances('connected_host')[0]
        self.assertEqual(
            instance.properties['controller_queue'], 'queue')
        instance = cfy_local.storage.get_node_instances(node_id='direct_override')[0]
        self.assertEqual(
            instance.properties['controller_queue'], 'direct_override')
        instance = cfy_local.storage.get_node_instances(node_id='contained_node')[0]
        self.assertEqual(
            instance.properties['controller_queue'], 'father_host')
