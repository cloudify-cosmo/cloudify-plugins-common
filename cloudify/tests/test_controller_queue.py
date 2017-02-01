import os
import testtools

from cloudify.test_utils import workflow_test


class ControllertTests(testtools.TestCase):

    test_blueprint_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/blueprints/test-controller-queue.yaml")

    @workflow_test(blueprint_path=test_blueprint_path)
    def test_controller_queue_property(self, cfy_local):
        cfy_local.execute('install')

        instance = cfy_local.provider_context.get_node_instances(
            node_id='direct')
        self.assertEqual(
            instance.properties['controller_queue'], 'direct')
        instance = cfy_local.provider_context.get_node_instances(
            node_id='host_none')
        self.assertEqual(instance.properties['controller,queue'], '')
        instance = cfy_local.provider_context.get_node_instances(
            'connected_host')
        self.assertEqual(
            instance.properties['controller_queue'], 'queue')
        instance = cfy_local.provider_context.get_node_instances(
            'direct_override')
        self.assertEqual(
            instance.properties['controller_queue'], 'direct_override')
        instance = cfy_local.provider_context.get_node_instances(
            'contained_node')
        self.assertEqual(
            instance.properties['controller_queue'], 'father_host')
