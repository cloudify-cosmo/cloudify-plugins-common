########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import os
import ghost
import tempfile
import testtools

from cloudify.state import current_ctx
from cloudify.mocks import MockCloudifyContext
from cloudify.plugins.secret_store import \
    CloudifySecretStore as SecClientCfg


class TestSecureClientConfig(testtools.TestCase):

    @staticmethod
    def mock_ctx(test_name, db_path=None):

        if not db_path:
            db_path = os.path.join(tempfile.mkdtemp(), test_name)

        test_node_id = test_name
        test_properties = {
            'controller_config': {
                'controller_queue': 'Boaty McBoatface',
                'secret_schemas': {
                    'aws_config': {
                        'key_name': 'aws',
                        'database_uri': db_path,
                        'secret_names': {
                            'secret_1': None,
                            'secret_2': None
                        }
                    },
                    'azure_config': {
                        'key_name': 'azure',
                        'database_uri': db_path,
                        'secret_names': {
                            'secret_5': None,
                            'secret_6': None
                        }
                    }
                }
            }
        }

        return MockCloudifyContext(
            node_id=test_node_id,
            properties=test_properties,
            provider_context={'resources': {}})

    def test_get_controller_config(self):
        """ _get_contoller_confg checks the current ctx
        for a controller_config
        It will return either a controller_config or a NoneType.

        :return:
        """

        # initialize the mock ctx with a standard controller_config
        # assert the value in the controller config
        # matches what we expect from the ctx
        ctx = self.mock_ctx('test_get_controller_config')
        current_ctx.set(ctx=ctx)
        ctlr_cfg = SecClientCfg().controller_config
        self.assertEqual(ctx.node.properties['controller_config'],
                         ctlr_cfg)

        # Take the original config and modify it.
        # Create a new mock ctx identical to the original
        # modify the instance runtime properties to have the modified config
        # Assert that the Node Properties do not match the new config
        # Assert that the Instance Attribute do match the new config
        ctlr_cfg_old = ctlr_cfg.copy()
        ctlr_cfg_old.update(
            {'controller_queue': 'Boaty McBoatface Returns'}
        )
        ctx = self.mock_ctx('test_get_controller_config')
        current_ctx.set(ctx=ctx)
        ctx.instance.runtime_properties['controller_config'] = ctlr_cfg_old

        ctlr_cfg = SecClientCfg().controller_config
        self.assertNotEqual(ctx.node.properties['controller_config'],
                            ctlr_cfg)
        self.assertEqual(ctx.instance.runtime_properties['controller_config'],
                         ctlr_cfg)

        # Now try to see that it doesn't exist if the node ctx does not deliver
        ctx = MockCloudifyContext(
            node_id='empty',
            properties={},
            provider_context={'resources': ''}
        )
        current_ctx.set(ctx=ctx)
        ctlr_cfg = SecClientCfg().controller_config
        self.assertEqual({},
                         ctlr_cfg)
        current_ctx.clear()

    def test_update_config_with_secrets(self):
        """ test_update_config_with_secrets checks to see that
        update_config_with_secrets will return the correct
        config with the correct secrets

        :return:
        """
        temp_db_dir = tempfile.mkdtemp()
        temp_db_path = os.path.join(temp_db_dir,
                                    'test_update_config_with_secrets')
        storage = ghost.TinyDBStorage(db_path=temp_db_path)
        self.addCleanup(os.remove, temp_db_path)
        passphrase = 'BoatyMcBoatfaceStrikesBack'
        stash = ghost.Stash(storage, passphrase=passphrase)
        stash.init()
        ctx = self.mock_ctx('test_get_controller_config', temp_db_path)

        aws_config = {
            'secret_1': None,
            'secret_2': None
        }
        azure_config = {
            'secret_5': None,
            'secret_6': None
        }

        current_ctx.set(ctx=ctx)
        ctlr_cfg = SecClientCfg(passphrase=passphrase,
                                database_uri=temp_db_path)
        # really long lines
        node_props_schemas = \
            ctx.node.properties['controller_config']['secret_schemas']
        expected = \
            node_props_schemas['aws_config']['secret_names']
        self.assertEquals(
            ctlr_cfg.update_config_with_secrets(aws_config, 'aws_config'),
            expected
        )
        expected = \
            node_props_schemas['azure_config']['secret_names']
        self.assertEquals(
            ctlr_cfg.update_config_with_secrets(azure_config, 'azure_config'),
            expected

        )

        # Now we will add secrets to the secret store
        # And we will check to see that those secrets that
        # match the schemas will get inserted
        stash.put(
            name='aws',
            value={
                'secret_1': 'Secret One',
                'secret_2': 'Secret Two',
                'secret_3': 'Better Not Show Up!!!'
            }
        )
        stash.put(
            name='azure',
            value={
                'secret_5': 'Secret Five',
                'secret_6': 'Secret Six',
                'secret_7': 'Better Not Show Up Either!!!'
            }
        )

        ctx = self.mock_ctx('test_get_controller_config', temp_db_path)
        current_ctx.set(ctx=ctx)
        ctlr_cfg = SecClientCfg(passphrase=passphrase,
                                database_uri=temp_db_path)

        expected = {
            'secret_1': 'Secret One',
            'secret_2': 'Secret Two'}
        self.assertEquals(
            ctlr_cfg.update_config_with_secrets(aws_config, 'aws_config'),
            expected
        )
        expected = {
            'secret_5': 'Secret Five',
            'secret_6': 'Secret Six'}
        self.assertEquals(
            ctlr_cfg.update_config_with_secrets(azure_config, 'azure_config'),
            expected
        )
        current_ctx.clear()
