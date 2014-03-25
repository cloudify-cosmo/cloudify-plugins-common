########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

__author__ = 'elip'

VIRTUALENV_PATH_KEY = 'VIRTUALENV'
CELERY_WORK_DIR_PATH_KEY = "CELERY_WORK_DIR"
COSMO_APP_NAME = "cloudify"

PLUGIN_INSTALLER_PLUGIN_PATH = "plugin_installer.tasks"
KV_STORE_PLUGIN_PATH = "kv_store.tasks"
AGENT_INSTALLER_PLUGIN_PATH = "worker_installer.tasks"
OPENSTACK_PROVISIONER_PLUGIN_PATH = "openstack_host_provisioner.tasks"
VAGRANT_PROVISIONER_PLUGIN_PATH = "vagrant_host_provisioner.tasks"

MANAGEMENT_NODE_ID = "cloudify.management"

MANAGER_FILE_SERVER_URL_KEY = "MANAGER_FILE_SERVER_URL"

MANAGER_IP_KEY = "MANAGEMENT_IP"
LOCAL_IP_KEY = "AGENT_IP"
MANAGER_REST_PORT_KEY = "MANAGER_REST_PORT"
MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY = \
    "MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL"

BUILT_IN_AGENT_PLUGINS = [PLUGIN_INSTALLER_PLUGIN_PATH, KV_STORE_PLUGIN_PATH]

BUILT_IN_MANAGEMENT_PLUGINS = [PLUGIN_INSTALLER_PLUGIN_PATH,
                               KV_STORE_PLUGIN_PATH,
                               AGENT_INSTALLER_PLUGIN_PATH]
