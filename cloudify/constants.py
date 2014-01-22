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
COSMO_APP_NAME = "cloudify"
CLOUDIFY_APP_DIR_KEY = 'CLOUDIFY_APP_DIR'

PLUGIN_INSTALLER_PLUGIN_PATH = "plugin_installer.tasks"
KV_STORE_PLUGIN_PATH = "kv_store.tasks"

BUILT_IN_AGENT_PLUGINS = [PLUGIN_INSTALLER_PLUGIN_PATH, KV_STORE_PLUGIN_PATH]
