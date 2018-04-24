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

REST_HOST_KEY = 'REST_HOST'
REST_PORT_KEY = 'REST_PORT'
CELERY_WORK_DIR_KEY = 'CELERY_WORK_DIR'
MANAGER_FILE_SERVER_URL_KEY = 'MANAGER_FILE_SERVER_URL'
MANAGER_FILE_SERVER_ROOT_KEY = 'MANAGER_FILE_SERVER_ROOT'
FILE_SERVER_RESOURCES_FOLDER = 'resources'
FILE_SERVER_BLUEPRINTS_FOLDER = 'blueprints'
FILE_SERVER_DEPLOYMENTS_FOLDER = 'deployments'
FILE_SERVER_UPLOADED_BLUEPRINTS_FOLDER = 'uploaded-blueprints'
FILE_SERVER_SNAPSHOTS_FOLDER = 'snapshots'
FILE_SERVER_PLUGINS_FOLDER = 'plugins'
FILE_SERVER_GLOBAL_RESOURCES_FOLDER = 'global-resources'
FILE_SERVER_TENANT_RESOURCES_FOLDER = 'tenant-resources'

AGENT_INSTALL_METHOD_NONE = 'none'
AGENT_INSTALL_METHOD_REMOTE = 'remote'
AGENT_INSTALL_METHOD_INIT_SCRIPT = 'init_script'
AGENT_INSTALL_METHOD_PROVIDED = 'provided'
AGENT_INSTALL_METHOD_PLUGIN = 'plugin'
AGENT_INSTALL_METHODS = [
    AGENT_INSTALL_METHOD_NONE,
    AGENT_INSTALL_METHOD_REMOTE,
    AGENT_INSTALL_METHOD_INIT_SCRIPT,
    AGENT_INSTALL_METHOD_PROVIDED,
    AGENT_INSTALL_METHOD_PLUGIN
]
AGENT_INSTALL_METHODS_SCRIPTS = [
    AGENT_INSTALL_METHOD_INIT_SCRIPT,
    AGENT_INSTALL_METHOD_PROVIDED,
    AGENT_INSTALL_METHOD_PLUGIN
]

COMPUTE_NODE_TYPE = 'cloudify.nodes.Compute'

BROKER_PORT_NO_SSL = 5672
BROKER_PORT_SSL = 5671
CELERY_TASK_RESULT_EXPIRES = 600
CLOUDIFY_TOKEN_AUTHENTICATION_HEADER = 'Authentication-Token'
LOCAL_REST_CERT_FILE_KEY = 'LOCAL_REST_CERT_FILE'
SECURED_PROTOCOL = 'https'

BROKER_SSL_CERT_PATH = 'BROKER_SSL_CERT_PATH'
BYPASS_MAINTENANCE = 'BYPASS_MAINTENANCE'
LOGGING_CONFIG_FILE = '/etc/cloudify/logging.conf'
CLUSTER_SETTINGS_PATH_KEY = 'CLOUDIFY_CLUSTER_SETTINGS_PATH'

MGMTWORKER_QUEUE = 'cloudify.management'
DEPLOYMENT = 'deployment'
NODE_INSTANCE = 'node-instance'
RELATIONSHIP_INSTANCE = 'relationship-instance'

DEFAULT_NETWORK_NAME = 'default'
