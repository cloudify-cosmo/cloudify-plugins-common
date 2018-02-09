########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

# AMQP broker configuration for agents and manager
# Primarily used by celery, so provided with variables it understands
from __future__ import absolute_import

import json
import os
import ssl

from cloudify.constants import BROKER_PORT_SSL, BROKER_PORT_NO_SSL

workdir_path = os.getenv('CELERY_WORK_DIR')
if workdir_path is None:
    # We are not in an appropriately configured celery environment
    config = {}
else:
    conf_file_path = os.path.join(workdir_path, 'broker_config.json')
    if os.path.isfile(conf_file_path):
        with open(conf_file_path) as conf_handle:
            conf_file = conf_handle.read()
            config = json.loads(conf_file)
    else:
        config = {}

# Provided as variables for retrieval by amqp_client and logger as required
broker_cert_path = config.get('broker_cert_path', '')
broker_username = config.get('broker_username', 'guest')
broker_password = config.get('broker_password', 'guest')
broker_hostname = config.get('broker_hostname', 'localhost')
broker_vhost = config.get('broker_vhost', '/')
broker_ssl_enabled = config.get('broker_ssl_enabled', False)
broker_port = BROKER_PORT_SSL if broker_ssl_enabled else BROKER_PORT_NO_SSL

# only enable heartbeat by default for agents connected to a cluster
DEFAULT_HEARTBEAT = 30
if os.name == 'nt':
    # celery doesn't support broker_heartbeat on windows
    broker_heartbeat = None
else:
    broker_heartbeat = config.get('broker_heartbeat', DEFAULT_HEARTBEAT)


if broker_ssl_enabled:
    BROKER_USE_SSL = {
        'ca_certs': broker_cert_path,
        'cert_reqs': ssl.CERT_REQUIRED,
    }

if broker_heartbeat:
    options = '?heartbeat={heartbeat}'.format(heartbeat=broker_heartbeat)
else:
    options = ''

# BROKER_URL is held in the config to avoid the password appearing
# in ps listings
URL_TEMPLATE = \
    'amqp://{username}:{password}@{hostname}:{port}/{vhost}{options}'
if config.get('cluster'):
    BROKER_URL = ';'.join(URL_TEMPLATE.format(username=broker_username,
                                              password=broker_password,
                                              hostname=node_ip,
                                              port=broker_port,
                                              vhost=broker_vhost,
                                              options=options)
                          for node_ip in config['cluster'])
else:
    BROKER_URL = URL_TEMPLATE.format(
        username=broker_username,
        password=broker_password,
        hostname=broker_hostname,
        port=broker_port,
        vhost=broker_vhost,
        options=options
    )

# celery will not use the failover strategy if there is only one broker url;
# we need it to try and failover even with one initial manager, because
# another node might've been added dynamically, while the worker was already
# running; we add an empty broker url so that celery always sees at least two -
# the failover strategy we're using (defined in cloudify_agent.app) filters out
# the empty one
BROKER_URL += ';'

CELERY_RESULT_BACKEND = BROKER_URL
CELERY_TASK_RESULT_EXPIRES = 600
CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_ACKS_LATE = False
