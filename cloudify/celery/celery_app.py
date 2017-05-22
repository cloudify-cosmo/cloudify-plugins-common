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

import os

from cloudify import broker_config
from cloudify.constants import MGMTWORKER_QUEUE, BROKER_PORT_SSL


def celery_app(tenant=None, target=None):
    """
    Return a celery app set with either the credentials for the mgmtworker,
    or a remote agent (depending on the target).
    """
    from cloudify_agent.api.utils import get_celery_client
    default_celery_config = os.environ['CELERY_CONFIG_MODULE']

    broker_url = _get_broker_url(tenant, target)
    try:
        app = get_celery_client(broker_url, broker_config.broker_cert_path)
    finally:
        os.environ['CELERY_CONFIG_MODULE'] = default_celery_config

    return app


def _get_broker_url(tenant, target):
    """
    If the target is the mgmtworker queue, or if no tenants was passed use
    the default broker URL. Otherwise, create a tenant-specific one
    """
    if target == MGMTWORKER_QUEUE or not tenant:
        return broker_config.BROKER_URL
    else:
        # The celery configuration set in the env var supersedes
        # the `broker_url` param, but here we connect with
        # credentials other than the default ones for the mgmtworker
        os.environ['CELERY_CONFIG_MODULE'] = ''
        return _get_tenant_broker_url(tenant)


def _get_tenant_broker_url(tenant):
    return broker_config.URL_TEMPLATE.format(
        username=tenant['rabbitmq_username'],
        password=tenant['rabbitmq_password'],
        hostname=broker_config.broker_hostname,
        port=BROKER_PORT_SSL,
        vhost=tenant['rabbitmq_vhost'],
        options=''
    )
