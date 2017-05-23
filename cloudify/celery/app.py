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
import ssl

from cloudify import broker_config
from cloudify.constants import CELERY_TASK_RESULT_EXPIRES
from cloudify.constants import MGMTWORKER_QUEUE, BROKER_PORT_SSL


def get_celery_app(broker_url=None,
                   broker_ssl_cert_path=None,
                   max_retries=None,
                   tenant=None,
                   target=None):
    """
    Return a Celery app
    
    :param broker_url: If supplied, will be used as the broker URL 
    :param broker_ssl_cert_path: If not supplied, default is in broker_config
    :param tenant: If supplied, and if target isn't the mgmtworker queue,
    the broker URL will be derived from the data kept in it
    :param target: The target queue; see `tenant`
    :param max_retries: 
    :return: A celery.Celery object
    """
    # celery is imported locally since it's not used by any other method, and
    # we want this utils module to be usable even if celery is not available
    from celery import Celery

    broker_url = broker_url or _get_broker_url(tenant, target)

    celery_client = Celery()
    celery_client.conf.update(
        BROKER_URL=broker_url,
        CELERY_RESULT_BACKEND=broker_url,
        CELERY_TASK_RESULT_EXPIRES=CELERY_TASK_RESULT_EXPIRES
    )

    ssl_cert_path = broker_ssl_cert_path or broker_config.broker_cert_path
    celery_client.conf.BROKER_USE_SSL = {
        'ca_certs': ssl_cert_path,
        'cert_reqs': ssl.CERT_REQUIRED,
    }

    # Connect eagerly to error out as early as possible, and to force choosing
    # the broker if multiple urls were passed.
    # If max_retries is provided and >0, we will raise an exception if we
    # can't connect; otherwise we'll keep retrying forever.
    # Need to raise an exception in the case of a cluster, so that the
    # next node can be tried
    celery_client.pool.connection.ensure_connection(max_retries=max_retries)
    return celery_client


def get_cluster_celery_app(broker_urls, cluster):
    err = None
    for broker_url, node in zip(broker_urls, cluster):
        try:
            return get_celery_app(
                broker_url=broker_url,
                broker_ssl_cert_path=node.get('internal_cert_path'),
                max_retries=1)
        except Exception as err:
            continue
    if err is not None:
        raise err


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
