########
# Copyright (c) 2017 GigaSpaces Technologies Ltd. All rights reserved
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
import json
import pika
import types
import requests
import itertools

from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.client import HTTPClient
from cloudify_rest_client.exceptions import (CloudifyClientError,
                                             NotClusterMaster)


def _get_cluster_settings_file():
    filename = os.path.expanduser('~/.cfy-agent/cloudify-cluster')
    if not filename:
        return None
    return filename


def get_cluster_settings():
    filename = _get_cluster_settings_file()
    if not filename:
        return None
    try:
        with open(filename) as f:
            return json.load(f)
    except (IOError, ValueError):
        return None


def set_cluster_settings(settings):
    filename = _get_cluster_settings_file()
    if not filename:
        return None
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(filename, 'w') as f:
        json.dump(settings, f, indent=4, sort_keys=True)


def get_cluster_nodes():
    settings = get_cluster_settings()
    if not settings:
        return None
    return settings.get('nodes')


def set_cluster_nodes(nodes):
    settings = get_cluster_settings() or {}

    certs_dir = os.path.dirname(_get_cluster_settings_file())
    for node in nodes:
        node_ip = node['broker_ip']
        for key in ['broker_ssl_cert', 'rest_cert']:
            cert_content = node.get(key)
            if cert_content:
                cert_filename = os.path.join(
                    certs_dir, '{0}_{1}.crt'.format(key, node_ip))
                with open(cert_filename, 'w') as f:
                    f.write(cert_content)
                node['{0}_path'.format(key)] = cert_filename

    settings['nodes'] = nodes
    set_cluster_settings(settings)


def get_cluster_active():
    settings = get_cluster_settings()
    if not settings:
        return None
    return settings.get('active')


def set_cluster_active(node):
    settings = get_cluster_settings() or {}
    settings['active'] = node
    set_cluster_settings(settings)


def get_cluster_amqp_settings():
    active = get_cluster_active()
    if not active:
        return {}
    settings = {
        'amqp_host': active.get('broker_ip'),
        'amqp_user': active.get('broker_user'),
        'amqp_pass': active.get('broker_pass'),
    }
    ssl_cert_path = active.get('broker_ssl_cert_path')
    if ssl_cert_path:
        settings['ssl_cert_path'] = ssl_cert_path
    return settings


def is_cluster_configured():
    return get_cluster_active() is not None


def _parse_url(broker_url):
    params = pika.URLParameters(broker_url)
    return {
        'broker_user': params.credentials.username,
        'broker_pass': params.credentials.password,
        'broker_ip': params.host
    }


def config_from_broker_urls(active, nodes):
    settings = {
        'active': _parse_url(active),
        'nodes': [_parse_url(node) for node in nodes if node]
    }
    set_cluster_settings(settings)


class ClusterHTTPClient(HTTPClient):
    default_timeout_sec = 5
    retries = 30
    retry_interval = 3

    def __init__(self, *args, **kwargs):
        super(ClusterHTTPClient, self).__init__(*args, **kwargs)

    def do_request(self, *args, **kwargs):
        kwargs.setdefault('timeout', self.default_timeout_sec)

        copied_data = None
        if isinstance(kwargs.get('data'), types.GeneratorType):
            copied_data = itertools.tee(kwargs.pop('data'), self.retries)

        for retry in range(self.retries):
            active = get_cluster_active()
            if active is not None:
                self._use_node(active)
            if copied_data is not None:
                kwargs['data'] = copied_data[retry]

            try:
                return super(ClusterHTTPClient, self).do_request(*args,
                                                                 **kwargs)
            except (NotClusterMaster, requests.exceptions.ConnectionError):
                    continue

        raise CloudifyClientError('No active node in the cluster!')

    def _use_node(self, node):
        self.host = node['broker_ip']
        self.cert = node.get('rest_cert_path')


class CloudifyClusterClient(CloudifyClient):
    client_class = ClusterHTTPClient
