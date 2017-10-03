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

from cloudify import constants

from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.client import HTTPClient
from cloudify_rest_client.exceptions import (CloudifyClientError,
                                             NotClusterMaster)


def _get_cluster_settings_file(filename=None):
    if filename is None \
            and constants.CLUSTER_SETTINGS_PATH_KEY not in os.environ:
        return None
    return filename or os.environ[constants.CLUSTER_SETTINGS_PATH_KEY]


def get_cluster_settings(filename=None):
    filename = _get_cluster_settings_file(filename=filename)
    if not filename:
        return None
    try:
        with open(filename) as f:
            return json.load(f)
    except (IOError, ValueError):
        return None


def set_cluster_settings(settings, filename=None):
    filename = _get_cluster_settings_file(filename=filename)
    if not filename:
        return None
    dirname = os.path.dirname(filename)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(filename, 'w') as f:
        json.dump(settings, f, indent=4, sort_keys=True)


def get_cluster_nodes(filename=None):
    settings = get_cluster_settings(filename=filename)
    if not settings:
        return None
    return settings.get('nodes')


def set_cluster_nodes(nodes, filename=None):
    settings = get_cluster_settings(filename=filename) or {}
    if not settings and not nodes:
        return
    settings['nodes'] = nodes
    set_cluster_settings(settings, filename=filename)
    active = get_cluster_active(filename=filename)
    if active is None:
        set_cluster_active(nodes[0], filename=filename)
    return nodes


def get_cluster_active(filename=None):
    settings = get_cluster_settings(filename=filename)
    if not settings:
        return None

    active = settings.get('active')
    if active:
        return active
    # when we don't know which node is the active, try the first one on the
    # list - if it's a replica, we'll failover normally
    nodes = get_cluster_nodes(filename=filename)
    if nodes:
        set_cluster_active(nodes[0], filename=filename)
    return nodes[0]


def set_cluster_active(node, filename=None):
    settings = get_cluster_settings(filename=filename) or {}
    if not settings and not node:
        return
    settings['active'] = node
    set_cluster_settings(settings, filename=filename)


def get_cluster_amqp_settings():
    active = get_cluster_active()
    if active:
        return {'amqp_host': active}
    else:
        return {}


def delete_cluster_settings(filename=None):
    """Remove all cluster settings.

    Delete the settings file, and also all stored certificates - find the
    certs dir first.
    """
    filename = _get_cluster_settings_file(filename)
    if not filename:
        return
    try:
        os.remove(filename)
    except (OSError, IOError):
        # the file doesn't exist?
        pass


def is_cluster_configured(filename=None):
    path = _get_cluster_settings_file(filename=filename)
    if not path or not os.path.exists(path):
        return False
    return get_cluster_active(filename=path) is not None


def _parse_url(broker_url):
    params = pika.URLParameters(broker_url)
    return params.host


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

    def _use_node(self, node_ip):
        self.host = node_ip


class CloudifyClusterClient(CloudifyClient):
    client_class = ClusterHTTPClient
