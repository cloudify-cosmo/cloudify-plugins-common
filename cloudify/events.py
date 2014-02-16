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

__author__ = 'idanmo'

import bernhard
import utils


def send_event(host, service, event_type, value, tags=[], ttl=60):
    event = {
        'host': host,
        'service': service,
        event_type: value,
        'ttl': ttl,
        'tags': tags
    }
    _send_event(event)


def _send_event(event):
    client = _get_riemann_client()
    try:
        client.send(event)
    finally:
        client.disconnect()


def _get_riemann_client():
    return bernhard.Client(host=utils.get_manager_ip())
