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

import json
import bernhard
from utils import get_cosmo_properties


def send_event(host, service, type, value):
    event = {
        'host': host,
        'service': service,
        type: value,
    }
    if is_cosmo_env():
        _send_event(event)
    else:
        print "not running inside cosmo. event is not sent : {0}".format(event)


def send_reachable(node_id, host):
    send_event(host, node_id, 'state', 'running')


def send_log_event(log_record):
    if is_cosmo_env():
        host = get_cosmo_properties()['ip']
        description = {
            'log_record': log_record
        }
        event = {
            'host': host,
            'service': 'celery-task-log',
            'state': '',
            'tags': ['cosmo-log'],
            'description': json.dumps(description)
        }
        try:
            _send_event(event)
        except BaseException as e:
            print "caught exception while sending log event : " \
                  "{0} . log was : {1}".format(e.message, log_record)
    else:
        print "not running inside cosmo. log event is not sent : {0}"\
            .format(log_record)


def _send_event(event):
    client = _get_riemann_client()
    try:
        client.send(event)
    finally:
        client.disconnect()


def _get_riemann_client():
    host = get_cosmo_properties()['management_ip']
    return bernhard.Client(host=host)


def is_cosmo_env():
    try:
        get_cosmo_properties()
        return True
    except RuntimeError:
        return False
