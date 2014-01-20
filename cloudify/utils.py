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

import fnmatch
import os
from os import path
from constants import CLOUDIFY_APP_DIR_KEY


MANAGER_IP_KEY = "MANAGEMENT_IP"
LOCAL_IP_KEY = "AGENT_IP"
MANAGER_REST_PORT_KEY = "MANAGER_REST_PORT"


def get_app_dir():
    """
    Returns the directory celery tasks are stored within.
    """
    return os.environ[CLOUDIFY_APP_DIR_KEY]


def get_local_ip():
    return os.environ[LOCAL_IP_KEY]


def get_manager_ip():
    return os.environ[MANAGER_IP_KEY]


def get_manager_rest_service_port():
    return int(os.environ[MANAGER_REST_PORT_KEY])


def get_cosmo_properties():
    return {
        "management_ip": get_manager_ip(),
        "ip": get_local_ip()
    }


def build_includes(celery_app_root_dir):
    """
    Returns a list of celery included modules
    (all python modules under root dir).
    """
    if not path.exists(celery_app_root_dir):
        raise IOError(
            "Celery application root directory: {0} not found"
            .format(celery_app_root_dir))

    includes = []

    app_root_dir = path.realpath(path.join(celery_app_root_dir, '..'))

    for root, dirnames, filenames in os.walk(celery_app_root_dir):
        for filename in fnmatch.filter(filenames, '*.py'):
            if filename.startswith("test"):
                continue
            elif filename == '__init__.py':
                includes.append(root)
            else:
                includes.append(os.path.join(root, filename))

    # remove .py suffix from include
    includes = map(lambda include: include[:-3]
                   if include.endswith('.py') else include, includes)

    # remove path prefix to start with cosmo
    includes = map(lambda include: include.replace(app_root_dir, ''), includes)

    # replace slashes with dots in include path
    includes = map(lambda include: include.replace('/', '.'), includes)

    # remove the dot at the start
    includes = map(lambda include: include[1:], includes)

    return includes
