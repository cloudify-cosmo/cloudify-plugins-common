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


def get_app_dir():
    """
    Returns the directory celery tasks are stored within.
    """
    return os.environ[CLOUDIFY_APP_DIR_KEY]


def get_cosmo_properties():
    management_ip_key = "MANAGEMENT_IP"
    agent_ip_key = "AGENT_IP"
    if management_ip_key not in os.environ:
        raise RuntimeError("{0} is not set in environment".format(management_ip_key))
    if agent_ip_key not in os.environ:
        raise RuntimeError("{0} is not set in environemnt".format(agent_ip_key))
    return {
        "management_ip": os.environ[management_ip_key],
        "ip": os.environ[agent_ip_key]
    }


def build_includes(celery_app_root_dir):
    """
    Returns a list of celery included modules (all python modules under root dir).
    """
    if not path.exists(celery_app_root_dir):
        raise IOError("Celery application root directory: {0} not found".format(celery_app_root_dir))

    includes = []

    app_root_dir = path.realpath(path.join(celery_app_root_dir, '..'))

    for root, dirnames, filenames in os.walk(celery_app_root_dir):
        for filename in fnmatch.filter(filenames, '*.py'):
            if filename == '__init__.py':
                includes.append(root)
            else:
                includes.append(os.path.join(root, filename))

    # remove .py suffix from include
    includes = map(lambda include: include[:-3] if include.endswith('.py') else include, includes)

    # remove path prefix to start with cosmo
    includes = map(lambda include: include.replace(app_root_dir, ''), includes)

    # replace slashes with dots in include path
    includes = map(lambda include: include.replace('/', '.'), includes)

    # remove the dot at the start
    includes = map(lambda include: include[1:], includes)

    return includes
