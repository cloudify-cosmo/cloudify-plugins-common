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

import logging
import random
import shlex
import string
import subprocess
import tempfile
import sys
import os

from cloudify.exceptions import CommandExecutionException
from cloudify.constants import LOCAL_IP_KEY, MANAGER_IP_KEY, \
    MANAGER_REST_PORT_KEY, MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY, \
    MANAGER_FILE_SERVER_URL_KEY


def setup_default_logger(logger_name):
    root = logging.getLogger()
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] '
                                      '[%(name)s] %(message)s',
                                  datefmt='%H:%M:%S')
    ch.setFormatter(formatter)
    # clear all other handlers
    for handler in root.handlers:
        root.removeHandler(handler)
    root.addHandler(ch)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    return logger


def get_local_ip():
    return os.environ[LOCAL_IP_KEY]


def get_manager_ip():
    return os.environ[MANAGER_IP_KEY]


def get_manager_file_server_blueprints_root_url():
    return os.environ[MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY]


def get_manager_file_server_url():
    return os.environ[MANAGER_FILE_SERVER_URL_KEY]


def get_manager_rest_service_port():
    return int(os.environ[MANAGER_REST_PORT_KEY])


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


def create_temp_folder():
    path_join = os.path.join(tempfile.gettempdir(), id_generator(5))
    os.makedirs(path_join)
    return path_join


def get_cosmo_properties():
    return {
        "management_ip": get_manager_ip(),
        "ip": get_local_ip()
    }


def find_type_in_kwargs(cls, all_args):
    result = [v for v in all_args if isinstance(v, cls)]
    if not result:
        return None
    if len(result) > 1:
        raise RuntimeError(
            "Expected to find exactly one instance of {0} in "
            "kwargs but found {1}".format(cls, len(result)))
    return result[0]


def get_machine_ip(ctx):
    if 'ip' in ctx.properties:
        return ctx.properties['ip']  # priority for statically specifying ip.
    if 'ip' in ctx.runtime_properties:
        return ctx.runtime_properties['ip']
    raise ValueError('ip property is not set for node: {0}. '
                     'This is mandatory for installing an agent remotely'
                     .format(ctx.node_id))


class LocalCommandRunner(object):

    '''
    Runs local command.

    '''

    def __init__(self, logger=setup_default_logger('LocalCommandRunner')):

        '''

        :param logger: This logger will be
        used for printing the output and the command.
        '''
        self.logger = logger

    def run(self, command, exit_on_failure=True):
        self.logger.debug('[{0}] run: {1}'.format(get_local_ip(), command))
        shlex_split = shlex.split(command)
        p = subprocess.Popen(shlex_split, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            self.logger.debug('[{0}] out: {1}'.format(get_local_ip(), out))
        else:
            error = CommandExecutionException(
                command=command,
                code=p.returncode,
                error=err,
                output=out)
            self.logger.error(error)
            if exit_on_failure:
                raise error

        return CommandExecutionResponse(command=command,
                                        std_out=out,
                                        std_err=err,
                                        return_code=p.returncode)


class CommandExecutionResponse(object):

    """
    Wrapper object for info returned when running commands.

    'command' - The command that was executed.
    'std_err' - The error message from the execution.
    'std_out' - The output from the execution.
    'return_code' - The return code from the execution.

    """

    def __init__(self, command, std_out, std_err, return_code):
        self.command = command
        self.std_out = std_out
        self.std_err = std_err
        self.return_code = return_code
