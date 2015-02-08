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


def setup_default_logger(logger_name, level=logging.DEBUG):
    """
    :param logger_name: Name of the logger.
    :return: A logger instance.
    :rtype: Logger
    """

    # clear all other handlers

    logger = logging.getLogger(logger_name)
    for handler in logger.handlers:
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] '
                                      '[%(name)s] %(message)s',
                                  datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


def get_local_ip():

    """
    Return the IP address used to connect to this machine by the management.
    machine
    """

    return os.environ[LOCAL_IP_KEY]


def get_manager_ip():
    """
    Returns the IP address of manager inside the management network.
    """
    return os.environ[MANAGER_IP_KEY]


def get_manager_file_server_blueprints_root_url():
    """
    Returns the blueprints root url in the file server.
    """
    return os.environ[MANAGER_FILE_SERVER_BLUEPRINTS_ROOT_URL_KEY]


def get_manager_file_server_url():
    """
    Returns the manager file server base url.
    """
    return os.environ[MANAGER_FILE_SERVER_URL_KEY]


def get_manager_rest_service_port():
    """
    Returns the port the manager REST service is running on.
    """
    return int(os.environ[MANAGER_REST_PORT_KEY])


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """
    Generate and return a random string using upper case letters and digits.
    """
    return ''.join(random.choice(chars) for x in range(size))


def create_temp_folder():
    """
    Create a temporary folder.
    """
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


class LocalCommandRunner(object):

    def __init__(self, logger=None, host='localhost'):

        """
        :param logger: This logger will be used for
                       printing the output and the command.
        """

        logger = logger or setup_default_logger('LocalCommandRunner')
        self.logger = logger
        self.host = host

    def run(self, command,
            exit_on_failure=True,
            stdout_pipe=True,
            stderr_pipe=True):

        """
        Runs local commands.

        :param command: The command to execute.
        :param exit_on_failure: False to ignore failures.
        :param stdout_pipe: False to not pipe the standard output.
        :param stderr_pipe: False to not pipe the standard error.

        :return: A wrapper object for all valuable info from the execution.
        :rtype: CommandExecutionResponse
        """

        self.logger.info('[{0}] run: {1}'.format(self.host, command))
        shlex_split = shlex.split(command)
        stdout = subprocess.PIPE if stdout_pipe else None
        stderr = subprocess.PIPE if stderr_pipe else None
        p = subprocess.Popen(shlex_split, stdout=stdout,
                             stderr=stderr)
        out, err = p.communicate()
        if p.returncode == 0:
            if out:
                self.logger.info('[{0}] out: {1}'.format(self.host, out))
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

    :param command: The command that was executed.
    :param std_out: The output from the execution.
    :param std_err: The error message from the execution.
    :param return_code: The return code from the execution.
    """

    def __init__(self, command, std_out, std_err, return_code):
        self.command = command
        self.std_out = std_out
        self.std_err = std_err
        self.return_code = return_code
