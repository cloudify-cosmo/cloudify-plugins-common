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

__author__ = 'elip'


class NonRecoverableError(Exception):
    """
    An error raised by plugins to denote that no retry should be attempted.
    """
    pass


class RecoverableError(Exception):
    """
    An error raised by plugins to explicitly denote that this is a recoverable
    error (note that this is the default behavior). It is possible specifying
    how many seconds should pass before a retry is attempted thus overriding
    the bootstrap context configuration parameter:
    cloudify.workflows.retry_interval
    """

    def __init__(self, message=None, retry_after=None):
        """
        :param retry_after: How many seconds should the workflow engine wait
                            before re-executing the task the raised this
                            exception. (only applies when the workflow engine
                            decides that this task should be retried)
        """
        message = message or ''
        if retry_after is not None:
            message = '{} [retry_after={}]'.format(message, retry_after)
        super(RecoverableError, self).__init__(message)
        self.retry_after = retry_after


class HttpException(NonRecoverableError):
    """
    Wraps any Http based exceptions that may arise in our code.

    'url' - The url the request was made to.
    'code' - The response status code.
    'message' - The underlying reason for the error.

    """

    def __init__(self, url, code, message):
        self.url = url
        self.code = code
        self.message = message
        super(HttpException, self).__init__(str(self))

    def __str__(self):
        return "{0} ({1}) : {2}".format(self.code, self.url, self.message)


class CommandExecutionException(Exception):

    """
    Indicates a failure to execute a command.

    'command' - The command that was executed
    'code' - The error code from the execution.
    'message' - The error from the execution

    """

    def __init__(self, command, error, output, code):
        self.command = command
        self.error = error
        self.code = code
        self.output = output
        Exception.__init__(self, self.__str__())

    def __str__(self):
        return "Failed executing command: {0}\ncode: {1}\nerror: {2}\nmessage: {3}"\
               .format(self.command, self.code, self.error, self.output)


class TimeoutException(Exception):

    """
    Indicates some kind of timeout happened.

    """
    def __init__(self, *args):
        Exception.__init__(self, args)
