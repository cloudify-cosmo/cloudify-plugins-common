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


class HttpException(Exception):
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
        Exception.__init__(self, self.__str__())

    def __str__(self):
        return "{0} ({1}) : {2}".format(self.code, self.url, self.message)


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

    def __init__(self, retry_after=None, *args, **kwargs):
        """
        :param retry_after: How many seconds should the workflow engine wait
                            before re-executing the task the raised this
                            exception. (only applies when the workflow engine
                            decides that this task should be retried)
        """
        super(RecoverableError, self).__init__(*args, **kwargs)
        self.retry_after = retry_after
