########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


EXECUTION_CANCELLED_RESULT = 'execution_cancelled'


ctx = None
pipe = None


def has_cancel_request():
    """
    Checks for requests to cancel the workflow execution.
    This should be used to allow graceful termination of workflow executions.

    If this method is not used and acted upon, a simple 'cancel'
    request for the execution will have no effect - 'force-cancel' will have
    to be used to abruptly terminate the execution instead.

    Note: When using this method, the workflow must return
    ``EXECUTION_CANCELLED_RESULT`` if indeed the execution gets cancelled.

    :return: whether there was a request to cancel the workflow execution
    """
    if pipe and pipe.poll():
        data = pipe.recv()
        return data['action'] == 'cancel'
    return False


class ExecutionCancelled(Exception):
    """
    Raised by the workflow engine when the workflow was cancelled during
    a blocking call to some workflow task's ``task.get()``
    """
    pass
