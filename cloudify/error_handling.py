########
# Copyright (c) 2018 GigaSpaces Technologies Ltd. All rights reserved
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

import StringIO
import traceback

from cloudify.utils import format_exception
from cloudify.exceptions import (
    OperationRetry,
    RecoverableError,
    NonRecoverableError,
    ProcessExecutionError,
    HttpException
)

def serialize_known_exception(e):
    tb = StringIO.StringIO()
    traceback.print_exc(file=tb)
    trace_out = tb.getvalue()

    # Needed because HttpException constructor sucks
    append_message = False
    # Convert exception to a know exception type that can be deserialized
    # by the calling process
    known_exception_type_args = []
    if isinstance(e, ProcessExecutionError):
        known_exception_type = ProcessExecutionError
        known_exception_type_args = [e.error_type, e.traceback]
        trace_out = e.traceback
    elif isinstance(e, HttpException):
        known_exception_type = HttpException
        known_exception_type_args = [e.url, e.code]
        append_message = True
    elif isinstance(e, NonRecoverableError):
        known_exception_type = NonRecoverableError
    elif isinstance(e, OperationRetry):
        known_exception_type = OperationRetry
        known_exception_type_args = [e.retry_after]
    elif isinstance(e, RecoverableError):
        known_exception_type = RecoverableError
        known_exception_type_args = [e.retry_after]
    else:
        # convert pure user exceptions to a RecoverableError
        known_exception_type = RecoverableError

    try:
        causes = e.causes
    except AttributeError:
        causes = []

    payload = {
        'traceback': trace_out,
        'exception_type': type(e).__name__,
        'message': format_exception(e),
        'known_exception_type': known_exception_type.__name__,
        'known_exception_type_args': known_exception_type_args,
        'known_exception_type_kwargs': {'causes': causes or []},
        'append_message': append_message,
    }
