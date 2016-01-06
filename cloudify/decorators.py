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


import traceback
import copy
import sys
import Queue
from StringIO import StringIO
from functools import wraps

from cloudify import context
from cloudify import amqp_client_utils
from cloudify.amqp_client_utils import AMQPWrappedThread
from cloudify.workflows.workflow_context import (
    CloudifyWorkflowContext,
    CloudifySystemWideWorkflowContext)
from cloudify.manager import update_execution_status, get_rest_client
from cloudify.workflows import api
from cloudify_rest_client.executions import Execution
from cloudify import exceptions
from cloudify.state import current_ctx, current_workflow_ctx


def _stub_task(fn):
    return fn
try:
    from cloudify_agent.app import app as _app
    _task = _app.task
except ImportError as e:
    _app = None
    _task = _stub_task


CLOUDIFY_ID_PROPERTY = '__cloudify_id'
CLOUDIFY_NODE_STATE_PROPERTY = 'node_state'
CLOUDIFY_CONTEXT_PROPERTY_KEY = '__cloudify_context'
CLOUDIFY_CONTEXT_IDENTIFIER = '__cloudify_context'


def _is_cloudify_context(obj):
    """
    Gets whether the provided obj is a CloudifyContext instance.
    From some reason Python's isinstance returned False when it should
    have returned True.
    """
    return context.CloudifyContext.__name__ in obj.__class__.__name__


def _find_context_arg(args, kwargs, is_context):
    """
    Find cloudify context in args or kwargs.
    Cloudify context is either a dict with a unique identifier (passed
        from the workflow engine) or an instance of CloudifyContext.
    """
    for arg in args:
        if is_context(arg):
            return arg
        if isinstance(arg, dict) and CLOUDIFY_CONTEXT_IDENTIFIER in arg:
            return arg
    for arg in kwargs.values():
        if is_context(arg):
            return arg
    return kwargs.get(CLOUDIFY_CONTEXT_PROPERTY_KEY)


def operation(func=None, **arguments):
    """
    Decorate plugin operation function with this decorator.
    Internally, if celery is installed, will also wrap the function
    with a ``@celery.task`` decorator

    The ``ctx`` injected to the function arguments is of type
    ``cloudify.context.CloudifyContext``

    The ``ctx`` object can also be accessed by importing
    ``cloudify.ctx``


    Example::

        from cloudify import ctx

        @operations
        def start(**kwargs):
            pass
    """
    if func is not None:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = _find_context_arg(args, kwargs, _is_cloudify_context)
            if ctx is None:
                ctx = {}
            if not _is_cloudify_context(ctx):
                ctx = context.CloudifyContext(ctx)
                # remove __cloudify_context
                raw_context = kwargs.pop(CLOUDIFY_CONTEXT_PROPERTY_KEY, {})
                if ctx.task_target:
                    # this operation requires an AMQP client
                    amqp_client_utils.init_amqp_client()
                else:
                    # task is local (not through celery) so we need to
                    # clone kwarg, and an amqp client is not required
                    kwargs = copy.deepcopy(kwargs)
                if raw_context.get('has_intrinsic_functions') is True:
                    kwargs = ctx._endpoint.evaluate_functions(payload=kwargs)
                kwargs['ctx'] = ctx
            try:
                current_ctx.set(ctx, kwargs)
                result = func(*args, **kwargs)
            except BaseException as e:
                ctx.logger.error(
                    'Exception raised on operation [%s] invocation',
                    ctx.task_name, exc_info=True)

                if ctx.task_target is None:
                    # local task execution
                    # no serialization issues
                    raise

                # extract exception details
                # type, value, traceback
                tpe, value, tb = sys.exc_info()

                # we re-create the exception here
                # since it will be sent
                # over the wire. And the original exception
                # may cause de-serialization issues
                # on the other side.

                # preserve original type in the message
                message = '{0}: {1}'.format(tpe.__name__, str(e))

                # if the exception type is directly one of our exception
                # than there is no need for conversion and we can just
                # raise the original exception
                if type(e) in [exceptions.OperationRetry,
                               exceptions.RecoverableError,
                               exceptions.NonRecoverableError,
                               exceptions.HttpException]:
                    raise

                # if the exception inherits from our base exceptions, there
                # still might be a de-serialization problem caused by one of
                # the types in the inheritance tree.
                if isinstance(e, exceptions.NonRecoverableError):
                    value = exceptions.NonRecoverableError(message)
                elif isinstance(e, exceptions.OperationRetry):
                    value = exceptions.OperationRetry(message, e.retry_after)
                elif isinstance(e, exceptions.RecoverableError):
                    value = exceptions.RecoverableError(message, e.retry_after)
                else:
                    # convert pure user exceptions
                    # to a RecoverableError
                    value = exceptions.RecoverableError(message)

                raise type(value), value, tb

            finally:
                amqp_client_utils.close_amqp_client()
                current_ctx.clear()
                if ctx.type == context.NODE_INSTANCE:
                    ctx.instance.update()
                elif ctx.type == context.RELATIONSHIP_INSTANCE:
                    ctx.source.instance.update()
                    ctx.target.instance.update()
            if ctx.operation._operation_retry:
                raise ctx.operation._operation_retry
            return result
        return _process_wrapper(wrapper, arguments)
    else:
        def partial_wrapper(fn):
            return operation(fn, **arguments)
        return partial_wrapper


def workflow(func=None, system_wide=False, **arguments):
    """
    Decorate workflow functions with this decorator.
    Internally, if celery is installed, ``@workflow`` will also wrap
    the function with a ``@celery.task`` decorator

    The ``ctx`` injected to the function arguments is of type
    ``cloudify.workflows.workflow_context.CloudifyWorkflowContext`` or
    ``cloudify.workflows.workflow_context.CloudifySystemWideWorkflowContext``
    if ``system_wide`` flag is set to True.

    The ``ctx`` object can also be accessed by importing
    ``cloudify.workflows.ctx``

    ``system_wide`` flag turns this workflow into a system-wide workflow that
    is executed by the management worker and has access to an instance of
    ``cloudify.workflows.workflow_context.CloudifySystemWideWorkflowContext``
    as its context.

    Example::

        from cloudify.workflows import ctx

        @workflow
        def reinstall(**kwargs):
            pass
    """
    if system_wide:
        ctx_class = CloudifySystemWideWorkflowContext
    else:
        ctx_class = CloudifyWorkflowContext
    if func is not None:
        @wraps(func)
        def wrapper(*args, **kwargs):

            def is_ctx_class_instance(obj):
                return isinstance(obj, ctx_class)

            ctx = _find_context_arg(args, kwargs, is_ctx_class_instance)

            if not is_ctx_class_instance(ctx):
                ctx = ctx_class(ctx)

            kwargs['ctx'] = ctx

            if ctx.local:
                workflow_wrapper = _local_workflow
            else:
                workflow_wrapper = _remote_workflow

            return workflow_wrapper(ctx, func, args, kwargs)
        return _process_wrapper(wrapper, arguments)
    else:
        def partial_wrapper(fn):
            return workflow(fn, system_wide, **arguments)
        return partial_wrapper


class RequestSystemExit(SystemExit):
    pass


def _remote_workflow(ctx, func, args, kwargs):
    def update_execution_cancelled():
        update_execution_status(ctx.execution_id, Execution.CANCELLED)
        _send_workflow_cancelled_event(ctx)

    rest = get_rest_client()
    parent_queue, child_queue = (Queue.Queue(), Queue.Queue())
    try:
        amqp_client_utils.init_amqp_client()
        if rest.executions.get(ctx.execution_id).status in \
                (Execution.CANCELLING, Execution.FORCE_CANCELLING):
            # execution has been requested to be cancelled before it
            # was even started
            update_execution_cancelled()
            return api.EXECUTION_CANCELLED_RESULT

        update_execution_status(ctx.execution_id, Execution.STARTED)
        _send_workflow_started_event(ctx)

        # the actual execution of the workflow will run in another
        # thread - this wrapper is the entry point for that
        # thread, and takes care of forwarding the result or error
        # back to the parent thread
        def child_wrapper():
            try:
                ctx.internal.start_event_monitor()
                workflow_result = _execute_workflow_function(
                    ctx, func, args, kwargs)
                child_queue.put({'result': workflow_result})
            except api.ExecutionCancelled:
                child_queue.put({
                    'result': api.EXECUTION_CANCELLED_RESULT})
            except BaseException as workflow_ex:
                tb = StringIO()
                traceback.print_exc(file=tb)
                err = {
                    'type': type(workflow_ex).__name__,
                    'message': str(workflow_ex),
                    'traceback': tb.getvalue()
                }
                child_queue.put({'error': err})
            finally:
                ctx.internal.stop_event_monitor()

        api.queue = parent_queue

        # starting workflow execution on child thread
        # AMQPWrappedThread is used to provide it with its own amqp client
        t = AMQPWrappedThread(target=child_wrapper)
        t.start()

        # while the child thread is executing the workflow,
        # the parent thread is polling for 'cancel' requests while
        # also waiting for messages from the child thread
        has_sent_cancelling_action = False
        result = None
        execution = None
        while True:
            # check if child thread sent a message
            try:
                data = child_queue.get(timeout=5)
                if 'result' in data:
                    # child thread has terminated
                    result = data['result']
                    break
                else:
                    # error occurred in child thread
                    error = data['error']
                    raise exceptions.ProcessExecutionError(error['message'],
                                                           error['type'],
                                                           error['traceback'])
            except Queue.Empty:
                pass
            # check for 'cancel' requests
            execution = rest.executions.get(ctx.execution_id)
            if execution.status == Execution.FORCE_CANCELLING:
                result = api.EXECUTION_CANCELLED_RESULT
                break
            elif not has_sent_cancelling_action and \
                    execution.status == Execution.CANCELLING:
                # send a 'cancel' message to the child thread. It
                # is up to the workflow implementation to check for
                # this message and act accordingly (by stopping and
                # raising an api.ExecutionCancelled error, or by returning
                # the deprecated api.EXECUTION_CANCELLED_RESULT as result).
                # parent thread then goes back to polling for
                # messages from child process or possibly
                # 'force-cancelling' requests
                parent_queue.put({'action': 'cancel'})
                has_sent_cancelling_action = True

        # updating execution status and sending events according to
        # how the execution ended
        if result == api.EXECUTION_CANCELLED_RESULT:
            update_execution_cancelled()
            if execution and execution.status == Execution.FORCE_CANCELLING:
                # TODO: kill worker externally
                raise RequestSystemExit()
        else:
            update_execution_status(ctx.execution_id, Execution.TERMINATED)
            _send_workflow_succeeded_event(ctx)
        return result
    except RequestSystemExit:
        raise
    except BaseException as e:
        if isinstance(e, exceptions.ProcessExecutionError):
            error_traceback = e.traceback
        else:
            error = StringIO()
            traceback.print_exc(file=error)
            error_traceback = error.getvalue()
        update_execution_status(ctx.execution_id, Execution.FAILED,
                                error_traceback)
        _send_workflow_failed_event(ctx, e, error_traceback)
        raise
    finally:
        amqp_client_utils.close_amqp_client()


def _local_workflow(ctx, func, args, kwargs):
    try:
        _send_workflow_started_event(ctx)
        result = _execute_workflow_function(ctx, func, args, kwargs)
        _send_workflow_succeeded_event(ctx)
        return result
    except Exception, e:
        error = StringIO()
        traceback.print_exc(file=error)
        _send_workflow_failed_event(ctx, e, error.getvalue())
        raise


def _execute_workflow_function(ctx, func, args, kwargs):
    try:
        ctx.internal.start_local_tasks_processing()
        current_workflow_ctx.set(ctx, kwargs)
        result = func(*args, **kwargs)
        if not ctx.internal.graph_mode:
            tasks = list(ctx.internal.task_graph.tasks_iter())
            for workflow_task in tasks:
                workflow_task.async_result.get()
        return result
    finally:
        ctx.internal.stop_local_tasks_processing()
        current_workflow_ctx.clear()


def _send_workflow_started_event(ctx):
    ctx.internal.send_workflow_event(
        event_type='workflow_started',
        message="Starting '{0}' workflow execution".format(ctx.workflow_id))


def _send_workflow_succeeded_event(ctx):
    ctx.internal.send_workflow_event(
        event_type='workflow_succeeded',
        message="'{0}' workflow execution succeeded"
        .format(ctx.workflow_id))


def _send_workflow_failed_event(ctx, exception, error_traceback):
    ctx.internal.send_workflow_event(
        event_type='workflow_failed',
        message="'{0}' workflow execution failed: {1}"
        .format(ctx.workflow_id, str(exception)),
        args={'error': error_traceback})


def _send_workflow_cancelled_event(ctx):
    ctx.internal.send_workflow_event(
        event_type='workflow_cancelled',
        message="'{0}' workflow execution cancelled"
        .format(ctx.workflow_id))


def _process_wrapper(wrapper, arguments):
    result_wrapper = _task
    if arguments.get('force_not_celery') is True:
        result_wrapper = _stub_task
    return result_wrapper(wrapper)

task = operation
