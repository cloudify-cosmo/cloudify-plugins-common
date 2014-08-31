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


import traceback

from multiprocessing import Process
from multiprocessing import Pipe
from StringIO import StringIO
from functools import wraps

from cloudify.context import CloudifyContext
from cloudify.workflows.workflow_context import CloudifyWorkflowContext
from cloudify.manager import update_execution_status, get_rest_client
from cloudify.logs import send_workflow_event
from cloudify.workflows.events import start_event_monitor
from cloudify.workflows import api
from cloudify_rest_client.executions import Execution
from cloudify.exceptions import ProcessExecutionError
from cloudify.state import current_ctx, current_workflow_ctx


try:
    from cloudify.celery import celery as _celery
    _task = _celery.task
except ImportError:
    _celery = None
    _task = lambda fn: fn


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
    return CloudifyContext.__name__ in obj.__class__.__name__


def _is_cloudify_workflow_context(obj):
    """
    Gets whether the provided obj is a CloudifyWorkflowContext instance.
    From some reason Python's isinstance returned False when it should
    have returned True.
    """
    return CloudifyWorkflowContext.__name__ in obj.__class__.__name__


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

    Example::

        @operations
        def start(ctx, **kwargs):
            pass
    """

    if func is not None:
        @_task
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = _find_context_arg(args, kwargs, _is_cloudify_context)
            if ctx is None:
                ctx = {}
            if not _is_cloudify_context(ctx):
                ctx = CloudifyContext(ctx)
                kwargs['ctx'] = ctx
            try:
                current_ctx.set(ctx, kwargs)
                result = func(*args, **kwargs)
            except BaseException:
                ctx.logger.error(
                    'Exception raised on operation [%s] invocation',
                    ctx.task_name, exc_info=True)
                raise
            finally:
                current_ctx.clear()
                ctx.update()
            return result
        return wrapper
    else:
        def partial_wrapper(fn):
            return operation(fn, **arguments)
        return partial_wrapper


def workflow(func=None, **arguments):
    """
    Decorate workflow functions with this decorator.
    Internally, if celery is installed, will also wrap the function
    with a ``@celery.task`` decorator

    The ``ctx`` injected to the function arguments is of type
    ``cloudify.workflows.workflow_context.CloudifyWorkflowContext``


    Example::

        @workflow
        def reinstall(ctx, **kwargs):
            pass
    """
    if func is not None:
        @_task
        @wraps(func)
        def wrapper(*args, **kwargs):

            ctx = _find_context_arg(args, kwargs,
                                    _is_cloudify_workflow_context)
            ctx = CloudifyWorkflowContext(ctx)
            kwargs['ctx'] = ctx

            if ctx.remote:
                workflow_wrapper = remote_workflow
            else:
                workflow_wrapper = local_workflow

            return workflow_wrapper(ctx, func, args, kwargs)

        return wrapper
    else:
        def partial_wrapper(fn):
            return workflow(fn, **arguments)

        return partial_wrapper


def remote_workflow(ctx, func, args, kwargs):
    def update_execution_cancelled():
        update_execution_status(ctx.execution_id, Execution.CANCELLED)
        send_workflow_event(
            ctx,
            event_type='workflow_cancelled',
            message="'{}' workflow execution cancelled"
                    .format(ctx.workflow_id))

    rest = get_rest_client()
    parent_conn, child_conn = Pipe()
    try:
        if rest.executions.get(ctx.execution_id).status in \
                (Execution.CANCELLING, Execution.FORCE_CANCELLING):
            # execution has been requested to be cancelled before it
            # was even started
            update_execution_cancelled()
            return api.EXECUTION_CANCELLED_RESULT

        update_execution_status(ctx.execution_id, Execution.STARTED)
        send_workflow_event(
            ctx,
            event_type='workflow_started',
            message="Starting '{}' workflow execution".format(ctx.workflow_id))

        # the actual execution of the workflow will run in another
        # process - this wrapper is the entry point for that
        # process, and takes care of forwarding the result or error
        # back to the parent process
        def child_wrapper():
            try:
                start_event_monitor(ctx)
                workflow_result = _execute_workflow_function(
                    ctx, func, args, kwargs)
                child_conn.send({'result': workflow_result})
            except api.ExecutionCancelled:
                child_conn.send({
                    'result': api.EXECUTION_CANCELLED_RESULT})
            except BaseException, workflow_ex:
                tb = StringIO()
                traceback.print_exc(file=tb)
                err = {
                    'type': type(workflow_ex).__name__,
                    'message': str(workflow_ex),
                    'traceback': tb.getvalue()
                }
                child_conn.send({'error': err})
            finally:
                child_conn.close()

        api.ctx = ctx
        api.pipe = child_conn

        # starting workflow execution on child process
        p = Process(target=child_wrapper)
        p.start()

        # while the child process is executing the workflow,
        # the parent process is polling for 'cancel' requests while
        # also waiting for messages from the child process
        has_sent_cancelling_action = False
        result = None
        while True:
            # check if child process sent a message
            if parent_conn.poll(5):
                data = parent_conn.recv()
                if 'result' in data:
                    # child process has terminated
                    result = data['result']
                    break
                else:
                    # error occurred in child process
                    error = data['error']
                    raise ProcessExecutionError(error['message'],
                                                error['type'],
                                                error['traceback'])
            # check for 'cancel' requests
            execution = rest.executions.get(ctx.execution_id)
            if execution.status == Execution.FORCE_CANCELLING:
                # terminate the child process immediately
                p.terminate()
                result = api.EXECUTION_CANCELLED_RESULT
                break
            elif not has_sent_cancelling_action and \
                    execution.status == Execution.CANCELLING:
                # send a 'cancel' message to the child process. It
                # is up to the workflow implementation to check for
                # this message and act accordingly (by stopping and
                # returning a api.EXECUTION_CANCELLED_RESULT result).
                # parent process then goes back to polling for
                # messages from child process or possibly
                # 'force-cancelling' requests
                parent_conn.send({'action': 'cancel'})
                has_sent_cancelling_action = True

        # updating execution status and sending events according to
        # how the execution ended
        if result == api.EXECUTION_CANCELLED_RESULT:
            update_execution_cancelled()
        else:
            update_execution_status(ctx.execution_id, Execution.TERMINATED)
            send_workflow_event(
                ctx, event_type='workflow_succeeded',
                message="'{}' workflow execution succeeded"
                .format(ctx.workflow_id))
        return result
    except BaseException, e:
        if isinstance(e, ProcessExecutionError):
            error_traceback = e.traceback
        else:
            error = StringIO()
            traceback.print_exc(file=error)
            error_traceback = error.getvalue()
        update_execution_status(ctx.execution_id, Execution.FAILED,
                                error_traceback)
        send_workflow_event(
            ctx,
            event_type='workflow_failed',
            message="'{}' workflow execution failed: {}"
                    .format(ctx.workflow_id, str(e)),
            args={'error': error_traceback})
        raise
    finally:
        parent_conn.close()
        child_conn.close()  # probably unneeded but cleanup anyway


def local_workflow(ctx, func, args, kwargs):
    return _execute_workflow_function(ctx, func, args, kwargs)


def _execute_workflow_function(ctx, func, args, kwargs):
    try:
        current_workflow_ctx.set(ctx, kwargs)
        result = func(*args, **kwargs)
        if not ctx.internal.graph_mode:
            tasks = list(ctx.internal.task_graph.tasks_iter())
            for workflow_task in tasks:
                workflow_task.async_result.get()
        return result
    finally:
        current_workflow_ctx.clear()


task = operation
