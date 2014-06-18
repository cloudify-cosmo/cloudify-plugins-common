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


from StringIO import StringIO
from functools import wraps
import traceback


from cloudify.celery import celery
from cloudify.context import CloudifyContext
from cloudify.workflows.workflow_context import CloudifyWorkflowContext
from cloudify.manager import update_execution_status
from cloudify.logs import send_workflow_event


CLOUDIFY_ID_PROPERTY = '__cloudify_id'
CLOUDIFY_NODE_STATE_PROPERTY = 'node_state'
CLOUDIFY_CONTEXT_PROPERTY_KEY = '__cloudify_context'
CLOUDIFY_CONTEXT_IDENTIFIER = '__cloudify_context'


def _inject_argument(arg_name, arg_value, kwargs=None):
    """Inject argument to kwargs.
    This is currently done by simply putting the key and value in kwargs
    since Celery's task decorator maps **kwargs to relevant arguments
    and when tasks are executed from workflow all arguments are passed
    in **kwargs.
    Args:
        arg_name: The argument name to inject.
        arg_value: The argument value to inject.
        args: Invocation arguments.
        kwargs: Invocation kwargs (optional).

    Returns:
        An (*args, **kwargs) tuple to be used for invoking method.
    """
    kwargs[arg_name] = arg_value
    return kwargs


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
    return kwargs[CLOUDIFY_CONTEXT_PROPERTY_KEY]\
        if CLOUDIFY_CONTEXT_PROPERTY_KEY in kwargs else None


def operation(func=None, **arguments):
    if func is not None:
        @celery.task
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = _find_context_arg(args, kwargs,
                                    _is_cloudify_context)
            if ctx is None:
                ctx = {}
            if not _is_cloudify_context(ctx):
                ctx = CloudifyContext(ctx)
                kwargs = _inject_argument('ctx', ctx, kwargs)
            try:
                result = func(*args, **kwargs)
            except BaseException:
                ctx.logger.error(
                    'Exception raised on operation [%s] invocation',
                    ctx.task_name, exc_info=True)
                raise
            ctx.update()
            return result
        return wrapper
    else:
        def partial_wrapper(fn):
            return operation(fn, **arguments)
        return partial_wrapper


def workflow(func=None, **arguments):
    if func is not None:
        @celery.task
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = _find_context_arg(args, kwargs,
                                    _is_cloudify_workflow_context)
            if ctx is None:
                ctx = {}
            if not _is_cloudify_workflow_context(ctx):
                ctx = CloudifyWorkflowContext(ctx)
                kwargs = _inject_argument('ctx', ctx, kwargs)
            try:
                send_workflow_event(ctx,
                                    event_type='workflow_started',
                                    message="Starting '{}' workflow execution"
                                            .format(ctx.workflow_id))
                update_execution_status(ctx.execution_id, 'started')
                result = func(*args, **kwargs)
                send_workflow_event(
                    ctx, event_type='workflow_succeeded',
                    message="'{}' workflow execution succeeded"
                            .format(ctx.workflow_id))
                update_execution_status(ctx.execution_id, 'terminated')
            except BaseException, e:
                error = StringIO()
                traceback.print_exc(file=error)
                send_workflow_event(
                    ctx,
                    event_type='workflow_failed',
                    message="'{}' workflow execution failed: {}"
                            .format(ctx.workflow_id, str(e)),
                    args={'error': error.getvalue()})
                update_execution_status(ctx.execution_id, 'failed',
                                        error.getvalue())
                raise
            return result
        return wrapper
    else:
        def partial_wrapper(fn):
            return workflow(fn, **arguments)
        return partial_wrapper

task = operation
