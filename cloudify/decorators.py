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
from functools import wraps
from manager import get_node_state
from manager import update_node_state

CLOUDIFY_ID_PROPERTY = '__cloudify_id'
CLOUDIFY_NODE_STATE_PROPERTY = 'node_state'


# overridden in celery.py
def operation(method):
    return method


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


def with_node_state(func=None, **arguments):
    """Injects node state for the node in context.
    Args:
        arg: argument name to inject the node state as where 'node_state'
            is the default.
    """
    if func is not None:
        @wraps(func)
        def wrapper(*args, **kwargs):
            node_id = _get_node_id_from_args(func, args, kwargs)
            if node_id is None:
                raise RuntimeError(
                    'Node id property [{0}] not present in '
                    'method invocation arguments'.format(CLOUDIFY_ID_PROPERTY))
            node_state = get_node_state(node_id)
            node_state_arg = arguments['arg'] if 'arg' in arguments\
                else 'node_state'
            kwargs = _inject_argument(node_state_arg, node_state, kwargs)
            result = func(*args, **kwargs)
            update_node_state(node_state)
            return result
        return wrapper
    else:
        def partial_wrapper(fn):
            return with_node_state(fn, **arguments)
        return partial_wrapper


def with_logger(func=None, **arguments):
    """Injects a Cloudify operation logger.
    Args:
        arg: argument name to inject the logger as where 'logger' is the
            default.
    """
    if func is not None:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger_arg = arguments['arg'] if 'arg' in arguments \
                else 'logger'
            logger = logging.getLogger('cloudify')
            kwargs = _inject_argument(logger_arg, logger, kwargs)
            result = func(*args, **kwargs)
            return result
        return wrapper
    else:
        def partial_wrapper(fn):
            return with_logger(fn, **arguments)
        return partial_wrapper


def _get_node_id_from_args(method, args, kwargs):
    """Gets node id for method invocation.
    Args:
        method: Invoked method.
        args: Invocation *args.
        kwargs: Invocation **kwargs.
    Returns:
        Extracted node id or None if not found.
    """
    if CLOUDIFY_ID_PROPERTY in kwargs:
        return kwargs[CLOUDIFY_ID_PROPERTY]
    try:
        arg_names = method.func_code.co_varnames
        arg_index = None
        for i in range(len(arg_names)):
            if arg_names[i].endswith(CLOUDIFY_ID_PROPERTY):
                arg_index = i
                break
        return args[arg_index]
    except ValueError:
        pass
    return None
