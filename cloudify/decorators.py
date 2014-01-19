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

from functools import wraps
from manager import get_node_state
from manager import update_node_state

CLOUDIFY_ID_PROPERTY = '__cloudify_id'
CLOUDIFY_NODE_STATE_PROPERTY = 'node_state'

# initialized in celery.py
operation = None


def inject_argument(arg_name, arg_value, method, args, kwargs=None):
    """Injects argument and its value to the provided args, if arg_name
    was not found in argss_names a named arg will be injected to kwargs.

    Args:
        arg_name: The argument name to inject.
        arg_value: The argument value to inject.
        method: The method to argument is to be injected for.
        args: Invocation arguments.
        kwargs: Invocation kwargs (optional).

    Returns:
        An (*args, **kwargs) tuple to be used for invoking method.
    """
    method_arg_names = method.func_code.co_varnames
    func_defaults = method.func_defaults

    print "method_args:", method_arg_names
    print "actual     :", args
    print "defaults   :", func_defaults
    print "kwargs     :", kwargs

    arg_index = None
    try:
        arg_index = method_arg_names.index(arg_name)
    except ValueError:
        pass
    if arg_index is None:
        if kwargs is not None:
            kwargs[arg_name] = arg_value
    else:
        if len(args) != len(method_arg_names) and func_defaults is not None:
            method_arg_names = method_arg_names[
                :len(method_arg_names) - len(func_defaults)]
        if arg_name in method_arg_names:
            args = list(args)
            if len(method_arg_names) == len(args):
                args[arg_index] = arg_value
            else:
                args.insert(arg_index, arg_value)
                if arg_name in kwargs:
                    del kwargs[arg_name]
        else:
            if kwargs is not None:
                kwargs[arg_name] = arg_value
    return args, kwargs


def with_node_state(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        node_id = _get_node_id_from_args(method, args, kwargs)
        if node_id is None:
            raise RuntimeError(
                'Node id property [{0}] not present in '
                'method invocation arguments'.format(CLOUDIFY_ID_PROPERTY))
        node_state = get_node_state(node_id)
        args, kwargs = inject_argument('node_state', node_state, method, args,
                                       kwargs)
        result = method(*args, **kwargs)
        update_node_state(node_state)
        return result
    return wrapper


def with_logger(task):
    @wraps(task)
    def task_wrapper(*args, **kwargs):
        print "## task started ##"
        try:
            import logging
            logger = logging.getLogger(task.func_name)

            print "arguments: {0}".format(task.func_code.co_varnames)
            print "values   : {0}".format(args)

            arg_names = task.func_code.co_varnames
            index = None
            try:
                index = arg_names.index('logger')
            except ValueError:
                pass
            if index is not None:
                if len(args) != len(arg_names):
                    args_list = list(args)
                    args_list.insert(index, logger)
                    args = args_list

            print "updated:"
            print "arguments: {0}".format(task.func_code.co_varnames)
            print "values   : {0}".format(args)


            result = task(*args, **kwargs)

            print "task result is:", result
            return result
        finally:
            print "## task ended ##"

    return task_wrapper


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

