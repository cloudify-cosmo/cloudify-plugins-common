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
    arg_index = None
    try:
        arg_index = method_arg_names.index(arg_name)
    except ValueError:
        pass
    if arg_index is None:
        if kwargs is not None:
            kwargs[arg_name] = arg_value
    else:
        if len(method_arg_names) == len(args):
            args[arg_index] = arg_value
        else:
            args_as_list = list(args)
            args_as_list.insert(arg_index, arg_value)
            args = args_as_list
    return args, kwargs


class DeploymentNode(object):
    """Represents a deployment node state.
    An instance of this class contains runtime information retrieved
    from Cloudify's runtime storage.
    Its API allows to set and get properties of the node's state,
     generate an updates dict to be used when requesting to save changes
     back to the storage (in an optimistic locking manner).
    """
    def __init__(self, node_id, runtime_properties=None):
        self.id = node_id
        self._runtime_properties = runtime_properties
        if runtime_properties is not None:
            self._runtime_properties = {k: [v, None] for k, v in runtime_properties.iteritems()}

    def get(self, key):
        return self._runtime_properties[key][0]

    def put(self, key, value):
        if self._runtime_properties is None:
            self._runtime_properties = {}
        if key in self._runtime_properties:
            values = self._runtime_properties[key]
            if len(values) == 1:
                self._runtime_properties[key] = [value, values[0]]
            else:
                values[0] = value
        else:
            self._runtime_properties[key] = [value]

    def get_updated_properties(self):
        if self._runtime_properties is None:
            return {}
        return {k: v for k, v in self._runtime_properties.iteritems() if len(v) == 1 or v[1] is not None}



def with_node_state(method):
    @wraps(method)
    def wrapper(method):






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


CLOUDIFY_ID_PROPERTY = '__cloudify_id'
CLOUDIFY_NODE_STATE_PROPERTY = 'node_state'


def _get_base_uri():
    return "http://localhost:{0}".format(os.environ['MANAGER_REST_PORT'])




# TODO runtime-model: use manager-rest-client
def get_node_state(node_id):
    response = requests.get("{0}/nodes/{1}".format(_get_base_uri(), node_id))
    if response.status_code != 200:
        raise RuntimeError(
            "Error getting node from cloudify runtime for node id {0} [code={1}]".format(node_id, response.status_code))
    return DeploymentNode(node_id, response.json()['runtimeInfo'])


# TODO runtime-model: use manager-rest-client
def update_node_state(node_state):
    updated_properties = node_state.get_updated_properties()
    if len(updated_properties) == 0:
        return None
    import json
    response = requests.patch("{0}/nodes/{1}".format(_get_base_uri(), node_state.id),
                              headers={'Content-Type': 'application/json'},
                              data=json.dumps(updated_properties))
    if response.status_code != 200:
        raise RuntimeError(
            "Error getting node from cloudify runtime for node id {0} [code={1}]".format(node_state.id,
                                                                                         response.status_code))
    return response.json()


def inject_node_state(task):
    # Necessary for keeping the returned method name as the provided task's name.
    from functools import wraps

    @wraps(task)
    def task_wrapper(*args, **kwargs):
        node_id = _get_cloudify_id_from_method_arguments(task, args, kwargs)
        state = None
        if node_id is not None:
            try:
                state = get_node_state(node_id)
            except Exception:
                # TODO: log exception
                pass
            kwargs[CLOUDIFY_NODE_STATE_PROPERTY] = state
        task(*args, **kwargs)
        if state is not None:
            try:
                update_node_state(state)
            except Exception as e:
                # TODO: log exception
                pass
    return task_wrapper


def _get_cloudify_id_from_method_arguments(method, args, kwargs):
    if CLOUDIFY_ID_PROPERTY in kwargs:
        return kwargs[CLOUDIFY_ID_PROPERTY]
    try:
        arg_index = method.func_code.co_varnames.index(CLOUDIFY_ID_PROPERTY)
        return args[arg_index]
    except ValueError:
        pass
    return None

