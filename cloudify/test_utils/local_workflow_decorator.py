#########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import sys
import shutil
import tempfile
from os import path, listdir, makedirs

from functools import wraps
from attrdict import AttrDict

from cloudify.workflows import local

PLUGIN_YAML_NAME = 'plugin.yaml'

IGNORED_LOCAL_WORKFLOW_MODULES = (
    'cloudify_agent.operations',
    'cloudify_agent.installer.operations',

    # maintained for backward compatibily with < 3.3 blueprints
    'worker_installer.tasks',
    'plugin_installer.tasks',
    'windows_agent_installer.tasks',
    'windows_plugin_installer.tasks'
)


def _find_plugin_yaml(original_path):
    """
    Tries to find the plugin.yaml file automatically (by traversing up the
    directory tree).
    :param original_path: The path to start the search from
    :return: The absolute path of the plugin.yaml file (if found, o.w.
    raises an Error)
    """
    running_path = original_path
    while PLUGIN_YAML_NAME not in listdir(running_path):
        level_up_path = path.realpath(path.join(running_path, '..'))
        if level_up_path == running_path:
            msg = 'Traversing up the folder tree from {0}, failed to find {1}.'
            raise IOError(msg.format(original_path, PLUGIN_YAML_NAME))
        else:
            running_path = level_up_path

    return path.abspath(path.join(running_path, PLUGIN_YAML_NAME))


def _assure_path_exists(dest_path):
    """
    Creates a the destination path (if not exists)
    :param dest_path:
    :return:
    """
    dir_path = path.dirname(dest_path)
    if not path.exists(dir_path):
        makedirs(dir_path)


def _copy_resources(test_root_path, resources):
    """
    Copies a list of resources to the dest_path

    :param test_root_path: the default destination path
    :param resources: a list of resources to be copied - can contain source
    path only, or a tuple of source and destination path.
    :return: None
    """
    for resource in resources:
        if isinstance(resource, tuple):
            resource_source_path, relative_dest_path = resource
            relative_dest_path = path.join(relative_dest_path,
                                           path.basename(resource_source_path))
        else:
            resource_source_path = resource
            relative_dest_path = path.basename(resource_source_path)

        resource_source_path = path.abspath(resource_source_path)
        resource_dest_path = path.join(test_root_path, relative_dest_path)
        _assure_path_exists(path.dirname(resource_dest_path))
        shutil.copyfile(resource_source_path, resource_dest_path)


# TODO: env_as_keyword not tested yet (as the whole project is in python2 only)
class WorkflowTestDecorator(object):
    def __init__(self,
                 blueprint_path,
                 env_as_keyword=False,
                 plugin_auto_copy=False,
                 resources_to_copy=None,
                 prefix=None,
                 init_args=None):
        """
        Sets the required parameters for future env init. passes the
        environment to the cfy_local argument.

        :param env_as_keyword: specified if the env will be passed as a
                           positional or keyword argument (keyword possible
                           only on py34) (Defaults to False).
        :param blueprint_path: The relative path to the blueprint
        :param plugin_auto_copy: Tries to find and copy plugin.yaml (optional)
        :param resources_to_copy: Paths to resources to copy (optional)
        :param prefix: prefix for the resources (optional)
        """
        # blueprint to run
        self.blueprint_path = blueprint_path
        self.temp_blueprint_path = None

        # Plugin path and name
        self.resources_to_copy = resources_to_copy if resources_to_copy else []

        self.copy_plugin = plugin_auto_copy
        if self.copy_plugin:
            self.plugin_yaml_filename = PLUGIN_YAML_NAME

        # Set prefix for resources
        self.prefix = prefix
        self.temp_dir = None

        self.env_as_keyword = env_as_keyword

        # set init args
        if init_args:
            self.init_args = init_args
            if 'ignored_modules' not in init_args:
                self.init_args['ignored_modules'] = \
                    IGNORED_LOCAL_WORKFLOW_MODULES
        else:
            self.init_args = {
                'ignored_modules': IGNORED_LOCAL_WORKFLOW_MODULES
            }

    def set_up(self, local_file_path, test_method_name):
        """
        Sets up the enviroment variables needed for this test.

        :param local_file_path: the path of the test file.
        :param test_method_name: the name of the test method.
        :return: The test env which is a wrapped Environment.
        """
        if not self.prefix:
            self.prefix = test_method_name

        # Creating temp dir
        self.temp_dir = tempfile.mkdtemp(self.prefix)

        # Adding blueprint to the resources to copy
        self.resources_to_copy.append(self.blueprint_path)

        # Finding and adding the plugin
        if self.copy_plugin:
            self.resources_to_copy.append(
                _find_plugin_yaml(path.dirname(local_file_path)))

        # Copying resources
        _copy_resources(self.temp_dir, self.resources_to_copy)

        # Updating the test_method_name (if not manually set)
        if self.init_args and not self.init_args.get('name', False):
            self.init_args['name'] = test_method_name

        # Init env with supplied args
        temp_blueprint_path = path.join(self.temp_dir,
                                        path.basename(self.blueprint_path))
        test_env = local.init_env(temp_blueprint_path, **self.init_args)

        # Creating a vars field to hold all the test based variables.
        test_env.vars = AttrDict()

        return test_env

    def tear_down(self):
        """
        Deletes the allocated temp dir
        :return: None
        """
        if path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def __call__(self, test):
        @wraps(test)
        def wrapped_test(func, *args, **kwargs):
            """
            The wrapper function itself.

            :param func: the function of which this test has been called from
            :param args:
            :param kwargs:
            :return:
            """
            test_env = self.set_up(
                sys.modules[func.__class__.__module__].__file__,
                test.__name__
            )
            try:
                if self.env_as_keyword and sys.version >= 3:
                    test(func, cfy_local=test_env, *args, **kwargs)
                else:
                    test(func, test_env, *args, **kwargs)
            finally:
                self.tear_down()

        return wrapped_test
