#########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import shutil
import tempfile
from os import path, listdir
from functools import wraps

from cloudify.workflows import local

PLUGIN_NAME = 'plugin.yaml'


def _find_plugin_yaml(original_path):
    running_path = original_path
    while PLUGIN_NAME not in listdir(running_path):
        level_up_path = path.realpath(path.join(running_path, '..'))
        if level_up_path == running_path:
            msg = 'Traversing up the folder tree from {0}, failed to find {1}.'
            raise IOError(msg.format(original_path, PLUGIN_NAME))
        else:
            running_path = level_up_path

    return path.join(running_path, PLUGIN_NAME)


class set_testing_env(object):
    def __init__(self,
                 source_file_path,
                 blueprint_relative_path,
                 copy_plugin=False,
                 auto_storage=False,
                 prefix=None,
                 init_args=None,
                 env_args=None):
        """
        Sets the required parameters for future env init

        :param source_file_path: The path to the current test py file
        :param blueprint_relative_path: The relative path to the blueprint
        :param copy_plugin: Specify whether copy plugin or not
        :param auto_storage: When true creates (and later destroy) a temporary
                             storage
        :param prefix: prefix for the resources (optional)
        :param init_args: args for the env init (optional)
        :param env_args: args which would be added to the env (optional) - Note
                         you can pass a function which would be expanded
                         (useful for passing constructor to an object)
        :return: local workflow decorator
        """
        # blueprint to run
        self.blueprint_filename = path.basename(blueprint_relative_path)
        self.blueprint_path = path.abspath(
            path.join(path.dirname(source_file_path),
                      blueprint_relative_path))

        # Plugin path and name
        self.copy_plugin = copy_plugin
        if self.copy_plugin:
            self.plugin_yaml_filename = PLUGIN_NAME
            self.plugin_yaml_path = _find_plugin_yaml(
                path.dirname(source_file_path))

        # Set prefix for resources
        self.prefix = prefix if prefix else "{}-unit-tests-".format(
            path.splitext(path.basename(source_file_path))[0])

        # Create a temp dirs
        self.temp_test_dir = None
        self.temp_storage_dir = None

        # set init args
        self.init_args = init_args

        # set env args
        self.env_args = env_args

        self.auto_storage = auto_storage

    def set_up(self, test_method_name):

        class TestEnv:
            def __init__(self):
                pass

        test_env = TestEnv()

        # Updating the test_method_name (if not manually set)
        if self.init_args and not self.init_args.get('name', False):
            self.init_args['name'] = test_method_name

        # Creating temp dir
        self.temp_test_dir = tempfile.mkdtemp(self.prefix)

        # Copying blueprint to temp dir
        temp_blueprint_path = path.join(self.temp_test_dir,
                                        self.blueprint_filename)
        shutil.copyfile(self.blueprint_path, temp_blueprint_path)

        # Copying plugin to temp dir (if needed)
        if self.copy_plugin:
            temp_plugin_yaml_path = \
                path.join(self.temp_test_dir, self.plugin_yaml_filename)
            shutil.copyfile(self.plugin_yaml_path, temp_plugin_yaml_path)

        # Creating storage if needed
        if self.auto_storage:
            self.temp_storage_dir = tempfile.mkdtemp()
        if self.auto_storage:
            if self.init_args:
                self.init_args['storage'] = \
                    local.FileStorage(self.temp_storage_dir)
            else:
                self.init_args = {'storage':
                                  local.FileStorage(self.temp_storage_dir)}

        # Init env with supplied args
        if self.init_args:
            test_env.env = local.init_env(temp_blueprint_path,
                                          **self.init_args)
        else:
            test_env.env = local.init_env(temp_blueprint_path)

        # Pushing custom env args
        if self.env_args:
            for key in self.env_args:
                if hasattr(self.env_args[key], '__call__'):
                    setattr(test_env, key, self.env_args[key]())
                else:
                    setattr(test_env, key, self.env_args[key])

        return test_env

    def tear_down(self):
        if self.auto_storage:
            shutil.rmtree(self.temp_storage_dir)
        if path.exists(self.temp_test_dir):
            shutil.rmtree(self.temp_test_dir)

    def __call__(self, test):
        @wraps(test)
        def wrapped_test(func, *args, **kwargs):
            test_env = self.set_up(
                str(func)[:str(func).index('(') - 1])
            try:
                test(func, test_env, *args, **kwargs)
            finally:
                self.tear_down()

        return wrapped_test
