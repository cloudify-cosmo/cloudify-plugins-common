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


import sys
import logging


from cloudify import manager
from cloudify import logs
from cloudify.logs import CloudifyPluginLoggingHandler


class Endpoint(object):

    def __init__(self, ctx):
        self.ctx = ctx

    def get_node_instance(self, node_instance_id):
        raise NotImplementedError('Implemented by subclasses')

    def update_node_instance(self, node_instance):
        raise NotImplementedError('Implemented by subclasses')

    def get_blueprint_resource(self, blueprint_id, resource_path):
        raise NotImplementedError('Implemented by subclasses')

    def download_blueprint_resource(self,
                                    blueprint_id,
                                    resource_path,
                                    logger,
                                    target_path=None):
        raise NotImplementedError('Implemented by subclasses')

    def get_provider_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_bootstrap_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_host_node_instance_ip(self,
                                  host_id,
                                  properties=None,
                                  runtime_properties=None):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def logging_handler(self):
        raise NotImplementedError('Implemented by subclasses')

    def send_plugin_event(self,
                          message=None,
                          args=None,
                          additional_context=None):
        raise NotImplementedError('Implemented by subclasses')


class ManagerEndpoint(Endpoint):

    def __init__(self, ctx):
        super(ManagerEndpoint, self).__init__(ctx)

    def get_node_instance(self, node_instance_id):
        return manager.get_node_instance(node_instance_id)

    def update_node_instance(self, node_instance):
        return manager.update_node_instance(node_instance)

    def get_blueprint_resource(self, blueprint_id, resource_path):
        return manager.get_blueprint_resource(blueprint_id, resource_path)

    def download_blueprint_resource(self,
                                    blueprint_id,
                                    resource_path,
                                    logger,
                                    target_path=None):
        return manager.download_blueprint_resource(blueprint_id,
                                                   resource_path,
                                                   logger,
                                                   target_path)

    def get_provider_context(self):
        return manager.get_provider_context()

    def get_bootstrap_context(self):
        return manager.get_bootstrap_context()

    def get_host_node_instance_ip(self,
                                  host_id,
                                  properties=None,
                                  runtime_properties=None):
        return manager.get_host_node_instance_ip(host_id,
                                                 properties,
                                                 runtime_properties)

    @property
    def logging_handler(self):
        return CloudifyPluginLoggingHandler

    def send_plugin_event(self,
                          message=None,
                          args=None,
                          additional_context=None):
        logs.send_plugin_event(self.ctx, message, args, additional_context)


class LocalEndpoint(Endpoint):

    def __init__(self, ctx, storage):
        super(LocalEndpoint, self).__init__(ctx)
        self.storage = storage

    def get_node_instance(self, node_instance_id):
        instance = self.storage.get_node_instance(node_instance_id)
        return manager.NodeInstance(
            node_instance_id,
            runtime_properties=instance.runtime_properties,
            state=instance.state,
            version=instance.version,
            host_id=instance.host_id)

    def update_node_instance(self, node_instance):
        return self.storage.update_node_instance(
            node_instance.id,
            runtime_properties=node_instance.runtime_properties,
            state=node_instance.state,
            version=node_instance.version)

    def get_blueprint_resource(self, blueprint_id, resource_path):
        return self.storage.get_resource(resource_path)

    def download_blueprint_resource(self,
                                    blueprint_id,
                                    resource_path,
                                    logger,
                                    target_path=None):
        return self.storage.download_resource(resource_path,
                                              target_path)

    def get_provider_context(self):
        # TODO
        return {}

    def get_bootstrap_context(self):
        # TODO
        return {}

    def get_host_node_instance_ip(self,
                                  host_id,
                                  properties=None,
                                  runtime_properties=None):
        # TODO
        return '127.0.0.1'

    @property
    def logging_handler(self):
        def handler(ctx):
            return logging.StreamHandler(sys.stdout)
        return handler

    def send_plugin_event(self,
                          message=None,
                          args=None,
                          additional_context=None):
        self.ctx.logger.info('[{}] {} [args={}, additional_context={}]'
                             .format(self.ctx.node_id,
                                     message,
                                     args or [],
                                     additional_context or {}))
