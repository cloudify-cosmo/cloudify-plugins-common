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
__author__ = 'dank'

import copy

import celery

from cloudify.manager import get_node_state, update_node_state
from cloudify.workflows.tasks import (RemoteWorkflowTask,
                                      LocalWorkflowTask,
                                      NOP)
from cloudify.logs import CloudifyWorkflowLoggingHandler, init_cloudify_logger

celery_client = celery.Celery(broker='amqp://', backend='amqp://')
celery_client.conf.update(CELERY_TASK_SERIALIZER='json')


class CloudifyWorkflowNode(object):

    def __init__(self, ctx, node):
        self.ctx = ctx
        self._node = node

    def set_state(self, state):
        def set_state_task():
            node_state = get_node_state(self.id)
            node_state.runtime_properties['state'] = state
            update_node_state(node_state)
            return node_state
        return LocalWorkflowTask(set_state_task, self.ctx, self)

    def get_state(self):
        def get_state_task():
            return get_node_state(self.id).runtime_properties.get('state')
        return LocalWorkflowTask(get_state_task, self.ctx, self)

    def send_event(self, event):
        def send_event_task():
            self.ctx.logger.info('Event[{}][{}]'.format(self.id, event))
        return LocalWorkflowTask(send_event_task, self.ctx, self)

    def execute_operation(self, operation, kwargs=None):
        kwargs = kwargs or {}
        node = self._node
        operations = node['operations']
        op_struct = operations.get(operation)
        if op_struct is None:
            return NOP
        plugin_name = op_struct['plugin']
        operation_mapping = op_struct['operation']
        operation_properties = op_struct.get('properties')
        task_queue = 'cloudify.management'
        if node['plugins'][plugin_name]['agent_plugin'] == 'true':
            task_queue = node['host_id']
        elif node['plugins'][plugin_name]['manager_plugin'] == 'true':
            task_queue = self.ctx.deployment_id
        task_name = '{0}.{1}'.format(plugin_name, operation_mapping)

        if operation_properties is None:
            operation_properties = self.properties
        else:
            operation_properties['cloudify_runtime'] = \
                self.properties.get('cloudify_runtime', {})

        task_kwargs = self._safe_update(operation_properties, kwargs)
        task_kwargs['__cloudify_id'] = self.id

        context_node_properties = copy.copy(operation_properties)
        context_capabilities = operation_properties.get('cloudify_runtime', {})
        context_node_properties.pop('__cloudify_id', None)
        context_node_properties.pop('cloudify_runtime', None)

        node_context = {
            'node_id': self.id,
            'node_name': self.name,
            'node_properties': context_node_properties,
            'plugin': plugin_name,
            'operation': operation,
            'capabilities': context_capabilities
        }

        return self.ctx.execute_task(task_queue, task_name,
                                     kwargs=task_kwargs,
                                     node_context=node_context)

    @staticmethod
    def _safe_update(dict1, dict2):
        result = copy.deepcopy(dict2)
        for key, value in dict1.items():
            if key == 'cloudify_runtime':
                if key not in result:
                    result[key] = {}
                result[key].update(value)
            elif key in result:
                raise RuntimeError('Target map already contains key: {0}'
                                   .format(key))
            else:
                result[key] = value
        return result

    @property
    def id(self):
        return self._node.get('id')

    @property
    def name(self):
        return self._node.get('name')

    @property
    def type(self):
        return self._node.get('type')

    @property
    def properties(self):
        return self._node.get('properties', {})

    @property
    def plugins_to_install(self):
        return self._node.get('plugins_to_install', [])


class CloudifyWorkflowContext(object):

    def __init__(self, ctx):
        self._context = ctx
        self._nodes = [CloudifyWorkflowNode(self, node) for
                       node in ctx['plan']['nodes']]
        self._logger = None

    @property
    def nodes(self):
        return self._nodes

    @property
    def deployment_id(self):
        return self._context.get('deployment_id')

    @property
    def blueprint_id(self):
        return self._context.get('blueprint_id')

    @property
    def execution_id(self):
        return self._context.get('execution_id')

    @property
    def workflow_id(self):
        return self._context.get('workflow_id')

    @property
    def logger(self):
        if self._logger is None:
            self._init_cloudify_logger()
        return self._logger

    def _init_cloudify_logger(self):
        logger_name = self.workflow_id if self.workflow_id is not None \
            else 'cloudify_workflow'
        init_cloudify_logger(self, CloudifyWorkflowLoggingHandler, logger_name)

    def send_event(self, event):
        pass

    def execute_task(self,
                     task_queue,
                     task_name,
                     kwargs=None,
                     node_context=None):
        kwargs = kwargs or {}
        cloudify_context = self._build_cloudify_context(
            task_queue,
            task_name,
            node_context)
        kwargs['__cloudify_context'] = cloudify_context

        task = celery.subtask(task_name,
                              kwargs=kwargs,
                              queue=task_queue,
                              immutable=True)

        return RemoteWorkflowTask(task, cloudify_context)

    def _build_cloudify_context(self,
                                task_queue,
                                task_name,
                                node_context):
        context = {
            '__cloudify_context': '0.3',
            'task_id': 'TODO',
            'task_name': task_name,
            'task_target': task_queue,
            'blueprint_id': self.blueprint_id,
            'deployment_id': self.deployment_id,
            'execution_id': self.execution_id,
            'workflow_id': self.workflow_id,
            'capabilities': None,
            'node_id': None,
            'node_name': None,
            'node_properties': None,
            'plugin': None,
            'operation': None
        }
        if node_context is not None:
            context.update(node_context)
        return context
