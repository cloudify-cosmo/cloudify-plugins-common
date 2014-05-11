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
import uuid

import celery

from cloudify.manager import (get_node_state as _get_node_state,
                              update_node_state as _update_node_state)

celery_client = celery.Celery(broker='amqp://', backend='amqp://')
celery_client.conf.update(CELERY_TASK_SERIALIZER='json')


@celery_client.task
def set_node_state(node_id, state):
    node_state = _get_node_state(node_id)
    node_state.runtime_properties['state'] = state
    _update_node_state(node_state)
    return state


@celery_client.task
def get_node_state(node_id):
    return _get_node_state(node_id).runtime_properties.get('state')


class CloudifyWorkflowNode(object):

    def __init__(self, ctx, node):
        self.ctx = ctx
        self._node = node

    def set_state(self, state, async_result=False, return_task=False):
        task = set_node_state.subtask(
            kwargs={'node_id': self.id, 'state': state},
            queue='cloudify.management',
            immutable=True
        )
        if return_task:
            return task
        result = task.apply_async()
        if async_result:
            return result
        return result.get()

    def get_state(self, async_result=False, return_task=False):
        task = get_node_state.subtask(
            kwargs={'node_id': self.id},
            queue='cloudify.management',
            immutable=True
        )
        if return_task:
            return task
        result = task.apply_async()
        if async_result:
            return result
        return result.get()

    def send_event(self, event):
        pass

    def execute_operation(self, operation, kwargs=None, async_result=False,
                          return_task=False):
        kwargs = kwargs or {}
        node = self._node
        operations = node['operations']
        op_struct = operations.get(operation)
        if op_struct is None:
            # nop
            return
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
                                     node_context=node_context,
                                     async_result=async_result,
                                     return_task=return_task)

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

    def send_event(self, event):
        pass

    def execute_task(self,
                     task_queue,
                     task_name,
                     kwargs=None,
                     node_context=None,
                     async_result=False,
                     return_task=False):
        task_id = str(uuid.uuid4())
        kwargs = kwargs or {}
        kwargs['__cloudify_context'] = self._build_cloudify_context(
            task_id,
            task_queue,
            task_name,
            node_context)

        task = celery.subtask(task_name,
                              kwargs=kwargs,
                              queue=task_queue,
                              task_id=task_id,
                              immutable=True)
        if return_task:
            return task
        result = task.apply_async()
        if async_result:
            return result
        return result.get()

    def _build_cloudify_context(self,
                                task_id,
                                task_queue,
                                task_name,
                                node_context):
        context = {
            '__cloudify_context': '0.3',
            'task_id': task_id,
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
