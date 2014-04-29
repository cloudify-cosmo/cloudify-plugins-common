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

from celery import Celery
celery = Celery(broker='amqp://', backend='amqp://')
celery.conf.update(CELERY_TASK_SERIALIZER='json')


class CloudifyWorkflowNode(object):

    def __init__(self, ctx, node):
        self.ctx = ctx
        self._node = node

    def set_state(self, state):
        pass

    def execute_operation(self, operation, **kwargs):
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
        celery_prefix_task_queue = 'celery.{0}'.format(task_queue)
        task_name = '{0}.{1}'.format(plugin_name, operation_mapping)

        if operation_properties is None:
            operation_properties = self.properties
        elif 'cloudify_runtime' in self.properties:
            operation_properties['cloudify_runtime'] = \
                self.properties['cloudify_runtime']

        task_kwargs = self._safe_update(operation_properties, kwargs)

        self.ctx.execute_task(task_queue,
                              'plugin_installer.tasks.verify_plugin',
                              worker_id=celery_prefix_task_queue,
                              plugin_name=plugin_name,
                              operation=operation_mapping,
                              throw_on_failure=True)

        return self.ctx.execute_task(task_queue, task_name,
                                     **task_kwargs)

    @staticmethod
    def _safe_update(dict1, dict2):
        result = copy.copy(dict2)
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
        self._nodes = [CloudifyWorkflowNode(ctx, node) for
                       node in ctx['plan']['nodes']]

    @property
    def nodes(self):
        return self.nodes

    @property
    def deployment_id(self):
        return self._context.get('deployment_id')

    def send_event(self, event):
        pass

    def execute_task(self, task_queue, task_name, node=None, **kwargs):
        task_id = uuid.uuid4()

        celery.send_task(task_name,
                         task_id=task_id,
                         kwargs=kwargs,
                         queues=[task_queue])
