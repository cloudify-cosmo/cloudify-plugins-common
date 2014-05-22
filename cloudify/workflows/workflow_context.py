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

from cloudify.manager import get_node_instance, update_node_instance, \
    update_execution_status
from cloudify.workflows.tasks import (RemoteWorkflowTask,
                                      LocalWorkflowTask,
                                      NOP)
from cloudify.logs import (CloudifyWorkflowLoggingHandler,
                           CloudifyWorkflowNodeLoggingHandler,
                           init_cloudify_logger,
                           send_workflow_event,
                           send_workflow_node_event)


class CloudifyWorkflowRelationship(object):

    def __init__(self, ctx, node, relationship):
        self.ctx = ctx
        self.node = node
        self._relationship = relationship

    @property
    def target_id(self):
        return self._relationship.get('target_id')

    @property
    def target_node(self):
        return self.ctx.get_node(self.target_id)

    @property
    def source_operations(self):
        return self._relationship.get('source_operations', {})

    @property
    def target_operations(self):
        return self._relationship.get('target_operations', {})

    def execute_source_operation(self, operation, kwargs=None):
        return self.ctx._execute_operation(
            operation,
            node=self.node,
            related_node=self.target_node,
            operations=self.source_operations,
            kwargs=kwargs)

    def execute_target_operation(self, operation, kwargs=None):
        return self.ctx._execute_operation(
            operation,
            node=self.target_node,
            related_node=self.node,
            operations=self.target_operations,
            kwargs=kwargs)


class CloudifyWorkflowNode(object):

    def __init__(self, ctx, node):
        self.ctx = ctx
        self._node = node
        self._relationships = [
            CloudifyWorkflowRelationship(self.ctx, self, relationship) for
            relationship in node.get('relationships', [])]
        self._logger = None

    def set_state(self, state):
        def set_state_task():
            node_state = get_node_instance(self.id)
            node_state.state = state
            update_node_instance(node_state)
            return node_state
        return LocalWorkflowTask(set_state_task, self.ctx, self, info=state)

    def get_state(self):
        def get_state_task():
            return get_node_instance(self.id).state
        return LocalWorkflowTask(get_state_task, self.ctx, self)

    def send_event(self, event, additional_context=None):
        def send_event_task():
            send_workflow_node_event(ctx=self,
                                     event_type='workflow_node_event',
                                     message=event,
                                     additional_context=additional_context)
        return LocalWorkflowTask(send_event_task, self.ctx, self, info=event)

    def execute_operation(self, operation, kwargs=None):
        return self.ctx._execute_operation(operation=operation,
                                           node=self,
                                           operations=self.operations,
                                           kwargs=kwargs)

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

    @property
    def relationships(self):
        return self._relationships

    @property
    def operations(self):
        return self._node.get('operations', {})

    @property
    def logger(self):
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    def _init_cloudify_logger(self):
        logger_name = self.id if self.id is not None \
            else 'cloudify_workflow_node'
        handler = CloudifyWorkflowNodeLoggingHandler(self)
        return init_cloudify_logger(handler, logger_name)


class CloudifyWorkflowContext(object):

    def __init__(self, ctx):
        self._context = ctx
        self._nodes = {node['id']: CloudifyWorkflowNode(self, node) for
                       node in ctx['plan']['nodes']}
        self._logger = None

    @property
    def nodes(self):
        return self._nodes.itervalues()

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
            self._logger = self._init_cloudify_logger()
        return self._logger

    def _init_cloudify_logger(self):
        logger_name = self.workflow_id if self.workflow_id is not None \
            else 'cloudify_workflow'
        handler = CloudifyWorkflowLoggingHandler(self)
        return init_cloudify_logger(handler, logger_name)

    def send_event(self, event, event_type='workflow_stage',
                   args=None,
                   additional_context=None):
        def send_event_task():
            send_workflow_event(ctx=self,
                                event_type=event_type,
                                message=event,
                                args=args,
                                additional_context=additional_context)
        return LocalWorkflowTask(send_event_task, self, info=event)

    def get_node(self, node_id):
        return self._nodes.get(node_id)

    def _execute_operation(self, operation, node, operations,
                           related_node=None,
                           kwargs=None):
        kwargs = kwargs or {}
        raw_node = node._node
        op_struct = operations.get(operation)
        if op_struct is None:
            return NOP
        plugin_name = op_struct['plugin']
        operation_mapping = op_struct['operation']
        operation_properties = op_struct.get('properties', node.properties)
        task_queue = 'cloudify.management'
        if raw_node['plugins'][plugin_name]['agent_plugin'] == 'true':
            task_queue = raw_node['host_id']
        elif raw_node['plugins'][plugin_name]['manager_plugin'] == 'true':
            task_queue = self.deployment_id
        task_name = '{0}.{1}'.format(plugin_name, operation_mapping)

        node_context = {
            'node_id': node.id,
            'node_name': node.name,
            'node_properties': copy.copy(operation_properties),
            'plugin': plugin_name,
            'operation': operation,
            'relationships': [rel.target_id for rel in node.relationships]
        }
        if related_node is not None:
            node_context['related'] = {
                'node_id': related_node.id,
                'node_properties': copy.copy(related_node.properties)
            }

        return self.execute_task(task_queue, task_name,
                                 kwargs=kwargs,
                                 node_context=node_context)

    def update_execution_status(self, new_status):
        """
        Updates the execution status to new_status.
        Note that the workflow status gets automatically updated before and
        after its run (whether the run succeeded or failed)
        """
        def update_execution_status_task():
            update_execution_status(self.execution_id, new_status)
        return LocalWorkflowTask(update_execution_status_task,
                                 self, info=new_status)

    def execute_task(self,
                     task_queue,
                     task_name,
                     kwargs=None,
                     node_context=None):
        kwargs = kwargs or {}
        task_id = str(uuid.uuid4())
        cloudify_context = self._build_cloudify_context(
            task_id,
            task_queue,
            task_name,
            node_context)
        kwargs['__cloudify_context'] = cloudify_context

        task = celery.subtask(task_name,
                              kwargs=kwargs,
                              queue=task_queue,
                              immutable=True)

        return RemoteWorkflowTask(task, cloudify_context, task_id)

    def _build_cloudify_context(self,
                                task_id,
                                task_queue,
                                task_name,
                                node_context):
        node_context = node_context or {}
        context = {
            '__cloudify_context': '0.3',
            'task_id': task_id,
            'task_name': task_name,
            'task_target': task_queue,
            'blueprint_id': self.blueprint_id,
            'deployment_id': self.deployment_id,
            'execution_id': self.execution_id,
            'workflow_id': self.workflow_id,
        }
        context.update(node_context)
        return context


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
