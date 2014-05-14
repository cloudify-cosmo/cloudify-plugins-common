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


import uuid

TASK_PENDING = 'pending'
TASK_SENT = 'sent'
TASK_STARTED = 'started'
TASK_RECEIVED = 'received'
TASK_SUCCEEDED = 'succeeded'
TASK_FAILED = 'failed'
# TASK_REVOKED = 'revoked'
# TASK_RETRIED = 'retried'


class WorkflowTask(object):

    def __init__(self):
        self.id = str(uuid.uuid4())
        self._state = TASK_PENDING
        self.async_result = None
        self.on_success = None
        self.on_failure = None

    def is_remote(self):
        return not self.is_local()

    def get_state(self):
        return self._state

    def set_state(self, state):
        self._state = state

    def handle_task_terminated(self):
        if self._state == TASK_SUCCEEDED and self.on_success:
            return self.on_success(self)
        elif self._state == TASK_FAILED and self.on_failure:
            return self.on_failure(self)


class RemoteWorkflowTask(WorkflowTask):

    def __init__(self, task, cloudify_context):
        """
        :param task: The celery (sub)task
        :param cloudify_context: the cloudify_context dict argument
        """
        super(RemoteWorkflowTask, self).__init__()
        self.task = task
        self.cloudify_context = cloudify_context

    def apply_async(self):
        self.set_state(TASK_SENT)
        async_result = self.task.apply_async(task_id=self.id)
        self.async_result = RemoteWorkflowTaskResult(async_result)
        return self.async_result

    @staticmethod
    def is_local():
        return False

    def duplicate(self):
        return RemoteWorkflowTask(self.task, self.cloudify_context)

    @property
    def name(self):
        return self.cloudify_context['task_name']


class LocalWorkflowTask(WorkflowTask):

    def __init__(self, local_task, workflow_context, node=None):
        """
        :param local_task: A callable
        :param workflow_context: the CloudifyWorkflowContext instance
        :param node: The CloudifyWorkflowNode instance (if in node context)
        """
        super(LocalWorkflowTask, self).__init__()
        self.local_task = local_task
        self.workflow_context = workflow_context
        self.node = node

    def apply_async(self):
        self.set_state(TASK_SENT)
        try:
            result = self.local_task()
            self.set_state(TASK_SUCCEEDED)
            self.async_result = LocalWorkflowTaskResult(result)
            return self.async_result
        except:
            self.set_state(TASK_FAILED)
            raise

    @staticmethod
    def is_local():
        return True

    def duplicate(self):
        return LocalWorkflowTask(self.local_task, self.workflow_context,
                                 self.node)

    @property
    def name(self):
        return self.local_task.__name__

NOP = LocalWorkflowTask(lambda: None, None, None)


class RemoteWorkflowTaskResult(object):

    def __init__(self, async_result):
        self.async_result = async_result

    def get(self):
        return self.async_result.get()


class LocalWorkflowTaskResult(object):

    def __init__(self, result):
        self.result = result

    def get(self):
        return self.result


