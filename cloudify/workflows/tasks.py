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


import uuid

from cloudify.celery import celery as celery_client


TASK_PENDING = 'pending'
TASK_SENDING = 'sending'
TASK_SENT = 'sent'
TASK_STARTED = 'started'
TASK_SUCCEEDED = 'succeeded'
TASK_FAILED = 'failed'


class WorkflowTask(object):
    """A base class for workflow tasks"""

    def __init__(self,
                 task_id=None,
                 info=None,
                 on_success=None,
                 on_failure=None):
        """
        :param task_id: The id of this task (generated if none is provided)
        :param info: A short description of this task (for logging)
        :param on_success: A handler called when the task's execution
                           terminates successfully
        :param on_failure: Not implemented yet (currently, when a task fails,
                           it fails the entire workflow)
        """
        self.id = task_id or str(uuid.uuid4())
        self._state = TASK_PENDING
        self.async_result = None
        self.on_success = on_success
        self.on_failure = on_failure
        self.info = info
        self.error = None

    def is_remote(self):
        """
        :return: Is this a remote task
        """
        return not self.is_local()

    def get_state(self):
        """
        Get the task state

        :return: The task state [pending, sending, sent, started, succeeded,
                                 failed]
        """
        return self._state

    def set_state(self, state):
        """
        Set the task state

        :param state: The state to set [pending, sending, sent, started,
                                           succeeded, failed]
        """

        if state not in [TASK_PENDING, TASK_SENDING, TASK_SENT, TASK_STARTED,
                         TASK_SUCCEEDED, TASK_FAILED]:
            raise RuntimeError('Illegal state set on task: {} '
                               '[task={}]'.format(state, str(self)))

        self._state = state

    def handle_task_terminated(self):
        """Call handler based on task terminated state"""
        if self._state == TASK_SUCCEEDED and self.on_success:
            return self.on_success(self)
        elif self._state == TASK_FAILED and self.on_failure:
            return self.on_failure(self)

    def __str__(self):
        suffix = self.info if self.info is not None else ''
        return '{}({})'.format(self.name, suffix)


class RemoteWorkflowTask(WorkflowTask):
    """A WorkflowTask wrapping a celery based task"""

    # cache for registered tasks queries to celery workers
    cache = {}

    def __init__(self,
                 task,
                 cloudify_context,
                 task_id=None,
                 info=None,
                 on_success=None,
                 on_failure=None):
        """
        :param task: The celery task
        :param cloudify_context: the cloudify context dict
        :param task_id: The id of this task (generated if none is provided)
        :param info: A short description of this task (for logging)
        :param on_success: A handler called when the task's execution
                           terminates successfully
        :param on_failure: Not implemented yet (currently, when a task fails,
                           it fails the entire workflow)
        """
        super(RemoteWorkflowTask, self).__init__(task_id,
                                                 info=info,
                                                 on_success=on_success,
                                                 on_failure=on_failure)
        self.task = task
        self.cloudify_context = cloudify_context

    def apply_async(self):
        """
        

        :return: a RemoteWorkflowTaskResult instance wrapping the
                 celery async result
        """

        self._verify_task_registered()

        # here to avoid cyclic dependencies
        from events import send_task_event
        send_task_event(TASK_SENDING, self)

        async_result = self.task.apply_async(task_id=self.id)
        self.set_state(TASK_SENT)
        self.async_result = RemoteWorkflowTaskResult(async_result)
        return self.async_result

    @staticmethod
    def is_local():
        return False

    def duplicate(self):
        dup = RemoteWorkflowTask(self.task,
                                 self.cloudify_context,
                                 info=self.info,
                                 on_success=self.on_success,
                                 on_failure=self.on_failure)
        dup.cloudify_context['task_id'] = dup.id
        return dup

    @property
    def name(self):
        return self.cloudify_context['task_name']

    @property
    def target(self):
        return self.cloudify_context['task_target']

    def _verify_task_registered(self):
        cache = RemoteWorkflowTask.cache
        registered = cache.get(self.target, set())
        if self.name not in registered:
            registered = self._get_registered()
            cache[self.target] = registered

        if self.name not in registered:
            raise RuntimeError('Missing task: {} in worker celery.{} \n'
                               'Registered tasks are: {}'
                               .format(self.name, self.target, registered))

    def _get_registered(self):
        worker_name = 'celery.{}'.format(self.target)
        inspect = celery_client.control.inspect(destination=[worker_name])
        registered = inspect.registered() or {}
        result = registered.get(worker_name, set())
        return set(result)


class LocalWorkflowTask(WorkflowTask):

    def __init__(self, local_task, workflow_context,
                 node=None,
                 info=None,
                 on_success=None,
                 on_failure=None):
        """
        :param local_task: A callable
        :param workflow_context: the CloudifyWorkflowContext instance
        :param node: The CloudifyWorkflowNode instance (if in node context)
        """
        super(LocalWorkflowTask, self).__init__(info=info,
                                                on_success=on_success,
                                                on_failure=on_failure)
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
        return LocalWorkflowTask(self.local_task,
                                 self.workflow_context,
                                 self.node,
                                 info=self.info,
                                 on_success=self.on_success,
                                 on_failure=self.on_failure)

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
