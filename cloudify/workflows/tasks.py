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

import sys
import time
import uuid
import Queue

from cloudify.celery import celery as celery_client
from cloudify.exceptions import NonRecoverableError, RecoverableError

from cloudify.workflows import api

INFINITE_TOTAL_RETRIES = -1
DEFAULT_TOTAL_RETRIES = INFINITE_TOTAL_RETRIES
DEFAULT_RETRY_INTERVAL = 30


TASK_PENDING = 'pending'
TASK_SENDING = 'sending'
TASK_SENT = 'sent'
TASK_STARTED = 'started'
TASK_SUCCEEDED = 'succeeded'
TASK_FAILED = 'failed'


def retry_failure_handler(task):
    """Basic on_success/on_failure handler that always returns retry"""
    return HandlerResult.retry()


class WorkflowTask(object):
    """A base class for workflow tasks"""

    def __init__(self,
                 task_id=None,
                 info=None,
                 on_success=None,
                 on_failure=None,
                 total_retries=DEFAULT_TOTAL_RETRIES,
                 retry_interval=DEFAULT_RETRY_INTERVAL,
                 workflow_context=None):
        """
        :param task_id: The id of this task (generated if none is provided)
        :param info: A short description of this task (for logging)
        :param on_success: A handler called when the task's execution
                           terminates successfully.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.cont()]
                           to indicate whether this task should be re-executed.
        :param on_failure: A handler called when the task's execution
                           fails.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.ignore(),
                            HandlerResult.fail()]
                           to indicate whether this task should be re-executed,
                           cause the engine to terminate workflow execution
                           immediately or simply ignore this task failure and
                           move on.
        :param total_retries: Maximum retry attempt for this task, in case
                              the handlers return a retry attempt.
        :param retry_interval: Number of seconds to wait between retries
        """
        self.id = task_id or str(uuid.uuid4())
        self._state = TASK_PENDING
        self.async_result = None
        self.on_success = on_success
        self.on_failure = on_failure
        self.info = info
        self.error = None
        self.total_retries = total_retries
        self.retry_interval = retry_interval
        self.terminated = Queue.Queue(maxsize=1)
        self.workflow_context = workflow_context

        self.current_retries = 0
        # timestamp for which the task should not be executed
        # by the task graph before reached, overridden by the task
        # graph during retries
        self.execute_after = time.time()

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
        if state in [TASK_SUCCEEDED, TASK_FAILED]:
            self.terminated.put_nowait(True)

    def wait_for_terminated(self, timeout=None):
        self.terminated.get(timeout=timeout)

    def handle_task_terminated(self):
        if self.get_state() == TASK_FAILED:
            handler_result = self._handle_task_failed()
        else:
            handler_result = self._handle_task_succeeded()

        if handler_result.action == HandlerResult.HANDLER_RETRY:
            if any([self.total_retries == INFINITE_TOTAL_RETRIES,
                    self.current_retries < self.total_retries,
                    handler_result.ignore_total_retries]):
                if handler_result.retry_after is None:
                    handler_result.retry_after = self.retry_interval
                new_task = self.duplicate()
                new_task.current_retries += 1
                new_task.execute_after = time.time() +\
                    handler_result.retry_after
                handler_result.retried_task = new_task
            else:
                handler_result.action = HandlerResult.HANDLER_FAIL
        return handler_result

    def _handle_task_succeeded(self):
        """Call handler for task success"""
        if self.on_success:
            return self.on_success(self)
        else:
            return HandlerResult.cont()

    def _handle_task_failed(self):
        """Call handler for task failure"""
        if self.on_failure:
            handler_result = self.on_failure(self)
        else:
            handler_result = HandlerResult.retry()
        if handler_result.action == HandlerResult.HANDLER_RETRY and \
                self.is_remote():
            try:
                exception = self.async_result.async_result.result
            except:
                exception = None
            if isinstance(exception, NonRecoverableError):
                handler_result = HandlerResult.fail()
            elif isinstance(exception, RecoverableError):
                handler_result.retry_after = exception.retry_after
        return handler_result

    def __str__(self):
        suffix = self.info if self.info is not None else ''
        return '{}({})'.format(self.name, suffix)

    def duplicate(self):
        """
        :return: A new instance of this task with a new task id
        """

        raise NotImplementedError('Implemented by subclasses')


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
                 on_failure=retry_failure_handler,
                 total_retries=DEFAULT_TOTAL_RETRIES,
                 retry_interval=DEFAULT_RETRY_INTERVAL,
                 workflow_context=None):
        """
        :param task: The celery task
        :param cloudify_context: the cloudify context dict
        :param task_id: The id of this task (generated if none is provided)
        :param info: A short description of this task (for logging)
        :param on_success: A handler called when the task's execution
                           terminates successfully.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.cont()]
                           to indicate whether this task should be re-executed.
        :param on_failure: A handler called when the task's execution
                           fails.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.ignore(),
                            HandlerResult.fail()]
                           to indicate whether this task should be re-executed,
                           cause the engine to terminate workflow execution
                           immediately or simply ignore this task failure and
                           move on.
        :param total_retries: Maximum retry attempt for this task, in case
                              the handlers return a retry attempt.
        :param retry_interval: Number of seconds to wait between retries
        :param workflow_context: the CloudifyWorkflowContext instance
        """
        super(RemoteWorkflowTask, self).__init__(
            task_id,
            info=info,
            on_success=on_success,
            on_failure=on_failure,
            total_retries=total_retries,
            retry_interval=retry_interval,
            workflow_context=workflow_context)
        self.task = task
        self.cloudify_context = cloudify_context

    def apply_async(self):
        """
        Call the underlying celery tasks apply_async. Verify the task
        is registered and send an event before doing so.

        :return: a RemoteWorkflowTaskResult instance wrapping the
                 celery async result
        """

        self._verify_task_registered()

        # here to avoid cyclic dependencies
        from events import send_task_event
        send_task_event(TASK_SENDING, self)

        async_result = self.task.apply_async(task_id=self.id)
        self.set_state(TASK_SENT)
        self.async_result = RemoteWorkflowTaskResult(self, async_result)
        return self.async_result

    @staticmethod
    def is_local():
        return False

    def duplicate(self):
        dup = RemoteWorkflowTask(task=self.task,
                                 cloudify_context=self.cloudify_context,
                                 info=self.info,
                                 on_success=self.on_success,
                                 on_failure=self.on_failure,
                                 total_retries=self.total_retries,
                                 retry_interval=self.retry_interval,
                                 workflow_context=self.workflow_context)
        dup.cloudify_context['task_id'] = dup.id
        dup.current_retries = self.current_retries
        return dup

    @property
    def name(self):
        """The task name"""
        return self.cloudify_context['task_name']

    @property
    def target(self):
        """The task target (queue name)"""
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
    """A WorkflowTask wrapping a local callable"""

    def __init__(self,
                 local_task,
                 workflow_context,
                 node=None,
                 info=None,
                 on_success=None,
                 on_failure=retry_failure_handler,
                 total_retries=DEFAULT_TOTAL_RETRIES,
                 retry_interval=DEFAULT_RETRY_INTERVAL,
                 kwargs=None,
                 task_id=None):
        """
        :param local_task: A callable
        :param workflow_context: the CloudifyWorkflowContext instance
        :param node: The CloudifyWorkflowNode instance (if in node context)
        :param info: A short description of this task (for logging)
        :param on_success: A handler called when the task's execution
                           terminates successfully.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.cont()]
                           to indicate whether this task should be re-executed.
        :param on_failure: A handler called when the task's execution
                           fails.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.ignore(),
                            HandlerResult.fail()]
                           to indicate whether this task should be re-executed,
                           cause the engine to terminate workflow execution
                           immediately or simply ignore this task failure and
                           move on.
        :param total_retries: Maximum retry attempt for this task, in case
                              the handlers return a retry attempt.
        :param retry_interval: Number of seconds to wait between retries
        :param kwargs: Local task keyword arguments
        """
        super(LocalWorkflowTask, self).__init__(
            info=info,
            on_success=on_success,
            on_failure=on_failure,
            total_retries=total_retries,
            retry_interval=retry_interval,
            task_id=task_id,
            workflow_context=workflow_context)
        self.local_task = local_task
        self.node = node
        self.kwargs = kwargs

    def apply_async(self):
        """
        Execute the task in the current thread
        :return: A wrapper for the task result
        """

        self.set_state(TASK_SENT)
        try:
            if self.kwargs:
                result = self.local_task(**self.kwargs)
            else:
                result = self.local_task()
            self.set_state(TASK_SUCCEEDED)
            self.async_result = LocalWorkflowTaskResult(self, result)
        except:
            exc_type, exception, tb = sys.exc_info()
            self.set_state(TASK_FAILED)
            self.async_result = LocalWorkflowTaskResult(self, result=None,
                                                        error=(exception, tb))

        return self.async_result

    @staticmethod
    def is_local():
        return True

    def duplicate(self):
        dup = LocalWorkflowTask(local_task=self.local_task,
                                workflow_context=self.workflow_context,
                                node=self.node,
                                info=self.info,
                                on_success=self.on_success,
                                on_failure=self.on_failure,
                                total_retries=self.total_retries,
                                retry_interval=self.retry_interval)
        dup.current_retries = self.current_retries
        return dup

    @property
    def name(self):
        """The task name"""
        return self.local_task.__name__


# NOP tasks class
class NOPLocalWorkflowTask(LocalWorkflowTask):

    def __init__(self):
        super(NOPLocalWorkflowTask, self).__init__(lambda: None, None)

    @property
    def name(self):
        """The task name"""
        return 'NOP'


class WorkflowTaskResult(object):
    """A base wrapper for workflow task results"""

    def __init__(self, task):
        self.task = task

    def _process(self, retry_on_failure):
        if self.task.workflow_context.internal.graph_mode:
            return self._get()
        task_graph = self.task.workflow_context.internal.task_graph
        while True:
            self._wait_for_task_terminated()
            handler_result = self.task.handle_task_terminated()
            task_graph.remove_task(self.task)
            try:
                result = self._get()
                if handler_result.action != HandlerResult.HANDLER_RETRY:
                    return result
            except:
                if (not retry_on_failure or
                        handler_result.action == HandlerResult.HANDLER_FAIL):
                    raise
            self._sleep(handler_result.retry_after)
            self.task = handler_result.retried_task
            task_graph.add_task(self.task)
            self._check_execution_cancelled()
            self.task.apply_async()
            self._refresh_state()

    @staticmethod
    def _check_execution_cancelled():
        if api.has_cancel_request():
            raise api.ExecutionCancelled()

    def _wait_for_task_terminated(self):
        while True:
            self._check_execution_cancelled()
            try:
                self.task.wait_for_terminated(timeout=1)
            except Queue.Empty:
                continue

    def _sleep(self, seconds):
        end_wait = time.time() + seconds
        while time.time() < end_wait:
            self._check_execution_cancelled()
            sleep_time = 1 if seconds > 1 else seconds
            time.sleep(sleep_time)

    def get(self, retry_on_failure=True):
        """
        Get the task result.
        Will block until the task execution ends.

        :return: The task result
        """
        return self._process(retry_on_failure)

    def _get(self):
        raise NotImplementedError('Implemented by subclasses')

    def _refresh_state(self):
        raise NotImplementedError('Implemented by subclasses')


class RemoteWorkflowTaskResult(WorkflowTaskResult):
    """A wrapper for celery's AsyncResult"""

    def __init__(self, task, async_result):
        super(RemoteWorkflowTaskResult, self).__init__(task)
        self.async_result = async_result

    def _get(self):
        return self.async_result.get()

    def _refresh_state(self):
        self.async_result = self.task.async_result.async_result


class LocalWorkflowTaskResult(WorkflowTaskResult):
    """A wrapper for local workflow task results"""

    def __init__(self, task, result, error=None):
        """
        :param task: The LocalWorkflowTask instance
        :param result: The result if the task finished successfully
        :param error: a tuple (exception, traceback) if the task failed
        """
        super(LocalWorkflowTaskResult, self).__init__(task)
        self.result = result
        self.error = error

    def _get(self):
        if self.error is not None:
            exception, traceback = self.error
            raise exception, None, traceback
        return self.result

    def _refresh_state(self):
        self.result = self.task.async_result.result
        self.error = self.task.async_result.error


class HandlerResult(object):

    HANDLER_RETRY = 'handler_retry'
    HANDLER_FAIL = 'handler_fail'
    HANDLER_IGNORE = 'handler_ignore'
    HANDLER_CONTINUE = 'handler_continue'

    def __init__(self,
                 action,
                 ignore_total_retries=False,
                 retry_after=None):
        self.action = action
        self.ignore_total_retries = ignore_total_retries
        self.retry_after = retry_after

        # this field is filled by handle_terminated_task() below after
        # duplicating the task and updating the relevant task fields
        self.retried_task = None

    @classmethod
    def retry(cls, ignore_total_retries=False, retry_after=None):
        return HandlerResult(cls.HANDLER_RETRY,
                             ignore_total_retries=ignore_total_retries,
                             retry_after=retry_after)

    @classmethod
    def fail(cls):
        return HandlerResult(cls.HANDLER_FAIL)

    @classmethod
    def cont(cls):
        return HandlerResult(cls.HANDLER_CONTINUE)

    @classmethod
    def ignore(cls):
        return HandlerResult(cls.HANDLER_IGNORE)
