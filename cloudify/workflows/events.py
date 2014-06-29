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


import threading

from cloudify.logs import send_remote_task_event
from cloudify.celery import celery as app
from cloudify.workflows import tasks as tasks_api


TASK_TO_FILTER = ['worker_installer.tasks.restart']


class Monitor(object):
    """Monitor with handlers for different celery events"""

    def __init__(self, tasks_graph):
        """
        :param tasks_graph: The task graph. Used to extract tasks based on the
                            events task id.
        """
        self.tasks_graph = tasks_graph

    def task_sent(self, event):
        pass

    def task_received(self, event):
        pass

    def task_started(self, event):
        self._handle(tasks_api.TASK_STARTED, event)

    def task_succeeded(self, event):
        self._handle(tasks_api.TASK_SUCCEEDED, event)

    def task_failed(self, event):
        self._handle(tasks_api.TASK_FAILED, event)

    def task_revoked(self, event):
        pass

    def task_retried(self, event):
        pass

    def _handle(self, state, event):
        task_id = event['uuid']
        task = self.tasks_graph.get_task(task_id)
        if task is not None:
            send_task_event(state, task, event)
            task.set_state(state)

    def capture(self):
        with app.connection() as connection:
            receive = app.events.Receiver(connection, handlers={
                'task-sent': self.task_sent,
                'task-received': self.task_received,
                'task-started': self.task_started,
                'task-succeeded': self.task_succeeded,
                'task-failed': self.task_failed,
                'task-revoked': self.task_revoked,
                'task-retried': self.task_retried
            })
            receive.capture(limit=None, timeout=None, wakeup=True)


def send_task_event(state, task, event=None):
    """
    Send a task event to RabbitMQ

    :param state: the task state (valid: ['sending', 'started', 'succeeded',
                  'failed'])
    :param task: a WorkflowTask instance to send the event for
    :param event: the celery monitor event (for states ['started', 'succeed',
                  'failed'])
    """

    if task.name in TASK_TO_FILTER:
        return

    if state != tasks_api.TASK_SENDING and event is None:
        raise RuntimeError('missing required event parameter')

    if state == tasks_api.TASK_SENDING:
        message = "Sending task '{}'".format(task.name)
        event_type = 'sending_task'
    elif state == tasks_api.TASK_STARTED:
        message = "Task started '{}'".format(task.name)
        event_type = 'task_started'
    elif state == tasks_api.TASK_SUCCEEDED:
        result = str(event.get('result'))
        suffix = ' ({})'.format(result) if result != str(None) else ''
        message = "Task succeeded '{}{}'".format(task.name, suffix)
        event_type = 'task_succeeded'
    elif state == tasks_api.TASK_FAILED:
        message = "Task failed '{}' -> {}".format(task.name,
                                                  event.get('exception'))
        event_type = 'task_failed'
        task.error = event.get('exception')
    else:
        raise RuntimeError('unhandled event type: {}'.format(state))

    if task.current_retries > 0 or state == tasks_api.TASK_FAILED:
        attempt = '[attempt {}{}]'.format(
            task.current_retries+1,
            '/{}'.format(task.total_retries + 1)
            if task.total_retries >= 0
            else '')
        message = '{} {}'.format(message, attempt)

    send_remote_task_event(remote_task=task,
                           event_type=event_type,
                           message=message)


def start_event_monitor(workflow_context):
    """
    Start an event monitor in its own thread for handling tasks
    defined in the task dependency graph

    :param workflow_context: The workflow context
    """
    monitor = Monitor(workflow_context.internal.task_graph)
    thread = threading.Thread(target=monitor.capture)
    thread.daemon = True
    thread.start()
