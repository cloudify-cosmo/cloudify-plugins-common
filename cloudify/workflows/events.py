import threading


from cloudify.logs import send_remote_task_event
from cloudify.celery import celery as app
from cloudify.workflows import tasks as tasks_api


TASK_TO_FILTER = ['worker_installer.tasks.restart']


class Monitor(object):

    def __init__(self, tasks_graph):
        self.tasks_graph = tasks_graph
        self.ctx = tasks_graph.ctx

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
        message = "Task succeeded '{} ({})'".format(task.name,
                                                    event.get('result'))
        event_type = 'task_succeeded'
    elif state == tasks_api.TASK_FAILED:
        message = "Task failed '{}' -> {}".format(task.name,
                                                  event.get('exception'))
        event_type = 'task_failed'
        task.error = event.get('exception')
    else:
        raise RuntimeError('unhandled event type: {}'.format(state))

    send_remote_task_event(remote_task=task,
                           event_type=event_type,
                           message=message)


def start_event_monitor(tasks_graph):
    monitor = Monitor(tasks_graph)
    thread = threading.Thread(target=monitor.capture)
    thread.daemon = True
    thread.start()
