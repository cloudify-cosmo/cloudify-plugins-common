import threading


from cloudify.celery import celery as app
from cloudify.workflows import tasks as tasks_api


class Monitor(object):

    def __init__(self, tasks_graph):
        self.tasks_graph = tasks_graph
        self.ctx = tasks_graph.ctx

    def task_sent(self, event):
        pass

    def task_received(self, event):
        self._update_task_state(tasks_api.TASK_RECEIVED, event)

    def task_started(self, event):
        self._update_task_state(tasks_api.TASK_STARTED, event)

    def task_succeeded(self, event):
        self._update_task_state(tasks_api.TASK_SUCCEEDED, event)

    def task_failed(self, event):
        self._update_task_state(tasks_api.TASK_FAILED, event)

    def task_revoked(self, event):
        pass

    def task_retried(self, event):
        pass

    def _update_task_state(self, state, event):
        task_id = event['uuid']
        task = self.tasks_graph.get_task(task_id)
        if task is not None:
            self.ctx.logger.info('task[{}] state[{}]'.format(task.name, state))
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


def start_event_monitor(tasks_graph):
    monitor = Monitor(tasks_graph)
    thread = threading.Thread(target=monitor.capture)
    thread.start()
