from __future__ import absolute_import
import sys
import traceback
import os
import json
import logging

from celery import Celery
from celery.signals import after_setup_task_logger

from cosmo.events import send_log_event
from cosmo import includes
import cosmo


celery = Celery('cosmo.celery', include=includes)

old_excepthook = sys.excepthook


def new_excepthook(type, value, the_traceback):
    with open(os.path.expanduser('~/celery_error.out'), 'w') as f:
        print "caught celery error!!!!!!!"
        f.write('Type: {0}\n'.format(type))
        f.write('Value: {0}\n'.format(value))
        traceback.print_tb(the_traceback, file=f)
    old_excepthook(type, value, the_traceback)

sys.excepthook = new_excepthook


class RiemannLoggingHandler(logging.Handler):
    """
    A Handler class for writing log messages to riemann.
    """
    def __init__(self):
        logging.Handler.__init__(self)

    def flush(self):
        pass

    def emit(self, record):
        message = self.format(record)
        log_record = {
            "name": record.name,
            "level": record.levelname,
            "message": message
        }
        try:
            send_log_event(log_record)
        except BaseException:
            pass

@after_setup_task_logger.connect
def setup_logger(loglevel=None, **kwargs):
    logger = logging.getLogger("cosmo")
    if not logger.handlers:
        handler = RiemannLoggingHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(loglevel)
        logger.propagate = True


def get_cosmo_properties():
    file_path = os.path.join(os.path.dirname(cosmo.__file__), 'cosmo.txt')
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.loads(f.read())

if __name__ == '__main__':
    celery.start()
