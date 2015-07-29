########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

"""
This module is not intended to be used as standalone.
On celery worker installation this file will be copied to the
application root directory.
"""

from __future__ import absolute_import


import logging
import logging.handlers
import sys
import traceback
import os
from os import path

from celery import Celery, signals

from cloudify.constants import CELERY_WORK_DIR_PATH_KEY


TASK_STATE_PENDING = 'PENDING'
TASK_STATE_STARTED = 'STARTED'
TASK_STATE_SUCCESS = 'SUCCESS'
TASK_STATE_RETRY = 'RETRY'
TASK_STATE_FAILURE = 'FAILURE'

DEFAULT_AMQP_URI = 'amqp://cloudify:c10udify@'

LOGFILE_SIZE_BYTES = 5 * 1024 * 1024
LOGFILE_BACKUP_COUNT = 5


celery_work_folder = os.environ.get(CELERY_WORK_DIR_PATH_KEY)

if celery_work_folder:
    @signals.setup_logging.connect
    def setup_logging_handler(loglevel, logfile, format, **kwargs):
        logger = logging.getLogger()
        if os.name == 'nt':
            logfile = logfile.format(os.getpid())
        handler = logging.handlers.RotatingFileHandler(
            logfile,
            maxBytes=LOGFILE_SIZE_BYTES,
            backupCount=LOGFILE_BACKUP_COUNT)
        handler.setFormatter(logging.Formatter(fmt=format))
        handler.setLevel(loglevel)
        logger.handlers = []
        logger.addHandler(handler)
        logger.setLevel(loglevel)


celery = Celery('cloudify.celery',
                broker=os.environ.get('BROKER_URL', DEFAULT_AMQP_URI),
                backend=os.environ.get('BROKER_URL', DEFAULT_AMQP_URI))
celery.conf.update(CELERY_TASK_RESULT_EXPIRES=600)

if celery_work_folder:
    current_excepthook = sys.excepthook

    def new_excepthook(exception_type, value, the_traceback):
        if not path.exists(celery_work_folder):
            os.makedirs(celery_work_folder)
        error_dump_path = path.join(celery_work_folder, 'celery_error.out')
        with open(error_dump_path, 'w') as f:
            f.write('Type: {0}\n'.format(exception_type))
            f.write('Value: {0}\n'.format(value))
            traceback.print_tb(the_traceback, file=f)
        current_excepthook(exception_type, value, the_traceback)

    sys.excepthook = new_excepthook
