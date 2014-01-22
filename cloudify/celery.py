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

__author__ = 'idanmo'

import sys
import traceback
import os

from celery import Celery
from celery.signals import after_setup_task_logger

from cloudify.logs import setup_logger

celery = Celery('cloudify.celery',
                broker='amqp://',
                backend='amqp://')

current_excepthook = sys.excepthook


def new_excepthook(exception_type, value, the_traceback):
    with open(os.path.expanduser('~/celery_error.out'), 'w') as f:
        f.write('Type: {0}\n'.format(exception_type))
        f.write('Value: {0}\n'.format(value))
        traceback.print_tb(the_traceback, file=f)
    current_excepthook(type, value, the_traceback)

sys.excepthook = new_excepthook


@after_setup_task_logger.connect
def setup_cloudify_logger(loglevel=None, **kwargs):
    setup_logger(loglevel, **kwargs)
