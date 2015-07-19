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

#########################################
# maintain backwards compatibility for < 3.3
#########################################
from cloudify_agent.app import app as celery  # NOQA
"""
This module is not intended to be used as standalone.
On celery worker installation this file will be copied to the
application root directory.
"""
# TODO: move to more appropriate place
TASK_STATE_PENDING = 'PENDING'
TASK_STATE_STARTED = 'STARTED'
TASK_STATE_SUCCESS = 'SUCCESS'
TASK_STATE_RETRY = 'RETRY'
TASK_STATE_FAILURE = 'FAILURE'
