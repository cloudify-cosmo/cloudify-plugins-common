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

__author__ = 'idanmo'


import logging
import datetime
import pika
import json
from cloudify.utils import get_manager_ip


class CloudifyPluginLoggingHandler(logging.Handler):
    """
    A Handler class for writing log messages to RabbitMQ.
    """
    amqp_client = None

    def __init__(self, ctx):
        logging.Handler.__init__(self)
        self._ctx = ctx

    def flush(self):
        pass

    def emit(self, record):
        message = self.format(record)
        timestamp = str(datetime.datetime.now())
        log = {
            'type': 'cloudify_log',
            'message_code': None,
            'timestamp': timestamp,
            'context': {
                'task_id': self._ctx.task_id,
                'plugin': self._ctx.plugin,
                'blueprint_id': self._ctx.blueprint_id,
                'task_target': self._ctx.task_target,
                'node_name': self._ctx.node_name,
                'workflow_id': self._ctx.workflow_id,
                'node_id': self._ctx.node_id,
                'task_name': self._ctx.task_name,
                'operation': self._ctx.operation,
                'deployment_id': self._ctx.deployment_id,
                'execution_id': self._ctx.execution_id
            },
            'logger': record.name,
            'level': record.levelname.lower(),
            'message': {
                'text': message
            }
        }
        try:
            self.publish_log(log)
        except BaseException as e:
            error_logger = logging.getLogger('cloudify_celery')
            error_logger.warning('Error publishing log to RabbitMQ ['
                                 'message={0}, log={1}]'
                                 .format(e.message, json.dumps(log)))

    def publish_log(self, log):
        if self.amqp_client is None:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=get_manager_ip()))
            channel = connection.channel()
            channel.queue_declare(queue='cloudify-logs',
                                  auto_delete=True,
                                  durable=True,
                                  exclusive=False)
            self.amqp_client = channel

        self.amqp_client.basic_publish(exchange='',
                                       routing_key='cloudify-logs',
                                       body=json.dumps(log))
