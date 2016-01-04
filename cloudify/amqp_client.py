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


import json
import logging
from threading import current_thread

import pika

from cloudify import broker_config
from cloudify.exceptions import ClosedAMQPClientException
from cloudify.utils import (
    get_manager_ip,
    internal,
)


logger = logging.getLogger(__name__)


class AMQPClient(object):

    EVENTS_QUEUE_NAME = 'cloudify-events'
    LOGS_QUEUE_NAME = 'cloudify-logs'

    def __init__(self,
                 amqp_user='guest',
                 amqp_pass='guest',
                 amqp_host=None,
                 ssl_enabled=False,
                 ssl_cert_path=''):
        if amqp_host is None:
            amqp_host = get_manager_ip()

        self.events_queue = None
        self.logs_queue = None
        self._is_closed = False

        credentials = pika.credentials.PlainCredentials(
            username=amqp_user,
            password=amqp_pass,
        )

        amqp_port, ssl_options = internal.get_broker_ssl_and_port(
            ssl_enabled=ssl_enabled,
            cert_path=ssl_cert_path,
        )

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=amqp_host,
                port=amqp_port,
                credentials=credentials,
                ssl=ssl_enabled,
                ssl_options=ssl_options,
            )
        )
        settings = {
            'auto_delete': True,
            'durable': True,
            'exclusive': False
        }
        self.logs_queue = self.connection.channel()
        self.logs_queue.queue_declare(queue=self.LOGS_QUEUE_NAME, **settings)
        self.events_queue = self.connection.channel()
        self.events_queue.queue_declare(queue=self.LOGS_QUEUE_NAME, **settings)

    def publish_message(self, message, message_type):
        queue_name = self.EVENTS_QUEUE_NAME if message_type == 'event' \
            else self.LOGS_QUEUE_NAME
        self._publish(message, queue_name)

    def close(self):
        if self._is_closed:
            return

        self._is_closed = True
        if self.logs_queue:
            logger.debug('closing logs queue channel of thread {0}'.
                         format(current_thread()))
            self.logs_queue.close()
        if self.events_queue:
            logger.debug('closing events queue channel of thread {0}'.
                         format(current_thread()))
            self.events_queue.close()
        if self.connection:
            logger.debug('closing amqp connection of thread {0}'.
                         format(current_thread()))
            self.connection.close()

    def _publish(self, item, queue):
        if self._is_closed:
            raise ClosedAMQPClientException('publish failed, AMQP client '
                                            'already closed')
        self.events_queue.basic_publish(exchange='',
                                        routing_key=queue,
                                        body=json.dumps(item))


def create_client(amqp_host=broker_config.broker_hostname,
                  amqp_user=broker_config.broker_username,
                  amqp_pass=broker_config.broker_password,
                  ssl_enabled=broker_config.broker_ssl_enabled,
                  ssl_cert_path=broker_config.broker_cert_path):
    try:
        logger.debug('creating a new AMQP client for thread {0}, using'
                     ' hostname; {1}, username: {2}, ssl_enabled: {3},'
                     ' cert_path: {4}'.
                     format(current_thread(),
                            broker_config.broker_hostname,
                            broker_config.broker_username,
                            broker_config.broker_ssl_enabled,
                            broker_config.broker_cert_path))

        client = AMQPClient(amqp_host=amqp_host, amqp_user=amqp_user,
                            amqp_pass=amqp_pass, ssl_enabled=ssl_enabled,
                            ssl_cert_path=ssl_cert_path)

        logger.debug('AMQP client created for thread {0}'.
                     format(current_thread()))
    except Exception as e:
        err_msg = 'Failed to create AMQP client for thread: {0}, error: {1}'.\
            format(current_thread(), e)
        logger.warning(err_msg)
        raise

    return client
