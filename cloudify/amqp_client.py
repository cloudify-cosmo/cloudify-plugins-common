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

import ssl
import json
import time
import uuid
import Queue
import logging
import threading

import pika
import pika.exceptions

from cloudify import broker_config
from cloudify import exceptions

try:
    from cloudify_agent.api.factory import DaemonFactory
except ImportError:
    DaemonFactory = None


logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 30


class AMQPConnection(object):
    MAX_BACKOFF = 30

    def __init__(self, handlers, name=None):
        self._handlers = handlers
        self._publish_queue = Queue.Queue()
        self.name = name
        self._connection_params = self._get_connection_params()
        self._reconnect_backoff = 1
        self._closed = False
        self.connection = None
        self._consumer_thread = None
        self.connected = threading.Event()

    def _get_common_connection_params(self):
        credentials = pika.credentials.PlainCredentials(
            username=broker_config.broker_username,
            password=broker_config.broker_password,
        )
        return {
            'host': broker_config.broker_hostname,
            'port': broker_config.broker_port,
            'virtual_host': broker_config.broker_vhost,
            'credentials': credentials,
            'ssl': broker_config.broker_ssl_enabled,
            'ssl_options': broker_config.broker_ssl_options,
            'heartbeat': HEARTBEAT_INTERVAL
        }

    def _get_connection_params(self):
        while True:
            params = self._get_common_connection_params()
            if self.name and DaemonFactory is not None:
                daemon = DaemonFactory().load(self.name)
                if daemon.cluster:
                    for node_ip in daemon.cluster:
                        params['host'] = node_ip
                        yield pika.ConnectionParameters(**params)
                    continue
            yield pika.ConnectionParameters(**params)

    def _get_reconnect_backoff(self):
        backoff = self._reconnect_backoff
        self._reconnect_backoff = min(backoff * 2, self.MAX_BACKOFF)
        return backoff

    def _reset_reconnect_backoff(self):
        self._reconnect_backoff = 1

    def connect(self):
        for params in self._connection_params:
            try:
                self.connection = pika.BlockingConnection(params)
            except pika.exceptions.AMQPConnectionError:
                time.sleep(self._get_reconnect_backoff())
            else:
                self._reset_reconnect_backoff()
                self._closed = False
                break

        out_channel = self.connection.channel()
        for handler in self._handlers:
            handler.register(self.connection, self._publish_queue)
            logger.info('Registered handler for {0} [{1}]'
                        .format(handler.__class__.__name__,
                                handler.routing_key))
        self.connected.set()
        return out_channel

    def consume(self):
        out_channel = self.connect()
        while not self._closed:
            try:
                self.connection.process_data_events(0.2)
                self._process_publish(out_channel)
            except pika.exceptions.ChannelClosed as e:
                # happens when we attempt to use an exchange/queue that is not
                # declared - nothing we can do to help it, just exit
                logger.error('Channel closed: {0}'.format(e))
                break
            except pika.exceptions.ConnectionClosed:
                self.connected.clear()
                out_channel = self.connect()
                continue
        self._process_publish(out_channel)
        self.connection.close()

    def consume_in_thread(self):
        """Spawn a thread to run consume"""
        if self._consumer_thread:
            return
        self._consumer_thread = threading.Thread(target=self.consume)
        self._consumer_thread.daemon = True
        self._consumer_thread.start()
        self.connected.wait()
        return self._consumer_thread

    def __enter__(self):
        self.consume_in_thread()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _process_publish(self, channel):
        while True:
            try:
                msg = self._publish_queue.get_nowait()
            except Queue.Empty:
                return
            try:
                channel.basic_publish(**msg)
            except pika.exceptions.ConnectionClosed:
                if self._closed:
                    return
                # if we couldn't send the message because the connection
                # was down, requeue it to be sent again later
                self._publish_queue.put(msg)
                raise

    def close(self):
        self._closed = True
        if self._consumer_thread:
            self._consumer_thread.join()
            self._consumer_thread = None

    def add_handler(self, handler):
        self._handlers.append(handler)


class TaskConsumer(object):
    routing_key = ''

    def __init__(self, queue, threadpool_size=5):
        self.threadpool_size = threadpool_size
        self.exchange = queue
        self.queue = '{0}_{1}'.format(queue, self.routing_key)
        self._sem = threading.Semaphore(threadpool_size)
        self._output_queue = None
        self.in_channel = None

    def register(self, connection, output_queue):
        self._output_queue = output_queue
        self.in_channel = connection.channel()
        self._register_queue(self.in_channel)

    def _register_queue(self, channel):
        channel.basic_qos(prefetch_count=self.threadpool_size)
        channel.confirm_delivery()
        channel.exchange_declare(
            exchange=self.exchange, auto_delete=False, durable=True)
        channel.queue_declare(queue=self.queue,
                              durable=True,
                              auto_delete=False)
        channel.queue_bind(queue=self.queue,
                           exchange=self.exchange,
                           routing_key=self.routing_key)
        channel.basic_consume(self.process, self.queue)

    def process(self, channel, method, properties, body):
        try:
            full_task = json.loads(body)
        except ValueError:
            logger.error('Error parsing task: {0}'.format(body))
            return

        self._sem.acquire()
        new_thread = threading.Thread(
            target=self._process_message,
            args=(properties, full_task)
        )
        new_thread.daemon = True
        new_thread.start()
        channel.basic_ack(method.delivery_tag)

    def _process_message(self, properties, full_task):
        try:
            result = self.handle_task(full_task)
        except Exception as e:
            result = {'ok': False, 'error': repr(e)}
            logger.error(
                'ERROR - failed message processing: '
                '{0!r}\nbody: {1}'.format(e, full_task)
            )

        if properties.reply_to:
            self._output_queue.put({
                'exchange': '',
                'routing_key': properties.reply_to,
                'properties': pika.BasicProperties(
                    correlation_id=properties.correlation_id),
                'body': json.dumps(result)
            })
        self._sem.release()

    def handle_task(self, full_task):
        raise NotImplementedError()


class SendHandler(object):
    exchange_settings = {
        'auto_delete': False,
        'durable': True,
    }

    def __init__(self, exchange, exchange_type='direct', routing_key=''):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.logger = logging.getLogger('dispatch.{0}'.format(self.exchange))

    def register(self, connection, output_queue):
        self._output_queue = output_queue

        out_channel = connection.channel()
        out_channel.exchange_declare(exchange=self.exchange,
                                     exchange_type=self.exchange_type,
                                     **self.exchange_settings)

    def _log_message(self, message):
        level = message.get('level', 'info')
        log_func = getattr(self.logger, level, self.logger.info)
        exec_id = message.get('context', {}).get('execution_id')
        text = message['message']['text']
        msg = '[{0}] {1}'.format(exec_id, text) if exec_id else text
        log_func(msg)

    def publish(self, message, **kwargs):
        self._log_message(message)
        self._output_queue.put({
            'exchange': self.exchange,
            'body': json.dumps(message),
            'routing_key': self.routing_key
        })


class _RequestResponseHandlerBase(TaskConsumer):
    def __init__(self, exchange):
        super(_RequestResponseHandlerBase, self).__init__(exchange)
        self.queue = None

    def _register_queue(self, channel):
        self.queue = uuid.uuid4().hex
        self.in_channel.queue_declare(queue=self.queue, exclusive=True,
                                      durable=True)
        channel.basic_consume(self.process, self.queue)

    def publish(self, message, correlation_id=None, routing_key=''):
        if correlation_id is None:
            correlation_id = uuid.uuid4().hex

        self._output_queue.put({
            'exchange': self.exchange,
            'body': json.dumps(message),
            'properties': pika.BasicProperties(
                reply_to=self.queue,
                correlation_id=correlation_id),
            'routing_key': routing_key
        })
        return correlation_id

    def process(self, channel, method, properties, body):
        raise NotImplementedError()


class BlockingRequestResponseHandler(_RequestResponseHandlerBase):
    def __init__(self, *args, **kwargs):
        super(BlockingRequestResponseHandler, self).__init__(*args, **kwargs)
        self._response_queues = {}

    def publish(self, *args, **kwargs):
        timeout = kwargs.pop('timeout', None)
        correlation_id = super(BlockingRequestResponseHandler, self)\
            .publish(*args, **kwargs)
        self._response_queues[correlation_id] = Queue.Queue()
        try:
            resp = self._response_queues[correlation_id].get(timeout=timeout)
            return resp
        except Queue.Empty:
            raise RuntimeError('No response received for task {0}'
                               .format(correlation_id))
        finally:
            del self._response_queues[correlation_id]

    def process(self, channel, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        try:
            response = json.loads(body)
        except ValueError:
            logger.error('Error parsing response: {0}'.format(body))
            return
        if properties.correlation_id in self._response_queues:
            self._response_queues[properties.correlation_id].put(response)


class CallbackRequestResponseHandler(_RequestResponseHandlerBase):
    def __init__(self, *args, **kwargs):
        super(CallbackRequestResponseHandler, self).__init__(*args, **kwargs)
        self._callbacks = {}

    def publish(self, *args, **kwargs):
        callback = kwargs.pop('callback')
        correlation_id = super(CallbackRequestResponseHandler, self)\
            .publish(*args, **kwargs)
        self._callbacks[correlation_id] = callback

    def process(self, channel, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        try:
            response = json.loads(body)
        except ValueError:
            logger.error('Error parsing response: {0}'.format(body))
            return
        if properties.correlation_id in self._callbacks:
            self._callbacks[properties.correlation_id](response)


class CloudifyConnectionAMQPConnection(AMQPConnection):
    """An AMQPConnection that takes amqp connection params overrides"""
    def __init__(self, amqp_settings, *args, **kwargs):
        self._amqp_settings = amqp_settings
        super(CloudifyConnectionAMQPConnection, self).__init__(*args, **kwargs)

    def _get_common_connection_params(self):
        params = super(CloudifyConnectionAMQPConnection, self)\
            ._get_common_connection_params()
        for setting_name, setting in self._amqp_settings.items():
            if setting:
                params[setting_name] = setting
        return params


def get_client(username=None, password=None, vhost=None):
    """
    Create a client without any handlers in it. Use the `add_handler` method
    to add handlers to this client
    :return: CloudifyConnectionAMQPConnection
    """
    username = username or broker_config.broker_username
    password = password or broker_config.broker_password
    vhost = vhost or broker_config.broker_vhost

    credentials = pika.credentials.PlainCredentials(
        username=username,
        password=password
    )
    return CloudifyConnectionAMQPConnection({
        'credentials': credentials,
        'virtual_host': vhost
    }, [])


class CloudifyEventsPublisher(object):
    EVENTS_EXCHANGE_NAME = 'cloudify-events'
    SOCKET_TIMEOUT = 5
    CONNECTION_ATTEMPTS = 3
    LOGS_EXCHANGE_NAME = 'cloudify-logs'
    channel_settings = {
        'auto_delete': False,
        'durable': True,
    }

    def __init__(self, amqp_settings):
        self.events_handler = SendHandler(self.EVENTS_EXCHANGE_NAME,
                                          exchange_type='fanout')
        self.logs_handler = SendHandler(self.LOGS_EXCHANGE_NAME,
                                        exchange_type='fanout')
        self._connection = CloudifyConnectionAMQPConnection(amqp_settings, [
            self.events_handler, self.logs_handler])
        self._is_closed = False

    def connect(self):
        self._connection.consume_in_thread()

    def publish_message(self, message, message_type):
        if self._is_closed:
            raise exceptions.ClosedAMQPClientException(
                'Publish failed, AMQP client already closed')
        if message_type == 'event':
            handler = self.events_handler
        else:
            handler = self.logs_handler
        handler.publish(message)

    def close(self):
        if self._is_closed:
            return
        self._is_closed = True
        thread = threading.current_thread()
        if self._connection:
            logger.debug('Closing amqp client of thread {0}'.format(thread))
            try:
                self._connection.close()
            except Exception as e:
                logger.debug('Failed to close amqp client of thread {0}, '
                             'reported error: {1}'.format(thread, repr(e)))


def create_events_publisher(amqp_host=None,
                            amqp_user=None,
                            amqp_pass=None,
                            amqp_vhost=None,
                            ssl_enabled=None,
                            ssl_cert_path=None):
    thread = threading.current_thread()

    # there's 3 possible sources of the amqp settings: passed in arguments,
    # current cluster active manager (if any), and broker_config; use the first
    # that is defined, in that order
    amqp_settings = {
        'host': amqp_host,
        'virtual_host': amqp_vhost,
        'ssl': ssl_enabled,
    }
    if amqp_user and amqp_pass:
        amqp_settings['credentials'] = pika.credentials.PlainCredentials(
            username=amqp_user,
            password=amqp_pass
        )
    if ssl_cert_path:
        amqp_settings['ssl_options'] = {
            'ca_certs': ssl_cert_path,
            'cert_reqs': ssl.CERT_REQUIRED,
        }

    try:
        client = CloudifyEventsPublisher(amqp_settings)
        client.connect()
        logger.debug('AMQP client created for thread {0}'.format(thread))
    except Exception as e:
        logger.warning(
            'Failed to create AMQP client for thread: {0} ({1}: {2})'
            .format(thread, type(e).__name__, e))
        raise
    return client
