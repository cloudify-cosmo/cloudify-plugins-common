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


import threading
import logging
import json

from cloudify.amqp_client import create_client

# A thread local for storing a separate amqp client for each thread
clients = threading.local()


def message_context_from_cloudify_context(ctx):
    """Build a message context from a CloudifyContext instance"""
    return {
        'blueprint_id': ctx.blueprint_id,
        'deployment_id': ctx.deployment_id,
        'execution_id': ctx.execution_id,
        'workflow_id': ctx.workflow_id,
        'task_id': ctx.task_id,
        'task_name': ctx.task_name,
        'task_target': ctx.task_target,
        'node_name': ctx.node_name,
        'node_id': ctx.node_id,
        'operation': ctx.operation,
        'plugin': ctx.plugin,
    }


def message_context_from_workflow_context(ctx):
    """Build a message context from a CloudifyWorkflowContext instance"""
    return {
        'blueprint_id': ctx.blueprint_id,
        'deployment_id': ctx.deployment_id,
        'execution_id': ctx.execution_id,
        'workflow_id': ctx.workflow_id,
    }


def message_context_from_workflow_node_instance_context(ctx):
    """Build a message context from a CloudifyWorkflowNode instance"""
    message_context = message_context_from_workflow_context(ctx.ctx)
    message_context.update({
        'node_name': ctx.name,
        'node_id': ctx.id,
    })
    return message_context


class CloudifyBaseLoggingHandler(logging.Handler):
    """A base handler class for writing log messages to RabbitMQ"""

    def __init__(self, ctx, message_context_builder):
        super(CloudifyBaseLoggingHandler, self).__init__()
        self.context = message_context_builder(ctx)

    def flush(self):
        pass

    def emit(self, record):
        message = self.format(record)
        log = {
            'context': self.context,
            'logger': record.name,
            'level': record.levelname.lower(),
            'message': {
                'text': message
            }
        }
        try:
            _amqp_client().publish_log(log)
        except BaseException as e:
            error_logger = logging.getLogger('cloudify_celery')
            error_logger.warning('Error publishing log to RabbitMQ ['
                                 'message={0}, log={1}]'
                                 .format(e.message, json.dumps(log)))


class CloudifyPluginLoggingHandler(CloudifyBaseLoggingHandler):
    """A handler class for writing plugin log messages to RabbitMQ"""
    def __init__(self, ctx):
        super(CloudifyPluginLoggingHandler, self).__init__(
            ctx, message_context_from_cloudify_context)


class CloudifyWorkflowLoggingHandler(CloudifyBaseLoggingHandler):
    """A Handler class for writing workflow log messages to RabbitMQ"""
    def __init__(self, ctx):
        super(CloudifyWorkflowLoggingHandler, self).__init__(
            ctx, message_context_from_workflow_context)


class CloudifyWorkflowNodeLoggingHandler(CloudifyBaseLoggingHandler):
    """A Handler class for writing workflow log messages to RabbitMQ"""
    def __init__(self, ctx):
        super(CloudifyWorkflowNodeLoggingHandler, self).__init__(
            ctx, message_context_from_workflow_node_instance_context)


def init_cloudify_logger(handler, logger_name,
                         logging_level=logging.INFO):
    """
    Instantiate an amqp backed logger based on the provided handler
    for sending log messages to RabbitMQ

    :param handler: A logger handler based on the context
    :param logger_name: The logger name
    :param logging_level: The logging level
    :return: An amqp backed logger
    """

    # TODO: somehow inject logging level (no one currently passes
    # logging_level)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging_level)
    for h in logger.handlers:
        logger.removeHandler(h)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.propagate = True
    logger.addHandler(handler)
    return logger


def send_workflow_event(ctx, event_type,
                        message=None,
                        args=None,
                        additional_context=None):
    """Send a workflow event to RabbitMQ

    :param ctx: A CloudifyWorkflowContext instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'workflow', event_type, message, args, additional_context)


def send_workflow_node_event(ctx, event_type,
                             message=None,
                             args=None,
                             additional_context=None):
    """Send a workflow node event to RabbitMQ

    :param ctx: A CloudifyWorkflowNode instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'workflow_node', event_type, message, args,
                additional_context)


def send_plugin_event(ctx,
                      message=None,
                      args=None,
                      additional_context=None):
    """Send a plugin event to RabbitMQ

    :param ctx: A CloudifyContext instance
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'plugin', 'plugin_event', message, args,
                additional_context)


def send_remote_task_event(remote_task,
                           event_type,
                           message=None,
                           args=None,
                           additional_context=None):
    """Send a remote task event to RabbitMQ

    :param remote_task: A RemoteWorkflowTask instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    # import here to avoid cyclic dependencies
    from cloudify.context import CloudifyContext
    _send_event(CloudifyContext(remote_task.cloudify_context),
                'remote_task', event_type, message, args,
                additional_context)


def _send_event(ctx, context_type, event_type,
                message, args, additional_context):
    if context_type in ['plugin', 'remote_task']:
        message_context = message_context_from_cloudify_context(
            ctx)
    elif context_type == 'workflow':
        message_context = message_context_from_workflow_context(ctx)
    elif context_type == 'workflow_node':
        message_context = message_context_from_workflow_node_instance_context(
            ctx)
    else:
        raise RuntimeError('Invalid context_type: {}'.format(context_type))

    additional_context = additional_context or {}
    message_context.update(additional_context)

    event = {
        'event_type': event_type,
        'context': message_context,
        'message': {
            'text': message,
            'arguments': args
        }
    }
    try:
        _amqp_client().publish_event(event)
    except BaseException as e:
        error_logger = logging.getLogger('cloudify_events')
        error_logger.warning('Error publishing event to RabbitMQ ['
                             'message={0}, event={1}]'
                             .format(e.message, json.dumps(event)))


def _amqp_client():
    """
    Get an AMQPClient for the current thread. If non currently exists,
    create one.

    :return: An AMQPClient belonging to the current thread
    """
    if not hasattr(clients, 'amqp_client'):
        clients.amqp_client = create_client()
    return clients.amqp_client
