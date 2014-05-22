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
import json

from cloudify.amqp_client import create_client


def message_context_from_cloudify_context(ctx):
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
    return {
        'blueprint_id': ctx.blueprint_id,
        'deployment_id': ctx.deployment_id,
        'execution_id': ctx.execution_id,
        'workflow_id': ctx.workflow_id,
    }


def message_context_from_workflow_node_context(ctx):
    message_context = message_context_from_workflow_context(ctx.ctx)
    message_context.update({
        'node_name': ctx.name,
        'node_id': ctx.id,
    })
    return message_context


class CloudifyBaseLoggingHandler(logging.Handler):
    """
    A Handler class for writing log messages to RabbitMQ.
    """
    amqp_client = None

    def __init__(self, ctx):
        super(CloudifyBaseLoggingHandler, self).__init__()
        self._ctx = ctx

    def flush(self):
        pass

    def emit(self, record):
        message = self.format(record)
        log = {
            'context': self.context(),
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
            CloudifyBaseLoggingHandler.amqp_client = create_client()
        self.amqp_client.publish_log(log)

    def context(self):
        raise NotImplementedError()


class CloudifyPluginLoggingHandler(CloudifyBaseLoggingHandler):
    """
    A Handler class for writing plugin log messages to RabbitMQ.
    """

    def context(self):
        return message_context_from_cloudify_context(self._ctx)


class CloudifyWorkflowLoggingHandler(CloudifyBaseLoggingHandler):
    """
    A Handler class for writing workflow log messages to RabbitMQ.
    """

    def context(self):
        return message_context_from_workflow_context(self._ctx)


class CloudifyWorkflowNodeLoggingHandler(CloudifyBaseLoggingHandler):
    """
    A Handler class for writing workflow log messages to RabbitMQ.
    """

    def context(self):
        return message_context_from_workflow_node_context(self._ctx)


def init_cloudify_logger(handler, logger_name,
                         logging_level=logging.INFO):
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
    _send_event(ctx, 'workflow', event_type, message, args, additional_context)


def send_workflow_node_event(ctx, event_type,
                             message=None,
                             args=None,
                             additional_context=None):
    _send_event(ctx, 'workflow_node', event_type, message, args,
                additional_context)


def send_plugin_event(ctx,
                      message=None,
                      args=None,
                      additional_context=None):
    _send_event(ctx, 'plugin', 'plugin_event', message, args,
                additional_context)


def send_remote_task_event(remote_task,
                           event_type,
                           message=None,
                           args=None,
                           additional_context=None):
    from cloudify.context import CloudifyContext
    _send_event(CloudifyContext(remote_task.cloudify_context),
                'remote_task', event_type, message, args,
                additional_context)


def _send_event(ctx, context_type, event_type,
                message, args, additional_context):
    if CloudifyBaseLoggingHandler.amqp_client is None:
        CloudifyBaseLoggingHandler.amqp_client = create_client()
    client = CloudifyWorkflowNodeLoggingHandler.amqp_client

    if context_type in ['plugin', 'remote_task']:
        message_context = message_context_from_cloudify_context(
            ctx)
    elif context_type == 'workflow':
        message_context = message_context_from_workflow_context(ctx)
    elif context_type == 'workflow_node':
        message_context = message_context_from_workflow_node_context(ctx)
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
    client.publish_event(event)
