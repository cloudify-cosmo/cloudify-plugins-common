########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

import logging
import logging.handlers
import os
import shutil
import tempfile
import time

import mock
from mock import patch
import testtools

from cloudify import logs
from cloudify.celery import logging_server


class TestLoggingServer(testtools.TestCase):

    HANDLER_CONTEXT = 'logger'

    def setUp(self):
        self.worker = mock.Mock()
        super(TestLoggingServer, self).setUp()
        self.workdir = tempfile.mkdtemp(prefix='cloudify-logging-server-')
        self.addCleanup(lambda: shutil.rmtree(self.workdir,
                                              ignore_errors=True))

    def test_basic(self):
        message = 'MESSAGE TEXT'
        server = self._start_server()
        self.assertEqual(server, self.worker.logging_server)
        logger = self._logger(server)
        logger.info(message)
        self._assert_in_log(message)
        return server, logger

    def test_disabled(self):
        server = self._start_server(enable=False)
        self.assertIsNone(server.logging_server)
        self.assertIsNone(server.socket_url)

    def test_handler_cache_size(self):
        num_loggers = 7
        cache_size = 3
        server = self._start_server(cache_size=cache_size)
        get_handler = server.logging_server._get_handler
        get_handler_cache = get_handler._cache
        logger_names = ['logger{0}'.format(i) for i in range(num_loggers)]
        for i in range(num_loggers):
            logger_name = logger_names[i]
            logger = self._logger(server, logger_name)
            logger.info(logger_name)
            self._assert_in_log(logger_name, logger_name)
            self.assertEqual(min(i+1, cache_size), len(get_handler_cache))
            actual_handler = get_handler_cache[(logger_name,)]
            expected_handler = get_handler(logger_name)
            self.assertEqual(expected_handler, actual_handler)
            for j in range(max(0, i-2), i+1):
                self.assertIn((logger_names[j],), get_handler_cache)

    def test_stop(self):
        server, logger = self.test_basic()
        logger.handlers[0]._socket.close()
        handler_cache = server.logging_server._get_handler._cache
        self.assertEqual(1, len(handler_cache))
        server_handler = next(handler_cache.itervalues())
        self.assertIsNotNone(server_handler.stream)
        server.stop(self.worker)
        server.thread.join()
        self.assertEqual(0, len(handler_cache))
        self.assertIsNone(server_handler.stream)

    def test_server_logging_handler_type_on_management(self):
        with patch.dict(os.environ, {'MGMTWORKER_HOME': 'stub'}):
            self._test_server_logging_type(logging.FileHandler)

    def test_server_logging_handler_type_on_agent(self):
        self._test_server_logging_type(logging.handlers.RotatingFileHandler)

    def _test_server_logging_type(self, expected_type):
        server, _ = self.test_basic()
        handler_cache = server.logging_server._get_handler._cache
        server_handler = handler_cache[(self.HANDLER_CONTEXT,)]
        # type(server_handler) doesn't do the right thing on 2.6
        self.assertEqual(server_handler.__class__, expected_type)

    def test_error_on_processing(self):
        server, logger = self.test_basic()
        for i in range(10):
            logger.handlers[0]._socket.send('Certainly not json')
        good_message = 'some new good message'
        logger.info(good_message)
        self._assert_in_log(good_message)

    def _start_server(self, enable=True, cache_size=10):
        server = logging_server.ZMQLoggingServerBootstep(
            self.worker,
            with_logging_server=enable,
            logging_server_logdir=self.workdir,
            logging_server_handler_cache_size=cache_size)
        self.addCleanup(lambda: server.stop(self.worker))
        server.start(self.worker)
        return server

    def _logger(self, server, handler_context=HANDLER_CONTEXT):
        import zmq
        context = server.logging_server.zmq_context
        socket = context.socket(zmq.PUSH)
        socket.connect(server.socket_url)
        logger = logging.getLogger(handler_context)
        logger.handlers = []
        handler = logs.ZMQLoggingHandler(handler_context, socket,
                                         fallback_logger=logging.getLogger())
        handler.setFormatter(logging.Formatter('%(message)s'))
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        self.addCleanup(lambda: socket.close())
        return logger

    def _assert_in_log(self, message, handler_context=HANDLER_CONTEXT):
        attempts = 0
        current = None
        while attempts < 500:
            try:
                with open(os.path.join(
                        self.workdir, '{0}.log'.format(handler_context))) as f:
                    current = f.read()
                self.assertIn(message, current)
                return
            except Exception:
                attempts += 1
                time.sleep(0.01)
        self.fail('failed asserting logs: {0}'.format(current))
