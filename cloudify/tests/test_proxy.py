########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


import unittest
import os
import threading
import time
import sys
import subprocess
from StringIO import StringIO

import testtools
from nose.tools import nottest, istest

from cloudify.mocks import MockCloudifyContext
from cloudify.proxy import client
from cloudify.proxy.server import (UnixCtxProxy,
                                   TCPCtxProxy,
                                   HTTPCtxProxy,
                                   PathDictAccess)

IS_WINDOWS = os.name == 'nt'


@nottest
class TestCtxProxy(testtools.TestCase):

    class StubAttribute(object):
        some_property = 'some_value'

    @staticmethod
    def stub_method(*args):
        return args

    @staticmethod
    def stub_sleep(seconds):
        time.sleep(float(seconds))

    @staticmethod
    def stub_args(arg1, arg2, arg3='arg3', arg4='arg4', *args, **kwargs):
        return dict(
            arg1=arg1,
            arg2=arg2,
            arg3=arg3,
            arg4=arg4,
            args=args,
            kwargs=kwargs)

    def setUp(self):
        super(TestCtxProxy, self).setUp()
        self.ctx = MockCloudifyContext(node_id='instance_id', properties={
            'prop1': 'value1',
            'prop2': {
                'nested_prop1': 'nested_value1'
            },
            'prop3': [
                {'index': 0, 'value': 'value_0'},
                {'index': 1, 'value': 'value_1'},
                {'index': 2, 'value': 'value_2'}
            ],
            'prop4': {
                'key': 'value'
            }
        })
        self.ctx.stub_method = self.stub_method
        self.ctx.stub_sleep = self.stub_sleep
        self.ctx.stub_args = self.stub_args
        self.ctx.stub_attr = self.StubAttribute()
        self.server = self.proxy_server_class(self.ctx)
        self.start_server()

    def start_server(self):
        self.stop_server = False
        self.server_stopped = False

        def serve():
            while not self.stop_server:
                self.server.poll_and_process(timeout=0.1)
            self.server.close()
            self.server_stopped = True
        self.server_thread = threading.Thread(target=serve)
        self.server_thread.daemon = True
        self.server_thread.start()

    def stop_server_now(self):
        self.stop_server = True
        while not self.server_stopped:
            time.sleep(0.1)

    def tearDown(self):
        self.stop_server_now()
        super(TestCtxProxy, self).tearDown()

    def request(self, *args):
        return client.client_req(self.server.socket_url, args)

    def test_attribute_access(self):
        response = self.request('stub_attr', 'some_property')
        self.assertEqual(response, 'some_value')

    def test_sugared_attribute_access(self):
        response = self.request('stub-attr', 'some-property')
        self.assertEqual(response, 'some_value')

    def test_dict_prop_access_get_key(self):
        response = self.request('node', 'properties', 'prop1')
        self.assertEqual(response, 'value1')

    def test_dict_prop_access_get_key_nested(self):
        response = self.request('node', 'properties', 'prop2.nested_prop1')
        self.assertEqual(response, 'nested_value1')

    def test_dict_prop_access_get_with_list_index(self):
        response = self.request('node', 'properties', 'prop3[2].value')
        self.assertEqual(response, 'value_2')

    def test_dict_prop_access_set(self):
        self.request('node', 'properties', 'prop4.key', 'new_value')
        self.request('node', 'properties', 'prop3[2].value', 'new_value_2')
        self.request('node', 'properties', 'prop4.some.new.path',
                     'some_new_value')
        self.assertEqual(self.ctx.node.properties['prop4']['key'], 'new_value')
        self.assertEqual(
            self.ctx.node.properties['prop3'][2]['value'],
            'new_value_2')
        self.assertEqual(
            self.ctx.node.properties['prop4']['some']['new']['path'],
            'some_new_value')

    def test_method_invocation(self):
        args = ['arg1', 'arg2', 'arg3']
        response_args = self.request('stub-method', *args)
        self.assertEqual(args, response_args)

    def test_method_invocation_no_args(self):
        response = self.request('stub-method')
        self.assertEqual([], response)

    def test_method_invocation_kwargs(self):
        arg1 = 'arg1'
        arg2 = 'arg2'
        arg4 = 'arg4_override'
        arg5 = 'arg5'
        kwargs = dict(
            arg4=arg4,
            arg5=arg5)
        response = self.request('stub_args', arg1, arg2, kwargs)
        self.assertEqual(response, dict(
            arg1=arg1,
            arg2=arg2,
            arg3='arg3',
            arg4=arg4,
            args=[],
            kwargs=dict(
                arg5=arg5)))

    def test_empty_return_value(self):
        response = self.request('blueprint', 'id')
        self.assertIsNone(response)

    def test_client_request_timeout(self):
        if hasattr(self, 'expected_exception'):
            expected_exception = self.expected_exception
        else:
            expected_exception = RuntimeError
        self.assertRaises(expected_exception,
                          client.client_req,
                          self.server.socket_url,
                          ['stub-sleep', '0.5'],
                          0.1)

    def test_processing_exception(self):
        self.assertRaises(client.RequestError,
                          self.request, 'property_that_does_not_exist')

    def test_not_json_serializable(self):
        self.assertRaises(client.RequestError,
                          self.request, 'logger')

    def test_no_string_arg(self):
        args = ['stub_method', 1, 2]
        response = self.request(*args)
        self.assertEqual(args[1:], response)


@istest
class TestUnixCtxProxy(TestCtxProxy):

    def setUp(self):
        if IS_WINDOWS:
            raise unittest.SkipTest('Test skipped on windows')
        self.proxy_server_class = UnixCtxProxy
        super(TestUnixCtxProxy, self).setUp()


@istest
class TestTCPCtxProxy(TestCtxProxy):

    def setUp(self):
        self.proxy_server_class = TCPCtxProxy
        super(TestTCPCtxProxy, self).setUp()


@istest
class TestHTTPCtxProxy(TestCtxProxy):

    def setUp(self):
        self.proxy_server_class = HTTPCtxProxy
        super(TestHTTPCtxProxy, self).setUp()

    def start_server(self):
        pass

    def stop_server_now(self):
        self.server.close()

    def test_client_request_timeout(self):
        self.expected_exception = IOError
        super(TestHTTPCtxProxy, self).test_client_request_timeout()


class TestArgumentParsing(testtools.TestCase):

    def mock_client_req(self, socket_url, args, timeout):
        self.assertEqual(socket_url, self.expected.get('socket_url'))
        self.assertEqual(args, self.expected.get('args'))
        self.assertEqual(timeout, int(self.expected.get('timeout')))
        return self.mock_response

    def setUp(self):
        super(TestArgumentParsing, self).setUp()
        self.original_client_req = client.client_req
        client.client_req = self.mock_client_req
        self.addCleanup(self.restore)
        self.expected = dict(
            args=[],
            timeout=30,
            socket_url='stub')
        self.mock_response = None
        os.environ['CTX_SOCKET_URL'] = 'stub'

    def restore(self):
        client.client_req = self.original_client_req
        if 'CTX_SOCKET_URL' in os.environ:
            del os.environ['CTX_SOCKET_URL']

    def test_socket_url_arg(self):
        self.expected.update(dict(
            socket_url='sock_url'))
        client.main(['--socket-url', self.expected.get('socket_url')])

    def test_socket_url_env(self):
        expected_socket_url = 'env_sock_url'
        os.environ['CTX_SOCKET_URL'] = expected_socket_url
        self.expected.update(dict(
            socket_url=expected_socket_url))
        client.main([])

    def test_socket_url_missing(self):
        del os.environ['CTX_SOCKET_URL']
        self.assertRaises(RuntimeError,
                          client.main, [])

    def test_args(self):
        self.expected.update(dict(
            args=['1', '2', '3']))
        client.main(self.expected.get('args'))

    def test_timeout(self):
        self.expected.update(dict(
            timeout='10'))
        client.main(['--timeout', self.expected.get('timeout')])
        self.expected.update(dict(
            timeout='15'))
        client.main(['-t', self.expected.get('timeout')])

    def test_mixed_order(self):
        self.expected.update(dict(
            args=['1', '2', '3'],
            timeout='20',
            socket_url='mixed_socket_url'))
        client.main(
            ['-t', self.expected.get('timeout')] +
            ['--socket-url', self.expected.get('socket_url')] +
            self.expected.get('args'))
        client.main(
            ['-t', self.expected.get('timeout')] +
            self.expected.get('args') +
            ['--socket-url', self.expected.get('socket_url')])
        client.main(
            self.expected.get('args') +
            ['-t', self.expected.get('timeout')] +
            ['--socket-url', self.expected.get('socket_url')])

    def test_json_args(self):
        args = ['@1', '@[1,2,3]', '@{"key":"value"}']
        expected_args = [1, [1, 2, 3], {'key': 'value'}]
        self.expected.update(dict(
            args=expected_args))
        client.main(args)

    def test_json_arg_prefix(self):
        args = ['_1', '@1']
        expected_args = [1, '@1']
        self.expected.update(dict(
            args=expected_args))
        client.main(args + ['--json-arg-prefix', '_'])

    def test_json_output(self):
        self.assert_valid_output('string', 'string', '"string"')
        self.assert_valid_output(1, '1', '1')
        self.assert_valid_output([1, '2'], "[1, '2']", '[1, "2"]')
        self.assert_valid_output({'key': 1},
                                 "{'key': 1}",
                                 '{"key": 1}')
        self.assert_valid_output(False, '', 'false')
        self.assert_valid_output(True, 'True', 'true')
        self.assert_valid_output([], '', '[]')
        self.assert_valid_output({}, '', '{}')

    def assert_valid_output(self, response, ex_typed_output, ex_json_output):
        self.mock_response = response
        current_stdout = sys.stdout

        def run(args, expected):
            output = StringIO()
            sys.stdout = output
            client.main(args)
            self.assertEqual(output.getvalue(), expected)

        try:
            run([], ex_typed_output)
            run(['-j'], ex_json_output)
            run(['--json-output'], ex_json_output)
        finally:
            sys.stdout = current_stdout


class TestCtxEntryPoint(testtools.TestCase):

    def test_ctx_in_path(self):
        subprocess.call(['ctx', '--help'])


class TestPathDictAccess(testtools.TestCase):
    def test_simple_set(self):
        obj = {}
        path_dict = PathDictAccess(obj)
        path_dict.set('foo', 42)
        self.assertEqual(obj, {'foo': 42})

    def test_nested_set(self):
        obj = {'foo': {}}
        path_dict = PathDictAccess(obj)
        path_dict.set('foo.bar', 42)
        self.assertEqual(obj, {'foo': {'bar': 42}})

    def test_set_index(self):
        obj = {'foo': [None, {'bar': 0}]}
        path_dict = PathDictAccess(obj)
        path_dict.set('foo[1].bar', 42)
        self.assertEqual(obj, {'foo': [None, {'bar': 42}]})

    def test_set_nonexistent_parent(self):
        obj = {}
        path_dict = PathDictAccess(obj)
        path_dict.set('foo.bar', 42)
        self.assertEqual(obj, {'foo': {'bar': 42}})

    def test_set_nonexistent_parent_nested(self):
        obj = {}
        path_dict = PathDictAccess(obj)
        path_dict.set('foo.bar.baz', 42)
        self.assertEqual(obj, {'foo': {'bar': {'baz': 42}}})

    def test_simple_get(self):
        obj = {'foo': 42}
        path_dict = PathDictAccess(obj)
        result = path_dict.get('foo')
        self.assertEqual(result, 42)

    def test_nested_get(self):
        obj = {'foo': {'bar': 42}}
        path_dict = PathDictAccess(obj)
        result = path_dict.get('foo.bar')
        self.assertEqual(result, 42)

    def test_nested_get_shadows_dotted_name(self):
        obj = {'foo': {'bar': 42}, 'foo.bar': 58}
        path_dict = PathDictAccess(obj)
        result = path_dict.get('foo.bar')
        self.assertEqual(result, 42)

    def test_index_get(self):
        obj = {'foo': [0, 1]}
        path_dict = PathDictAccess(obj)
        result = path_dict.get('foo[1]')
        self.assertEqual(result, 1)

    def test_get_nonexistent(self):
        obj = {}
        path_dict = PathDictAccess(obj)
        self.assertRaises(RuntimeError, path_dict.get, 'foo')

    def test_get_by_index_not_list(self):
        obj = {'foo': {0: 'not-list'}}
        path_dict = PathDictAccess(obj)
        self.assertRaises(RuntimeError, path_dict.get, 'foo[0]')

    def test_get_by_index_nonexistent_parent(self):
        obj = {}
        path_dict = PathDictAccess(obj)
        self.assertRaises(RuntimeError, path_dict.get, 'foo[1]')
