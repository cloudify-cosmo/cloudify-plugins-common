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


import unittest
from cloudify.decorators import operation
from cloudify.context import CloudifyContext


@operation
def method_empty_ctx(a, b, ctx, **kwargs):
    if ctx is None or not isinstance(ctx, CloudifyContext):
        raise RuntimeError()


@operation
def method_with_ctx(a, b, ctx=None, **kwargs):
    if ctx is None or ctx.node_id != '1234':
        raise RuntimeError()


class OperationTest(unittest.TestCase):

    def test_empty_ctx(self):
        method_empty_ctx(0, 0)

    def test_provided_ctx(self):
        ctx = {'node_id': '1234'}
        kwargs = {'__cloudify_context': ctx}
        method_with_ctx(0, 0, **kwargs)


