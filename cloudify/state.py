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


import threading
from contextlib import contextmanager

from proxy_tools import proxy


class NotInContext(RuntimeError):
    """Attempted accesing a context, but no context is available.
    """


class CtxParameters(dict):

    def __init__(self, parameters):
        parameters = parameters or {}
        super(CtxParameters, self).__init__(parameters)

    def __getattr__(self, attr):
        if attr in self:
            return self.get(attr)
        else:
            raise KeyError(attr)


class CurrentContext(threading.local):

    def set(self, ctx, parameters=None):
        self.ctx = ctx
        self.parameters = CtxParameters(parameters)

    def get_ctx(self):
        return self._get('ctx')

    def get_parameters(self):
        return self._get('parameters')

    def _get(self, attribute):
        if not hasattr(self, attribute):
            raise NotInContext('No context set in current execution thread')
        result = getattr(self, attribute)
        if result is None:
            raise NotInContext('No context set in current execution thread')
        return result

    def clear(self):
        if hasattr(self, 'ctx'):
            delattr(self, 'ctx')
        if hasattr(self, 'parameters'):
            delattr(self, 'parameters')

    @contextmanager
    def push(self, ctx, parameters=None):
        try:
            previous_ctx = self.get_ctx()
        except NotInContext:
            previous_ctx = None
        try:
            previous_parameters = self.get_parameters()
        except NotInContext:
            previous_parameters = None

        self.set(ctx, parameters)
        try:
            yield self
        finally:
            try:
                self.set(previous_ctx, previous_parameters)
            except Exception:
                # this can only happen during interpreter shutdown, if running
                # inside a daemon thread; in that case, things can fail
                # semi-randomly, and we should just ignore the exceptions
                # (see CFY-6802)
                pass


current_ctx = CurrentContext()
current_workflow_ctx = CurrentContext()


@proxy
def ctx():
    return current_ctx.get_ctx()


@proxy
def ctx_parameters():
    return current_ctx.get_parameters()


@proxy
def workflow_ctx():
    return current_workflow_ctx.get_ctx()


@proxy
def workflow_parameters():
    return current_workflow_ctx.get_parameters()
