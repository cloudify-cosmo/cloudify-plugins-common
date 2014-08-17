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

from proxy_tools import proxy


class CurrentContext(threading.local):

    def set(self, ctx):
        self.ctx = ctx

    def get(self):
        if not hasattr(self, 'ctx'):
            raise RuntimeError('No context set in current execution thread')
        result = self.ctx
        if result is None:
            raise RuntimeError('No context set in current execution thread')
        return result

    def clear(self):
        if hasattr(self, 'ctx'):
            delattr(self, 'ctx')

current_ctx = CurrentContext()


@proxy
def ctx():
    return current_ctx.get()
