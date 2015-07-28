########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.


class PluginFileNotFoundError(Exception):
    def __init__(self, original_path, file_to_find):
        msg = 'Traversing up the folder tree from {0}, failed to find {1}.'
        super(PluginFileNotFoundError, self).__init__(
            msg.format(original_path, file_to_find))
