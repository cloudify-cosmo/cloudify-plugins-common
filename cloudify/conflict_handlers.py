########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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


def simple_merge_handler(previous_props, next_props):
    """Merge properties if the keys on old and new are disjoint.

    Return a mapping containing the values from both old and new properties,
    but only if there is no key that exists in both.
    """
    for name, value in previous_props.items():
        if name in next_props and value != next_props[name]:
            raise ValueError('Cannot merge - {0} changed (old value: {1}, '
                             ' new value: {2})'.format(
                                 name, value, next_props[name]))
        if name not in next_props:
            next_props[name] = value
    return next_props
