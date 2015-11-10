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

import unittest
import collections

from cloudify.lru_cache import lru_cache


class TestLRUCacheDecorator(unittest.TestCase):

    def test_max_size(self):
        size = 10
        counters = collections.defaultdict(int)

        @lru_cache(maxsize=size)
        def func(index):
            result = counters[index]
            counters[index] = result + 1
            return result

        for time in range(2):
            for multiplier in range(2):
                for _ in range(3):
                    for i in range(size*multiplier, size*(multiplier+1)):
                        self.assertEqual(time, func(i))

    def test_on_purge(self):
        test_index = 1
        purges = []

        @lru_cache(on_purge=lambda index: purges.append(index), maxsize=1)
        def func(index):
            return index

        for _ in range(2):
            func(test_index)
            self.assertEqual(0, len(purges))
        func(test_index + 1)
        self.assertEqual(1, len(purges))
        self.assertEqual(test_index, purges[0])

    def test_clear(self):
        size = 3
        purges = []

        @lru_cache(maxsize=size, on_purge=lambda index: purges.append(index))
        def func(index):
            return index

        for i in range(size):
            func(i)
        self.assertEqual(0, len(purges))
        self.assertEqual(size, len(func._cache))
        func.clear()
        self.assertEqual(set([0, 1, 2]), set(purges))
        self.assertEqual(0, len(func._cache))
