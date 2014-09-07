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


import argparse

from cloudify.workflows import local

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('workflow')
    arg_parser.add_argument('blueprint_path')
    arg_parser.add_argument('--name', default='local')
    arg_parser.add_argument('--storage_dir', default='/tmp/cloudify-workflows')
    arg_parser.add_argument('--clear', action='store_true')
    args = arg_parser.parse_args()

    env = local.Environment(args.blueprint_path,
                            name=args.name,
                            storage_cls=local.FileStorage,
                            storage_dir=args.storage_dir,
                            clear=args.clear)
    env.execute(args.workflow, task_retries=3, task_retry_interval=1)
