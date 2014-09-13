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

from cloudify_rest_client import CloudifyClient
from cloudify.workflows import local

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('workflow')
    arg_parser.add_argument('blueprint_path')
    arg_parser.add_argument('--name', default='local')
    arg_parser.add_argument('--storage_dir', default='/tmp/cloudify-workflows')
    arg_parser.add_argument('--clear', action='store_true')
    arg_parser.add_argument('--bootstrap', action='store_true')
    arg_parser.add_argument('--pool-size', type=int, default=1)
    args = arg_parser.parse_args()

    env = local.Environment(args.blueprint_path,
                            name=args.name,
                            storage_cls=local.FileStorage,
                            storage_dir=args.storage_dir,
                            clear=args.clear)
    env.execute(args.workflow,
                task_retries=3,
                task_retry_interval=1,
                task_thread_pool_size=args.pool_size)
    if args.bootstrap:
        outputs = env.outputs()
        provider_context = outputs['provider_context']['value'][0] or {}
        bootstrap_context = outputs['cloudify']['value']
        agent_key_path = bootstrap_context['cloudify_agent'][
            'agent_key_path'][0]
        bootstrap_context['cloudify_agent']['agent_key_path'] = agent_key_path
        provider_context['cloudify'] = bootstrap_context
        management_ip = outputs['management_ip']['value'][0]
        provider_name = outputs['provider_name']['value'][0]
        rest = CloudifyClient(management_ip)
        rest.manager.create_context(provider_name, provider_context)
