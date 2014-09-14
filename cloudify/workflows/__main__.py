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
    arg_parser.add_argument('--init', action='store_true')
    arg_parser.add_argument('--bootstrap', action='store_true')
    arg_parser.add_argument('--pool-size', type=int, default=1)
    args = arg_parser.parse_args()

    storage = local.FileStorage(args.storage_dir)
    name = args.name
    if args.init:
        env = local.init_env(args.blueprint_path, name=name, storage=storage)
    else:
        env = local.load_env(name=name, storage=storage)
        env.execute(args.workflow,
                    task_retries=3,
                    task_retry_interval=1,
                    task_thread_pool_size=args.pool_size)
        if args.bootstrap:
            outputs = env.outputs()
            provider = outputs['provider']['value']
            provider_context = provider['context'][0] or {}
            bootstrap_context = outputs['cloudify']['value']
            agent_key_path = bootstrap_context['cloudify_agent'][
                'agent_key_path'][0]
            bootstrap_context['cloudify_agent'][
                'agent_key_path'] = agent_key_path
            provider_context['cloudify'] = bootstrap_context
            management_endpoint = outputs['management_endpoint']['value'][0]
            rest = CloudifyClient(management_endpoint['manager_ip'])
            rest.manager.create_context(provider['name'],
                                        provider_context)
