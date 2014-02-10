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

__author__ = 'elip'

from setuptools import setup

REST_CLIENT_VERSION = '0.3'
REST_CLIENT_BRANCH = 'develop'
REST_CLIENT =\
    "https://github.com/CloudifySource/cosmo-manager-rest-client/tarball/{0}"\
    .format(REST_CLIENT_BRANCH)


setup(
    name='cosmo-celery-common',
    version='0.3',
    author='elip',
    author_email='elip@gigaspaces.com',
    packages=['cloudify'],
    license='LICENSE',
    description=
    'Package that holds common cosmo modules needed by many plugins',
    zip_safe=False,
    install_requires=[
        # we include this dependency here because protobuf may fail
        # to install transitively.
        # see https://pypi.python.org/pypi/bernhard/0.1.0
        "protobuf",
        "bernhard",
        "celery==3.0.24",
        "cosmo-manager-rest-client"
    ],
    test_requires=[
        "nose"
    ],
    dependency_links=["{0}#egg=cosmo-manager-rest-client-{1}"
                      .format(REST_CLIENT, REST_CLIENT_VERSION)]
)
