########
# Copyright (c) 2017 GigaSpaces Technologies Ltd. All rights reserved
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
import os
import yaml
import shutil
import tarfile
import tempfile


METADATA_FILE_NAME = 'METADATA'


class InvalidCaravanException(Exception):
    pass


class Caravan(object):
    def __init__(self, caravan_path):
        self._caravan_path = caravan_path
        self._tempdir = tempfile.mkdtemp()
        self._cvn_dir = self._extract(self._caravan_path, self._tempdir)
        self._metadata = self._get_metadata(self._cvn_dir)

    @property
    def root_dir(self):
        return self._cvn_dir

    @staticmethod
    def _get_metadata(path):
        try:
            with open(os.path.join(path, METADATA_FILE_NAME)) as metadata_file:
                metadata = yaml.load(metadata_file)
        except Exception:
            raise InvalidCaravanException()
        return metadata

    @property
    def metadata(self):
        return self._metadata

    def __iter__(self):
        for wgn_path, yaml_path in self._metadata.iteritems():
            yield os.path.join(self._cvn_dir, wgn_path), \
                  os.path.join(self._cvn_dir, yaml_path)

    def __getitem__(self, item):
        return os.path.join(self._cvn_dir, self._metadata[item])

    @staticmethod
    def _extract(src, dest):
        tarfile_ = tarfile.open(name=src)
        try:
            # Get the top level dir
            root_dir = tarfile_.getmembers()[0]
            tarfile_.extractall(path=dest, members=tarfile_.getmembers())
        finally:
            tarfile_.close()
        return os.path.join(dest, root_dir.path)


def create(mappings, dest, name=None):
    tempdir = tempfile.mkdtemp()
    metadata = {}

    for wgn_path, yaml_path in mappings.iteritems():
        plugin_root_dir = os.path.basename(wgn_path).split('.', 1)[0]
        os.mkdir(os.path.join(tempdir, plugin_root_dir))

        dest_wgn_path = os.path.join(plugin_root_dir,
                                     os.path.basename(wgn_path))
        dest_yaml_path = os.path.join(plugin_root_dir,
                                      os.path.basename(yaml_path))

        shutil.copy(wgn_path, os.path.join(tempdir, dest_wgn_path))
        shutil.copy(yaml_path, os.path.join(tempdir, dest_yaml_path))
        metadata[dest_wgn_path] = dest_yaml_path

    yaml.dump(metadata, open(os.path.join(tempdir, METADATA_FILE_NAME), 'w+'))

    tar_name = name or 'palace'
    tar_path = os.path.join(dest, '{0}.cvn'.format(tar_name))
    tarfile_ = tarfile.open(tar_path, 'w:gz')
    try:
        tarfile_.add(tempdir, arcname=tar_name)
    finally:
        tarfile_.close()

    return tar_path


def validate(cvn_path):
    try:
        caravan_ = Caravan(cvn_path)
        if caravan_.metadata is not None:
            return True
    except Exception:
        pass

    return False
