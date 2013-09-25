__author__ = 'elip'

from setuptools import setup

setup(
    name='cosmo-celery-common',
    version='0.1.1',
    author='elip',
    author_email='elip@gigaspaces.com',
    packages=['cosmo'],
    license='LICENSE',
    description='Package that holds common cosmo modules needed by many plugins',
    install_requires=[
        # we include this dependency here because protobuf may fail to install transitively.
        # see https://pypi.python.org/pypi/bernhard/0.1.0
        "protobuf",
        "bernhard",
        "celery"
    ]
)
