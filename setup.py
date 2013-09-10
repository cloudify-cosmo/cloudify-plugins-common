__author__ = 'elip'

from setuptools import setup

setup(
    name='cosmo-celery-common',
    version='0.1.0',
    author='elip',
    author_email='elip@gigaspaces.com',
    packages=['cosmo'],
    license='LICENSE',
    description='Package that holds common cosmo modules needed by many plugins',
    install_requires=[
        "bernhard",
        "celery"
    ],
    tests_require=['nose']
)
