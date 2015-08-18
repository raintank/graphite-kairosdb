# coding: utf-8
from setuptools import setup

setup(
    name='graphite-kairosdb',
    version='0.2',
    url='https://github.com/raintank/graphite-kairosdb',
    license='apache2',
    author='Anthony Woods',
    author_email='awoods@raintank.io',
    description=('Kairosdb backend plugin for graphite-api'),
    long_description='',
    py_modules=('graphite_kairosdb',),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=(
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: System :: Monitoring',
    ),
    install_requires=(
        'cassandra-driver',
        'blist',
        'elasticsearch',
        'flask',
        'graphite_api'
    ),
)
