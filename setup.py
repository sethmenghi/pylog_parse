#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    # TODO: put package requirements here
    'numpy>=1.9.0',
    'pandas>=0.16.0',
    'IPython>=3.1.0',
    'sqlalchemy>=0.9.9'
]

test_requirements = [
    # TODO: put package test requirements here
    'numpy>=1.9.0',
    'pandas>=0.16.0',
    'IPython>=3.1.0',
    'sqlalchemy>=0.9.9'
]

setup(
    name='pylog_parse',
    version='0.1.0',
    description="Parses logs",
    long_description=readme + '\n\n' + history,
    author="Seth Menghi",
    author_email='sethmenghi@gmail.com',
    url='https://github.com/sethmenghi/pylog_parse',
    packages=[
        'pylog_parse',
    ],
    package_dir={'pylog_parse':
                 'pylog_parse'},
    include_package_data=True,
    scripts=['bin/pylog'],
    install_requires=requirements,
    license="BSD",
    zip_safe=False,
    keywords='pylog_parse',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
