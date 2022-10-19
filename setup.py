# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from setuptools import setup, find_packages

setup(
    name='amazon.ionbenchmark',
    version='0.1.0',
    description='Benchmarking tool for ion-python (https://github.com/amzn/ion-python/)',
    # url='https://github.com/amzn/ion-python-benchmark-cli',
    author='Amazon Ion Team',
    author_email='ion-team@amazon.com',
    license='Apache License 2.0',
    packages=find_packages(exclude=['tests*']),
    namespace_packages=['amazon'],
)
