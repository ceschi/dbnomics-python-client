# dbnomics-python-client -- Access DBnomics time series from Python
# By: DBnomics Team <christophe.benz@cepremap.org>
#
# Copyright (C) 2017-2018 Cepremap
# https://git.nomics.world/dbnomics/dbnomics-python-client
#
# dbnomics-python-client is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# dbnomics-python-client is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


"""
Project properties and packaging infos.
"""

import codecs
from os import path

from setuptools import find_packages, setup

HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with codecs.open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

setup(
    name='DBnomics',
    version='1.0.1',

    description='DBnomics Web API Client',
    long_description=LONG_DESCRIPTION,

    url='https://git.nomics.world/dbnomics/dbnomics-python-client',

    author='DBnomics Team',
    author_email='contact@nomics.world',

    license='AGPLv3',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        'Topic :: Scientific/Engineering :: Information Analysis',
        'Environment :: Web Environment',
        'Operating System :: POSIX',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Affero General Public License v3',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
    ],

    # What does your project relate to?
    keywords='economics time-series db.nomics',

    packages=find_packages(),
    install_requires=[
        'requests >= 2.18.4',
        'pandas >= 0.21',
    ],

    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest',
    ],
)
