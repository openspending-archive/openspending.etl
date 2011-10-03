"""\
OpenSpending ETL test module
============================

Run the OpenSpending test suite by running

    nosetests

in the root of the repository, while in an active virtualenv. See
doc/install.rst for more information.
"""

import os

from pylons import config

from openspending import model
from openspending.test import TestCase, DatabaseTestCase, \
        setup_package as _setup_package

__all__ = ['TestCase', 'DatabaseTestCase']

def setup_package():
    return _setup_package()

