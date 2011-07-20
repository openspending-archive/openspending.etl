"""\
OpenSpending ETL UI test module
===============================

Run the OpenSpending test suite by running

    nosetests

in the root of the repository, while in an active virtualenv. See
doc/install.rst for more information.
"""
import os
import sys

import pylons
from pylons.i18n.translation import _get_translator
from paste.deploy import loadapp
from pylons import url
from paste.script.appinstall import SetupCommand
from routes.util import URLGenerator
from webtest import TestApp

from openspending.test import helpers, TestCase, DatabaseTestCase
from openspending.ui.test import ControllerTestCase

from openspending.etl.ui.config.environment import load_environment

__all__ = [
    'environ', 'url', 'TestCase', 'DatabaseTestCase', 'ControllerTestCase'
]

environ = {}

here_dir = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
conf_dir = os.path.dirname(os.path.dirname(os.path.dirname(here_dir)))

sys.path.insert(0, conf_dir)

# Clear everything before any tests are run.
def setup_module():
    helpers.clean_all()

class LoaderTestCase(DatabaseTestCase):
    '''
    A basic TestCase class for tests that need to create data in the database.
    '''

    def _make_loader(self, dataset_name=u'test_dataset', unique_keys=['name'],
                     label=u'Test Dataset', **kwargs):
        from openspending.etl.ui.lib.loader import Loader
        loader = Loader(dataset_name, unique_keys, label, **kwargs)
        return loader

    def _make_entry(self, loader, **kwargs):
        from openspending.etl.ui.model import Entry

        entry = {'name': 'Test Entry',
                 'amount': 1000.00,
                 'time': {'from': {'year': 2009,
                                   'day': 20090101}}}
        entry.update(kwargs)

        if 'from' not in entry:
            entry['from'] = loader.create_entity(u'Test From Entity')
        if 'to' not in entry:
            entry['to'] = loader.create_entity(u'Test To Entity')

        spec = loader.create_entry(**entry)
        new = Entry.find_one(spec)
        return new

    def _make_entity(self, loader, **kwargs):
        return loader.create_entity(**kwargs)