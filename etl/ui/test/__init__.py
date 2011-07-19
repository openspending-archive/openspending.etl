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

import helpers as h

from openspending.etl.ui.config.environment import load_environment

__all__ = [
    'environ', 'url', 'TestCase', 'DatabaseTestCase', 'ControllerTestCase'
]

environ = {}

here_dir = os.path.dirname(os.path.abspath(__file__))
conf_dir = os.path.dirname(os.path.dirname(os.path.dirname(here_dir)))

sys.path.insert(0, conf_dir)

# Clear everything before any tests are run.
def setup_module():
    h.clean_all()

class TestCase(object):
    def setup(self):
        pass

    def teardown(self):
        # Remove any mocked CkanClients
        from openspending.etl.ui.lib import ckan
        ckan._client = None

class DatabaseTestCase(TestCase):
    def teardown(self):
        h.clean_all()
        super(DatabaseTestCase, self).teardown()

class ControllerTestCase(DatabaseTestCase):
    def __init__(self, *args, **kwargs):
        wsgiapp = loadapp('config:test.ini', relative_to=conf_dir)
        config = wsgiapp.config
        pylons.app_globals._push_object(config['pylons.app_globals'])
        pylons.config._push_object(config)

        # Initialize a translator for tests that utilize i18n
        translator = _get_translator(pylons.config.get('lang'))
        pylons.translator._push_object(translator)

        url._push_object(URLGenerator(config['routes.map'], environ))
        self.app = TestApp(wsgiapp)
        super(ControllerTestCase, self).__init__(*args, **kwargs)

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