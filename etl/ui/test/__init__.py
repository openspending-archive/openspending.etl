"""\
OpenSpending ETL UI test module
===============================

Run the OpenSpending test suite by running

    nosetests

in the root of the repository, while in an active virtualenv. See
doc/install.rst for more information.
"""

from paste.script.appinstall import SetupCommand
from pylons import url, config
from routes.util import URLGenerator
from webtest import TestApp
import pylons.test

from helpers import clean_all

__all__ = [
    'environ', 'url', 'TestCase', 'DatabaseTestCase', 'ControllerTestCase'
]

# Invoke websetup with the current config file
SetupCommand('setup-app').run([config['__file__']])

environ = {}

# Clear everything before any tests are run.
def setup_module():
    clean_all()

class TestCase(object):
    def setup(self):
        pass

    def teardown(self):
        pass

class DatabaseTestCase(TestCase):
    def teardown(self):
        clean_all()
        super(DatabaseTestCase, self).teardown()

class ControllerTestCase(DatabaseTestCase):
    def __init__(self, *args, **kwargs):
        wsgiapp = pylons.test.pylonsapp
        self.app = TestApp(wsgiapp)
        url._push_object(URLGenerator(config['routes.map'], environ))
        super(DatabaseTestCase, self).__init__(*args, **kwargs)

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
        from openspending.model import Entry

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