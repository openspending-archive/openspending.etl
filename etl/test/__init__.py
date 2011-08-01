"""\
OpenSpending ETL test module
============================

Run the OpenSpending test suite by running

    nosetests

in the root of the repository, while in an active virtualenv. See
doc/install.rst for more information.
"""

import os

from paste.deploy import appconfig

from openspending import mongo
from helpers import clean_all

__all__ = ['TestCase', 'DatabaseTestCase']

here_dir = os.getcwd()
config = appconfig('config:test.ini', relative_to=here_dir)
mongo.configure(config)

class TestCase(object):
    def setup(self):
        pass

    def teardown(self):
        pass

class DatabaseTestCase(TestCase):
    def teardown(self):
        clean_all()
        super(DatabaseTestCase, self).teardown()


class LoaderTestCase(DatabaseTestCase):
    '''
    A basic TestCase class for tests that need to create data in the database.
    '''

    def _make_loader(self, dataset_name=u'test_dataset', unique_keys=['name'],
                     label=u'Test Dataset', **kwargs):
        from openspending.etl.loader import Loader
        loader = Loader(dataset_name, unique_keys, label, **kwargs)
        return loader

    def _make_entry(self, loader, **kwargs):
        from openspending import model

        entry = {'name': 'Test Entry',
                 'amount': 1000.00,
                 'time': {'from': {'year': 2009,
                                   'day': 20090101}}}
        entry.update(kwargs)

        if 'from' not in entry:
            entry['from'] = loader.create_entity(u'Test From Entity')
        if 'to' not in entry:
            entry['to'] = loader.create_entity(u'Test To Entity')

        entry['from'] = model.entity.get_ref_dict(entry['from'])
        entry['to'] = model.entity.get_ref_dict(entry['to'])

        _id = loader.create_entry(**entry)
        new = model.entry.get(_id)
        return new

    def _make_entity(self, loader, **kwargs):
        return loader.create_entity(**kwargs)