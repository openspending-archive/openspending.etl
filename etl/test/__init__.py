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

from openspending import mongo
from openspending import model
from openspending.etl.loader import Loader
from openspending.test import TestCase, DatabaseTestCase

__all__ = ['TestCase', 'DatabaseTestCase', 'LoaderTestCase']

def setup_package():
    mongo.configure(config)

class LoaderTestCase(DatabaseTestCase):
    '''
    A basic TestCase class for tests that need to create data in the database.
    '''

    def _make_loader(self, dataset_name=u'test_dataset', unique_keys=['name'],
                     label=u'Test Dataset', **kwargs):
        loader = Loader(dataset_name, unique_keys, label, **kwargs)
        return loader

    def _make_entry(self, loader, **kwargs):
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