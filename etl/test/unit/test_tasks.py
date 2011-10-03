from openspending import model
from openspending.etl import tasks

from .. import TestCase, helpers as h

def test_drop_collections():
    mongo.db.test_collection.insert({"name": "test thingy"})
    tasks.drop_collections()
    h.assert_equal(mongo.db.collection_names(), ['system.js', 'system.indexes'])

def test_remove_dataset():
    model.dataset.create({'name':'mydataset'})
    tasks.remove_dataset('mydataset')
    res = model.dataset.find_one_by('name', 'mydataset')
    h.assert_equal(res, None)
