from openspending.model import Dataset, meta as db
from openspending.etl import tasks

from .. import TestCase, helpers as h

@h.skip
def test_drop_datasets():
    #mongo.db.test_collection.insert({"name": "test thingy"})
    tasks.drop_datasets()
    #h.assert_equal(mongo.db.collection_names(), ['system.js', 'system.indexes'])

def test_remove_dataset():
    dataset = Dataset({'name':'mydataset'})
    db.session.add(dataset)
    db.session.commit()
    tasks.remove_dataset('mydataset')
    res = db.session.query(Dataset).filter_by(name='mydataset').first()
    h.assert_equal(res, None)
