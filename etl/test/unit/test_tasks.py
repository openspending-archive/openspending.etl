from openspending.model import Dataset, meta as db
from openspending.etl import tasks

from .. import DatabaseTestCase, helpers as h


class TestCSVImporter(DatabaseTestCase):

    def test_remove_dataset(self):
        dataset = Dataset({'dataset': {'name':'mydataset'}})
        db.session.add(dataset)
        db.session.commit()
        tasks.remove_dataset('mydataset')
        res = db.session.query(Dataset).filter_by(name='mydataset').first()
        h.assert_equal(res, None)
