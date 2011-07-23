import time

from openspending.model import mongo
from openspending.etl.command import daemon
from openspending.etl.ui.test import ControllerTestCase, url, helpers as h

class TestHomeController(ControllerTestCase):
    def test_index_load_dataset_link(self):
        response = self.app.get(url(controller='home', action='index'))

        load_url = url(controller='load', action='index')
        assert load_url in response, \
            "No link to LoadController index action in response!"

    def test_index_drop_database_link(self):
        response = self.app.get(url(controller='home', action='index'))

        dropdb_url = url(controller='home', action='drop_database')
        assert dropdb_url in response, \
            "No link to drop_database action in response!"

    @h.patch('openspending.ui.lib.authz.have_role')
    def test_drop_database(self, have_role_mock):
        have_role_mock.return_value = True

        db = mongo.db()
        db.test_collection.insert({"name": "test thingy"})

        response = self.app.get(url(controller='home', action='drop_database'))

        assert daemon.job_running('drop_database'), \
            "drop_database action didn't start the drop_database job!"

        while daemon.job_running('drop_database'):
            time.sleep(0.1)

        h.assert_equal(db.collection_names(), ['system.js', 'system.indexes'])
