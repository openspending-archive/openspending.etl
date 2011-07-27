import time

from openspending import model
from openspending import mongo
from openspending.etl.command import daemon
from openspending.etl.ui.test import ControllerTestCase, url, helpers as h

class TestTaskController(ControllerTestCase):

    @h.patch('openspending.ui.lib.authz.have_role')
    def test_drop_database(self, have_role_mock):
        have_role_mock.return_value = True

        mongo.db.test_collection.insert({"name": "test thingy"})

        response = self.app.get(url(controller='task', action='drop_database'))

        assert daemon.job_running('drop_database'), \
            "drop_database action didn't start the drop_database job!"

        while daemon.job_running('drop_database'):
            time.sleep(0.1)

        h.assert_equal(mongo.db.collection_names(), ['system.js', 'system.indexes'])


    @h.patch('openspending.ui.lib.authz.have_role')
    def test_remove_dataset_select(self, have_role_mock):
        have_role_mock.return_value = True

        datasets = ['one', 'two', 'three']

        for name in datasets:
            ds = model.Dataset(name=name)
            ds.save()

        response = self.app.get(url(controller='task', action='remove_dataset'))

        for name in datasets:
            remove_url = url(controller='task', action='remove_dataset', dataset=name)
            assert remove_url in response, \
                "No link to remove dataset '%s' in response!" % name

    @h.patch('openspending.ui.lib.authz.have_role')
    def test_remove_dataset(self, have_role_mock):
        have_role_mock.return_value = True

        ds = model.Dataset(name='mydataset')
        ds.save()

        response = self.app.get(url(controller='task',
                                    action='remove_dataset',
                                    dataset='mydataset'))

        assert daemon.job_running('remove_mydataset'), \
            "remove_dataset action didn't start the remove_mydataset job!"

        while daemon.job_running('remove_mydataset'):
            time.sleep(0.1)

        res = model.Dataset.find_one({'name': 'mydataset'})
        h.assert_equal(res, None)

