from .. import ControllerTestCase, url, helpers as h

class TestHomeController(ControllerTestCase):
    def test_index_csv_import_link(self):
        response = self.app.get(url(controller='home', action='index'))

        load_url = url(controller='load', action='index')
        assert load_url in response, \
            "No link to LoadController#index action in response!"

    def test_index_drop_database_link(self):
        response = self.app.get(url(controller='home', action='index'))

        dropdb_url = url(controller='task', action='drop_database')
        assert dropdb_url in response, \
            "No link to TaskController#drop_database action in response!"

    def test_index_remove_dataset_link(self):
        response = self.app.get(url(controller='home', action='index'))

        rmdataset_url = url(controller='task', action='remove_dataset')
        assert rmdataset_url in response, \
            "No link to TaskController#remove_dataset action in response!"


