import time

from pylons import config

from openspending import model
from openspending import mongo
from openspending.etl.command import daemon
from .. import ControllerTestCase, url, helpers as h

class TestTaskController(ControllerTestCase):

    def setup(self):
        super(TestTaskController, self).setup()
        self.patcher_authz = h.patch('openspending.ui.lib.authz.have_role')
        self.mock_authz = self.patcher_authz.start()

        self.patcher_dispatch = h.patch('openspending.etl.command.daemon.dispatch_job')
        self.mock_dispatch = self.patcher_dispatch.start()

    def teardown(self):
        self.patcher_authz.stop()
        self.patcher_dispatch.stop()
        super(TestTaskController, self).teardown()

    def test_drop_database(self):
        self.mock_authz.return_value = True

        response = self.app.get(url(controller='task', action='drop_database'))

        self.mock_dispatch.assert_called_once_with(job_id='drop_database',
                                                   config=config['__file__'],
                                                   task='drop_collections')

    def test_remove_dataset_select(self):
        self.mock_authz.return_value = True

        datasets = ['one', 'two', 'three']

        for name in datasets:
            model.dataset.create({'name': name, 'label': "Test dataset %s" % name})

        response = self.app.get(url(controller='task', action='remove_dataset'))

        for name in datasets:
            remove_url = url(controller='task', action='remove_dataset', dataset=name)
            assert remove_url in response, \
                "No link to remove dataset '%s' in response!" % name

    def test_remove_dataset(self):
        self.mock_authz.return_value = True

        model.dataset.create({'name':'mydataset'})

        response = self.app.get(url(controller='task',
                                    action='remove_dataset',
                                    dataset='mydataset'))

        self.mock_dispatch.assert_called_once_with(job_id='remove_mydataset',
                                                   config=config['__file__'],
                                                   task='remove_dataset',
                                                   args=('mydataset',))


