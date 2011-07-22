from openspending.lib import ckan
from openspending.lib import json
from openspending.etl.command import daemon
from openspending.etl.ui.test import ControllerTestCase, url, helpers as h

MOCK_REGISTRY = json.load(h.fixture_file('mock_ckan.json'))

class TestLoadController(ControllerTestCase):
    def setup(self):
        super(TestLoadController, self).setup()
        self.patcher = h.patch('openspending.etl.ui.controllers.load.ckan.CkanClient',
                               spec=ckan.CkanClient)
        self.MockCkanClient = self.patcher.start()
        self.MockCkanClient.return_value = self.c = h.mock_ckan(MOCK_REGISTRY)

    def teardown(self):
        self.patcher.stop()
        super(TestLoadController, self).teardown()

    def test_packages(self):
        response = self.app.get(url(controller='load', action='packages'))

        # Show title for packages
        assert '<a href="http://ckan.net/package/baz">The Baz dataset</a>' in response

        # Show 'import' link for importable packages
        import_url = url(controller='load', action='start', package='bar')
        assert '<a href="%s">' % import_url in response

        # Show 'diagnose' link for non-importable packages
        diagnose_url = url(controller='load', action='diagnose', package='baz')
        assert '<a href="%s">' % diagnose_url in response

    def test_diagnose_valid_package(self):
        response = self.app.get(url(controller='load',
                                    action='diagnose',
                                    package='bar'))

        assert 'http://example.com/barmodel.js' in response, \
            "No link to package model in response!"

        assert 'http://example.com/bardata.csv' in response, \
            "No link to package data in response!"

        assert 'error-message' not in response, \
            "There was an error-message in response"

    def test_diagnose_broken_package_no_hints(self):
        response = self.app.get(url(controller='load',
                                    action='diagnose',
                                    package='foo'))

        assert 'None set' in response, "'None set' not in response!"

    def test_diagnose_broken_package_ambiguous_hints(self):
        response = self.app.get(url(controller='load',
                                    action='diagnose',
                                    package='baz'))

        assert "multiple resources with hint &#39;model&#39;" in response, \
            "No warning about ambiguous resources in response!"

    @h.patch('openspending.ui.lib.authz.have_role')
    def test_start(self, have_role_mock):
        have_role_mock.return_value = True # Pretend to be admin user.

        response = self.app.get(url(controller='load',
                                    action='start',
                                    package='bar'))

        # Redirects to status page
        status_path = url(controller='load', action='status', package='bar')
        assert response.headers['Location'].endswith(status_path), \
            "LoadController start action didn't redirect to status page."

        assert daemon.job_running('import_bar'), \
            "LoadController start action didn't start the import_bar job!"