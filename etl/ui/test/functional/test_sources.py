from openspending.lib import ckan
from openspending.lib import json
from openspending.etl.ui.test import ControllerTestCase, url, helpers as h

MOCK_REGISTRY = json.load(h.fixture_file('mock_ckan.json'))

class TestSourcesController(ControllerTestCase):
    def setup(self):
        super(TestSourcesController, self).setup()
        self.patcher = h.patch('openspending.etl.ui.controllers.sources.ckan.CkanClient',
                               spec=ckan.CkanClient)
        self.MockCkanClient = self.patcher.start()
        self.MockCkanClient.return_value = self.c = h.mock_ckan(MOCK_REGISTRY)

    def teardown(self):
        self.patcher.stop()
        super(TestSourcesController, self).teardown()

    def test_ckan_packages(self):
        response = self.app.get(url(controller='sources', action='ckan_packages'))

        # Show title for packages
        assert '<a href="http://ckan.net/package/baz">The Baz dataset</a>' in response

        # Show 'import' link for importable packages
        import_url = url(controller='sources', action='import', package='bar')
        assert '<a href="%s">' % import_url in response

        # Show 'diagnose' link for non-importable packages
        diagnose_url = url(controller='sources', action='diagnose', package='baz')
        assert '<a href="%s">' % diagnose_url in response

    def test_diagnose_valid_package(self):
        response = self.app.get(url(controller='sources',
                                    action='diagnose',
                                    package='bar'))

        assert 'http://example.com/barmodel.js' in response, \
            "No link to package model in response!"

        assert 'http://example.com/bardata.csv' in response, \
            "No link to package data in response!"

        assert 'error-message' not in response, \
            "There was an error-message in response"

    def test_diagnose_broken_package_no_hints(self):
        response = self.app.get(url(controller='sources',
                                    action='diagnose',
                                    package='foo'))

        assert 'None set' in response, "'None set' not in response!"

    def test_diagnose_broken_package_ambiguous_hints(self):
        response = self.app.get(url(controller='sources',
                                    action='diagnose',
                                    package='baz'))

        assert "multiple resources with hint &#39;model&#39;" in response, \
            "No warning about ambiguous resources in response!"