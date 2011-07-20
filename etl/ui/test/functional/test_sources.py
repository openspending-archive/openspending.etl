from openspending.lib import json
from openspending.etl.ui.test import ControllerTestCase, url, helpers as h

MOCK_CKAN = json.load(h.fixture_file('mock_ckan.json'))

class TestSourcesController(ControllerTestCase):

    @h.patch('openspending.etl.ui.controllers.sources.ckan.CkanClient')
    def test_ckan_packages(self, ckan_mock):
        ckan_mock.return_value = h.mock_ckan(MOCK_CKAN)

        response = self.app.get(url(controller='sources', action='ckan_packages'))

        print response

        # Show title for packages
        assert '<a href="http://ckan.net/package/baz">The Baz dataset</a>' in response

        # Show 'import' link for importable packages
        import_url = url(controller='sources', action='import', package='bar')
        assert '<a href="%s">' % import_url in response

        # Show 'diagnose' link for non-importable packages
        diagnose_url = url(controller='sources', action='diagnose', package='baz')
        assert '<a href="%s">' % diagnose_url in response
