from openspending.etl.ui.test import ControllerTestCase, url, helpers as h

MOCK_CKAN = {
    'foo': {
        'name': 'foo',
        'title': 'Foo',
        'groups': ['openspending'],
        'ckan_url': 'http://ckan.net/package/foo',
        'description': 'The Foo package.',
        'resources': [
            {'id': 123, 'position': 0, 'description': 'Foo resource 0', 'format': 'CSV'}
        ]
    },
    'bar': {
        'name': 'bar',
        'title': 'Bar',
        'groups': ['openspending'],
        'ckan_url': 'http://ckan.net/package/bar',
        'description': 'The Bar package.',
    }
}

class TestSourcesController(ControllerTestCase):

    @h.patch('openspending.etl.ui.controllers.sources.ckan.CkanClient')
    def test_index(self, ckan_mock):
        ckan_mock.return_value = h.mock_ckan(MOCK_CKAN)

        response = self.app.get(url(controller='sources', action='index'))

        # Show title for packages with resources
        assert '<a href="http://ckan.net/package/foo">Foo</a>' in response
        # But don't for those without
        assert 'http://ckan.net/package/bar' not in response

        # Show resource details
        assert 'Foo resource 0' in response

        # Show 'import' link for resources
        import_resource_url = url(controller='sources', action='mapping_form',
                                  package='foo', resource=123)
        assert '<a href="%s">import</a>' % import_resource_url in response
