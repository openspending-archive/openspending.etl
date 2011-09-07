from openspending.etl.importer import ckan
from openspending.lib import json
from openspending.etl.test import TestCase, helpers as h

MOCK_REGISTRY = json.load(h.fixture_file('mock_ckan.json'))

class TestCkan(TestCase):
    def setup(self):
        super(TestCkan, self).setup()
        self.patcher = h.patch('openspending.etl.importer.ckan.CkanClient', spec=ckan.CkanClient)
        self.MockCkanClient = self.patcher.start()
        ckan._client = None
        self.MockCkanClient.return_value = self.c = h.mock_ckan(MOCK_REGISTRY)

    def teardown(self):
        self.patcher.stop()
        super(TestCkan, self).teardown()

    def test_make_client(self):
        h.assert_equal(ckan.make_client(), self.c)

        self.MockCkanClient.return_value = None
        h.assert_equal(ckan.make_client(), None)

    def test_get_client(self):
        h.assert_equal(ckan.get_client(), self.c)

        # singleton now created, so this should have no effect.
        self.MockCkanClient.return_value = None
        h.assert_equal(ckan.get_client(), self.c)

    def test_package_init(self):
        p = ckan.Package('foo')
        h.assert_true(isinstance(p, ckan.Package))

    def test_package_init_from_dict(self):
        p = ckan.Package('foo', from_dict={'name': 'foo', 'bar': 123})
        h.assert_equal(p.data, {'name': 'foo', 'bar': 123})

    def test_package_getitem(self):
        p = ckan.Package('foo')
        h.assert_equal(p['name'], 'foo')
        h.assert_equal(p['id'], '123')

    def test_package_openspending_resource(self):
        p = ckan.Package('bar')
        h.assert_equal(p.openspending_resource('model')['id'], '123-model')
        h.assert_equal(p.openspending_resource('data')['id'], '456-data')
        h.assert_equal(p.openspending_resource('foobar'), None)

    @h.raises(ckan.AmbiguousResourceError)
    def test_package_openspending_resource_ambiguous(self):
        p = ckan.Package('baz')
        p.openspending_resource('model')

    def test_package_is_importable(self):
        p = ckan.Package
        h.assert_equal(p('foo').is_importable(), False)
        h.assert_equal(p('bar').is_importable(), True)
        h.assert_equal(p('baz').is_importable(), False)
        h.assert_equal(p('missingdata').is_importable(), False)
        h.assert_equal(p('withmapping').is_importable(), True)

    def test_package_to_json(self):
        p = ckan.Package('foo', from_dict={'name': 'foo', 'bar': 123})
        h.assert_equal(p.to_json(), '{"bar": 123, "name": "foo"}')

    def test_package_add_hint(self):
        p = ckan.Package('foo')
        p.add_hint('1', 'my_hint')
        h.assert_equal(p.data['resources'][0]['openspending_hint'], 'my_hint')
        h.assert_true(self.c.package_entity_put.called)

    def test_package_remove_hint(self):
        p = ckan.Package('bar')
        p.remove_hint('123-model')
        h.assert_equal(p.data['resources'][0]['openspending_hint'], '')
        h.assert_true(self.c.package_entity_put.called)

    def test_metadata_for_resource(self):
        p = ckan.Package('bar')
        r = p['resources'][1]
        print p.metadata_for_resource(r)
        h.assert_equal(p.metadata_for_resource(r), {
            'source_description': u'Some bar data',
            'description': u'Notes for bar',
            'temporal_granularity': 'year',
            u'ckan_url': u'http://ckan.net/package/bar',
            'source_url': u'http://example.com/bardata.csv',
            'currency': 'usd',
            'source_format': u'text/csv',
            'source_id': u'456-data',
            'label': u'The Bar dataset',
            u'name': u'bar'
        })

    def test_get_resource(self):
        p = ckan.Package('bar')
        h.assert_equal(p.get_resource('456-data')['url'], 'http://example.com/data.csv')

    @h.raises(ckan.MissingResourceError)
    def test_get_resource(self):
        p = ckan.Package('bar')
        p.get_resource('not-there')

