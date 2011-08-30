from ckanclient import CkanClient, CkanApiError

from openspending.lib import json

from openspending.etl import util
from openspending.etl.mappingimporter import MappingImporter
from openspending.etl.importer.csv import CSVImporter

openspending_group = 'openspending'
base_location = 'http://ckan.net/api'
api_key = None

_client = None

def configure(config=None):
    global openspending_group
    global base_location
    global api_key

    if not config:
        config = {}

    openspending_group = config.get('openspending.ckan_group', openspending_group)
    base_location = config.get('openspending.ckan_location', base_location)
    api_key = config.get('openspending.ckan_api_key', api_key)

def make_client():
    return CkanClient(base_location=base_location, api_key=api_key)

def get_client():
    global _client

    if _client:
        return _client
    else:
        _client = make_client()
        return _client

def openspending_packages():
    client = get_client()
    group = client.group_entity_get(openspending_group)

    return [Package(name) for name in group.get('packages')]

class ResourceError(Exception):
    pass

class AmbiguousResourceError(ResourceError):
    pass

class MissingResourceError(ResourceError):
    pass

class Package(object):
    def __init__(self, name):
        client = get_client()
        data = client.package_entity_get(name)
        self.name = data['name']
        self.data = data

    def __getitem__(self, k):
        return self.data[k]

    def __str__(self):
        return '<%s "%s">' % (self.__class__.__name__, self.name)

    def __repr__(self):
        return '<%s "%s" at %s>' % (self.__class__.__name__, self.name, hex(id(self)))

    def get_resource(self, id):
        for r in self['resources']:
            if r['id'] == id:
                return r

        raise MissingResourceError(
            "Resource with id '%s' not found in %s"
            % (id, self)
        )

    def openspending_resource(self, hint):
        def has_hint(r):
            return r.get('openspending_hint') == hint

        with_hint = filter(has_hint, self['resources'])

        if len(with_hint) == 1:
            return with_hint[0]
        elif len(with_hint) == 0:
            return None
        else:
            raise AmbiguousResourceError(
                "%s has multiple resources with hint '%s'" % (self, hint)
            )

    def is_importable(self):
        try:
            model = self.openspending_resource('model')
            mapping = self.openspending_resource('model:mapping')
            data = self.openspending_resource('data')
        except AmbiguousResourceError:
            return False
        else:
            importable = (data and (model and not mapping) or (mapping and not model))
            return bool(importable)

    def metadata_for_resource(self, resource):
        ds = self.data.copy()

        del ds['id']

        ds['label'] = ds.pop('title')
        ds['description'] = ds.pop('notes')
        ds['source_url'] = resource.get('url')
        ds['source_description'] = resource.get('description')
        ds['source_format'] = resource.get('format')
        ds['source_id'] = resource.get('id')
        extras = ds.pop('extras', {})
        ds['currency'] = extras.get('currency', 'usd')
        ds['temporal_granularity'] = extras.get('temporal_granularity',
                                                'year').lower()

        del ds['resources']
        del ds['groups']

        ds = dict([ (k, v) for k, v in ds.iteritems() ])

        return ds

class CKANImporter(CSVImporter):
    def __init__(self, package,
                 model_url=None, mapping_url=None, resource_uuid=None):

        if not isinstance(package, Package):
            package = Package(package)

        if resource_uuid:
            data = package.get_resource(resource_uuid)
        else:
            data = package.openspending_resource('data')

        explicit = (model_url and not mapping_url) or (mapping_url and not model_url)

        if not explicit:
            # Use magic CKAN tags
            model = package.openspending_resource('model')
            mapping = package.openspending_resource('model:mapping')

            if model:
                model_url = model['url']
            elif mapping:
                mapping_url = mapping['url']


        # Model given
        if model_url and data:
            model = json.load(util.urlopen(model_url))

        # Mapping given, need to extract metadata from CKAN
        elif mapping_url and data:
            model = {}
            model['mapping'] = MappingImporter().import_from_url(mapping_url)
            model['dataset'] = package.metadata_for_resource(data)

        csv = util.urlopen_lines(data["url"])
        super(CKANImporter, self).__init__(csv, model, data["url"])
