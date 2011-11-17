from ckanclient import CkanClient

from openspending.lib import json

from openspending.etl import util
from openspending.etl.importer.csv import CSVImporter
from openspending.etl.importer.base import ImporterError

openspending_group = 'openspending'
base_location = 'http://thedatahub.org/api'
api_key = None

_client = None

class LineImportError(ImporterError):
    def __init__(self, field, exc):
        self.field = field
        self.exc = exc

    def __str__(self):
        return "Column `%s': %s" % (self.field, repr(self.exc))

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

    if not _client:
        _client = make_client()

    return _client

def openspending_packages():
    client = get_client()

    resp = client.package_search('', {'groups': openspending_group,
                                      'all_fields': '1',
                                      'limit': 9999}) # Sorry, but the CKAN API doesn't
                                                      # provide for anything more elegant...

    return [Package(x['name'], from_dict=x) for x in resp['results']]

class ResourceError(Exception):
    pass

class AmbiguousResourceError(ResourceError):
    pass

class MissingResourceError(ResourceError):
    pass

class Package(object):
    def __init__(self, name, from_dict=None):
        if from_dict:
            data = from_dict
        else:
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
            data = self.openspending_resource('data')
        except AmbiguousResourceError:
            return False
        else:
            importable = (data and model) 
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

    def to_json(self):
        return json.dumps(self.data)

    def add_hint(self, resource_uuid, hint):
        r = self.get_resource(resource_uuid)
        r['openspending_hint'] = hint

        client = get_client()
        client.package_entity_put(self.data)

    def remove_hint(self, resource_uuid):
        self.add_hint(resource_uuid, '')

class CKANImporter(CSVImporter):
    def __init__(self, package,
                 model_url=None, resource_uuid=None):

        if not isinstance(package, Package):
            package = Package(package)

        if resource_uuid:
            data = package.get_resource(resource_uuid)
        else:
            data = package.openspending_resource('data')

        if not model_url:
            # Use magic CKAN tags
            model = package.openspending_resource('model')
            model_url = model['url']


        model_fp = util.urlopen(model_url)
        try:
            model = json.load(model_fp)
        except Exception as e:
            raise ImporterError("Error encountered while parsing JSON model. "
                                "http://jsonlint.com might help! Error was: %s"
                                % e)


        csv = util.urlopen_lines(data["url"])
        super(CKANImporter, self).__init__(csv, model, data["url"])
