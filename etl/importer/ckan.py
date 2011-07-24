from openspending.lib import ckan
from openspending.lib import json

from openspending.etl import util
from openspending.etl.mappingimporter import MappingImporter
from openspending.etl.importer.csv import CSVImporter

class CKANImporter(CSVImporter):
    def __init__(self, package,
                 model_url=None, mapping_url=None, resource_uuid=None):

        if not isinstance(package, ckan.Package):
            package = ckan.Package(package)

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
