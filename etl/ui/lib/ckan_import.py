from urllib2 import urlopen

from openspending.lib import ckan, json
from openspending.model import Model
from openspending.logic.model import save_model
from openspending.etl.ui.lib.mappingimporter import MappingImporter
from openspending.etl.ui.lib.resourceimport import package_and_resource, load_from_ckan
from openspending.etl.ui.lib.csvimport import load_dataset

class _FakeAccount(object):
    def __init__(self, name): self.name = name

def ckan_import(package, model_url=None, mapping_url=None,
                resource_uuid=None, **kwargs):

    if resource_uuid:
        print resource_uuid
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

    account = _FakeAccount('admin-cli')

    # Model given
    if model_url and data:
        model = json.load(urlopen(model_url))
        model_id = save_model(account, **model)

    # Mapping given, need to extract metadata from CKAN
    elif mapping_url and data:
        mapping = MappingImporter().import_from_url(mapping_url)
        dataset = package.metadata_for_resource(data)
        model_id = save_model(account, dataset=dataset, mapping=mapping)

    model = Model.by_id(model_id)

    _, _, errors = load_dataset(data["url"], model, **kwargs)
    return errors
