from openspending.etl.ui.lib import ckan, csvimport
from openspending.etl.ui.model import Dataset

class ResourceImportError(StandardError):
    pass

def package_and_resource(package_name, resource_id):
    package = ckan.get_client().package_entity_get(package_name)

    for resource in package.get('resources', []):
        if resource.get('id') == resource_id:
            return package, resource

    # Fallthrough
    raise ResourceImportError("Could not find resource '%s' in package '%s'."
                              % (resource_id, package_name))

# TODO: remove this in favour of ckan.Package and ckan.Package#metadata_for_resource
def load_from_ckan(package, resource):
    '''
    Get `Dataset` and `DatasetImporter` given package and resource dict from
    CKAN.
    '''

    ds = package.copy()
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

    ds = dict([ (str(k), v) for k, v in ds.iteritems() ])
    dataset = csvimport.Dataset(**ds)
    importer = csvimport.DatasetImporter(
        csvimport.resource_lines(resource.get('url')),
        {
            'dataset': dataset,
            'mapping': {}
        }
    )
    return dataset, importer
