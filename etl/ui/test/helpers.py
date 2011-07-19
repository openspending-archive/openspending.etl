from nose.tools import *
from mock import Mock, patch

def load_fixture(name):
    """
    Load fixture data and appropriate mappings into the database.

    Returns the corresponding Dataset.
    """
    from openspending.etl.ui import model

    raise NotImplementedError("Generic CSV loading for tests not yet implemented.")

def fixture_file(name):
    """Return a file-like object pointing to a named fixture."""
    import pkg_resources
    path_in_pkg = 'tests/fixtures/%s' % (name)
    return pkg_resources.resource_stream('openspending.etl.ui', path_in_pkg)

def fixture_path(name):
    """
    Return the full path to a named fixture.

    Use fixture_file rather than this method wherever possible.
    """
    import pkg_resources
    path_in_pkg = 'tests/fixtures/%s' % (name)
    return pkg_resources.resource_filename('openspending.etl.ui', path_in_pkg)

def clean_all():
    clean_db()
    clean_solr()

def clean_db():
    from openspending.etl.ui.model import mongo
    mongo.drop_collections()

def clean_solr():
    '''Clean all entries from Solr.'''
    from openspending.etl.ui.lib.solr import get_connection
    solr = get_connection()
    solr.delete_query('*:*')
    solr.commit()

def clean_and_reindex_solr():
    '''Clean Solr and reindex all entries in the database.'''
    clean_solr()
    from openspending.etl.ui.lib.solr import build_index
    from openspending.etl.ui.model import Dataset
    dataset_names = Dataset.c.distinct('name')
    for name in dataset_names:
        build_index(name)

def skip_if_stubbed_solr():
    from openspending.etl.ui.lib.solr import get_connection, _Stub
    if type(get_connection()) == _Stub:
        skip("Not running test with stubbed Solr.")

def skip(*args, **kwargs):
    from nose.plugins.skip import SkipTest
    raise SkipTest(*args, **kwargs)

def mock_ckan(registry):
    '''
    Return a mock CKANClient that can be monkeypatched into the code while
    testing.
    '''
    class MockCKANClient(object):
        pass

    ckan = MockCKANClient()

    def mock_group_entity_get(name, *args, **kwargs):
        def in_group(p):
            return name in registry[p].get('groups', [])

        packages = filter(in_group, registry.keys())

        return {'packages': packages}

    def mock_package_entity_get(name, *args, **kwargs):
        return registry[name]

    ckan.group_entity_get = Mock(side_effect=mock_group_entity_get)
    ckan.package_entity_get = Mock(side_effect=mock_package_entity_get)

    return ckan