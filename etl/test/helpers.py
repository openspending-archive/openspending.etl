from openspending.test.helpers import *

import os as _os

TEST_ROOT = _os.path.dirname(__file__)

# We redefine the fixture helpers here so they load from the correct
# fixtures directory, and don't attempt to load OpenSpending fixtures.

def load_fixture(name):
    """
    Load fixture data into the database.
    """
    _pymongodump.restore(_mongo.db, fixture_path('%s.pickle' % name), drop=False)

def fixture_file(name):
    """Return a file-like object pointing to a named fixture."""
    return open(fixture_path(name))

def fixture_path(name):
    """
    Return the full path to a named fixture.

    Use fixture_file rather than this method wherever possible.
    """
    return _os.path.join(TEST_ROOT, 'fixtures', name)

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

    def mock_package_entity_put(data):
        registry[data['name']] = data

    def mock_package_search(*args, **kwargs):
        res = mock_group_entity_get('openspending')
        packages = map(lambda x: mock_package_entity_get(x), res['packages'])
        return {'results': packages}

    ckan.group_entity_get = Mock(side_effect=mock_group_entity_get)
    ckan.package_entity_get = Mock(side_effect=mock_package_entity_get)
    ckan.package_entity_put = Mock(side_effect=mock_package_entity_put)
    ckan.package_search = Mock(side_effect=mock_package_search)

    return ckan
