from openspending.test.helpers import *
import pkg_resources as _pkg_resources

# Fixture helpers.
#
# These are redefined here, so that we load the fixtures for the
# openspending.etl package, rather than the openspending package.

def _fixture_relpath(name):
    return 'fixtures/%s' % name

def fixture_file(name):
    """Return a file-like object pointing to a named fixture."""
    return _pkg_resources.resource_stream(__name__, _fixture_relpath(name))

def fixture_path(name):
    """
    Return the full path to a named fixture.

    Use fixture_file rather than this method wherever possible.
    """
    return _pkg_resources.resource_filename(__name__, _fixture_relpath(name))

def fixture_listdir(name):
    """Return a directory listing for the named fixture."""
    return _pkg_resources.resource_listdir(__name__, _fixture_relpath(name))
