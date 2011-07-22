import sys
import logging
log = logging.getLogger(__name__)

from openspending.lib import aggregator
from openspending.lib.cubes import Cube

from openspending.etl.csvimport import load_dataset as csv_load_dataset

def ckan_import(package_name):
    from openspending.lib import ckan
    from openspending.etl.ckan_import import ckan_import

    package = ckan.Package(package_name)
    ckan_import(package, progress_callback=lambda x: log.info(x))

def csv_import(resource_url, model, **kwargs):
    out = csv_load_dataset(resource_url, model, **kwargs)
    return out

def update_distincts(dataset_name):
    '''
    update the collection for all distinct values in the entries in
    a dataset *dataset_name*

    ``dataset_name``
        Name of a dataset

    Returns: None
    Raises: :exc:`pymongo.errors.OperationFailure` if the dataset does
    not exist.
    '''
    log.debug("Compute distincts collection for dataset: %s"
              % dataset_name)
    aggregator.update_distincts(dataset_name)


def update_all_cubes(dataset):
    Cube.update_all_cubes(dataset)

def remove_entries(dataset_name):
    # NB: does not record changesets
    from openspending.etl.ui.model import Entry
    log.info("Deleting all entries in dataset: %s" % dataset_name)
    errors = Entry.c.remove({"dataset.name": dataset_name})
    log.info("Errors: %s" % errors)

# What follow are helper tasks for testing the etl.command.daemon module.

def test_noop():
    pass

def test_stdout():
    print "Text to standard out"

def test_stderr():
    print >>sys.stderr, "Text to standard error"

def test_args(*args):
    print args