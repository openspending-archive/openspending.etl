import logging
log = logging.getLogger(__name__)

from openspending.lib import aggregator
from openspending.lib.cubes import Cube

from openspending.etl.csvimport import load_dataset as csv_load_dataset

def noop():
    pass

def longrunner():
    import time
    for i in range(10):
        log.info("%d", i)
        time.sleep(1)

def load_dataset(resource_url, model, **kwargs):
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

