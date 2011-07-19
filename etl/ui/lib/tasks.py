import logging

from celery.task import task

from openspending.etl.ui.lib import aggregator
from openspending.etl.ui.lib.csvimport import load_dataset as csv_load_dataset
from openspending.etl.ui.lib.cubes import Cube


@task
def load_dataset(resource_url, model, **kwargs):
    out = csv_load_dataset(resource_url, model, **kwargs)
    return out


@task
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
    logging.debug("Compute distincts collection for dataset: %s"
                  % dataset_name)
    aggregator.update_distincts(dataset_name)


@task
def update_all_cubes(dataset):
    Cube.update_all_cubes(dataset)


@task
def remove_entries(dataset_name):
    # NB: does not record changesets
    from openspending.etl.ui.model import Entry
    logging.info("Deleting all entries in dataset: %s" % dataset_name)
    errors = Entry.c.remove({"dataset.name": dataset_name})
    logging.info("Errors: %s" % errors)