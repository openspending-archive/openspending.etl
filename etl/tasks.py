import sys
import logging
log = logging.getLogger(__name__)

def ckan_import(package_name):
    from openspending.lib import ckan
    from openspending.etl.ckan_import import ckan_import

    package = ckan.Package(package_name)
    ckan_import(package)

def csv_import(resource_url, model, **kwargs):
    from openspending.etl import csv_import

    out = csv_import.load_dataset(resource_url, model, **kwargs)
    return out

def remove_dataset(dataset_name):
    log.warn("Dropping dataset '%s'", dataset_name)

    from openspending.model import mongo
    db = mongo.db()

    log.info("Removing entries for dataset %s", dataset_name)
    db.entry.remove({'dataset.name': dataset_name})

    log.info("Removing dimensions for dataset %s", dataset_name)
    db.dimension.remove({'dataset': dataset_name})

    log.info("Removing distincts for dataset %s", dataset_name)
    db['distincts__%s' % dataset_name].drop()

    log.info("Removing cubes for dataset %s", dataset_name)
    cubes = filter(lambda x: x.startswith('cubes.%s.' % dataset_name),
                   db.collection_names())
    for c in cubes:
        db[c].drop()

    log.info("Removing dataset object for dataset %s", dataset_name)
    db.dataset.remove({'name': dataset_name})

def drop_collections():
    from openspending.model.mongo import drop_collections
    log.info("Dropping all collections in database...")
    drop_collections()
    log.info("Done!")

# What follow are helper tasks for testing the etl.command.daemon module.

def test_noop():
    pass

def test_stdout():
    print "Text to standard out"

def test_stderr():
    print >>sys.stderr, "Text to standard error"

def test_args(*args):
    print args