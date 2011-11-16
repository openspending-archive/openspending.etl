import sys
import logging
log = logging.getLogger(__name__)

def ckan_import(package_name, **kwargs):
    from openspending.etl.importer import CKANImporter

    importer = CKANImporter(package_name)
    importer.on_error = lambda e: log.warn(e)

    opts = {
        'max_errors': 500
    }

    opts.update(kwargs)

    importer.run(**opts)

def csv_import(resource_url, model_url, **kwargs):
    import urllib
    from openspending.lib import json
    from openspending.etl import util
    from openspending.etl.importer import CSVImporter

    model = json.load(urllib.urlopen(model_url))
    csv = util.urlopen_lines(resource_url)
    importer = CSVImporter(csv, model, resource_url)

    importer.run(**kwargs)

def remove_dataset(dataset_name):
    log.warn("Dropping dataset '%s'", dataset_name)

    from openspending import mongo
    db = mongo.db

    log.info("Removing entries")
    db.entry.remove({'dataset.name': dataset_name})

    log.info("Removing dimensions")
    db.dimension.remove({'dataset': dataset_name})

    log.info("Removing distincts")
    db['distincts__%s' % dataset_name].drop()

    log.info("Removing cubes")
    cubes = filter(lambda x: x.startswith('cubes.%s.' % dataset_name),
                   db.collection_names())
    for c in cubes:
        db[c].drop()

    log.info("Removing dataset object for dataset %s", dataset_name)
    db.dataset.remove({'name': dataset_name})

def drop_datasets():
    from openspending.model import Dataset, meta as db
    log.info("Dropping all datasets in database...")
    for dataset in db.session.query(Dataset):
        dataset.drop()
    log.info("Done!")

# What follow are helper tasks for testing the etl.command.daemon module.

def test_noop():
    pass

def test_sleep(seconds):
    import time
    time.sleep(int(seconds))

def test_stdout():
    print "Text to standard out"

def test_stderr():
    print >>sys.stderr, "Text to standard error"

def test_args(*args):
    print args
