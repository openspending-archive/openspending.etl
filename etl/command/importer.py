from __future__ import print_function

import argparse
import logging
import sys
import urllib2

from openspending.etl.importer import ckan
from openspending.lib import json

from openspending.etl import util
from openspending.etl.importer import CSVImporter, CKANImporter, ImporterError

log = logging.getLogger(__name__)

import_parser = argparse.ArgumentParser(add_help=False)

import_parser.add_argument('-n', '--dry-run',
                           action="store_true", dest='dry_run', default=False,
                           help="Perform a dry run, don't load any data.")

import_parser.add_argument('--no-index', action="store_false", dest='build_indices',
                           default=True, help='Suppress Solr index build.')

import_parser.add_argument('--mapping', action="store", dest='mapping',
                           default=None, metavar='URL',
                           help="URL of JSON format mapping.")

import_parser.add_argument('--max-errors', action="store", dest='max_errors',
                           type=int, default=None, metavar='N',
                           help="Maximum number of import errors to tolerate before giving up.")

import_parser.add_argument('--max-lines', action="store", dest='max_lines',
                           type=int, default=None, metavar='N',
                           help="Number of lines to import.")

import_parser.add_argument('--raise-on-error', action="store_true",
                           dest='raise_errors', default=False,
                           help='Get full traceback on first error.')

def csvimport(csv_data_url, args):

    def json_of_url(url):
        return json.load(urllib2.urlopen(url))

    have_model = args.model or (args.mapping and args.metadata)

    if not have_model:
        print("You must provide --model OR (--mapping AND --metadata)!",
              file=sys.stderr)
        return 1

    if args.model:
        model = json_of_url(args.model)
    else:
        model = {}

        from openspending.ui.lib.mappingimporter import MappingImporter
        mi = MappingImporter()
        model["mapping"] = mi.import_from_url(args.mapping)
        model["dataset"] = json_of_url(args.metadata)

    csv = util.urlopen_lines(csv_data_url)
    importer = CSVImporter(csv, model, csv_data_url)

    importer.run(**vars(args))
    return 0

def ckanimport(package_name, args):
    package = ckan.Package(package_name)

    if not args.use_ckan_tags:
        if not args.mapping:
            print("You must specify metadata mapping URL (--mapping)!", file=sys.stderr)
            return 1
        if not args.resource:
            print("You must specify the resource UUID (--resource)!", file=sys.stderr)
            return 1

    if args.use_ckan_tags:
        importer = CKANImporter(package)
    else:
        importer = CKANImporter(package,
                                args.mapping,
                                args.resource)

    importer.run(**vars(args))
    return 0

def importreport():
    print("-- Finding OpenSpending packages on CKAN...", file=sys.stderr)

    packages = [ p for p in ckan.openspending_packages() ]
    packages = filter(lambda x: x.is_importable(), packages)

    kwargs = {
        'build_indices': False,
        'max_lines': 30,
        'max_errors': 1
    }

    def first_error(package):
        try:
            importer = CKANImporter(package)
            importer.run(**kwargs)
            # If we reach the next line, no errors have been thrown, as
            # max_errors is set to 1.
            return "No errors"
        except ImporterError as e:
            return e

    import_errors = {}

    for p in packages:
        print("-- Starting import of '%s'" % p.name, file=sys.stderr)
        import_errors[p.name] = str(first_error(p))

    print("-- Results:", file=sys.stderr)
    for name, err in import_errors.iteritems():
        indented_err = "\n".join("  " +l for l in err.split("\n"))
        print("%s:\n%s" % (name, indented_err), file=sys.stderr)
    return 0

def _importreport(args):
    return importreport()

def _csvimport(args):
    return csvimport(args.dataset_url, args)

def _ckanimport(args):
    return ckanimport(args.pkgname, args)

def configure_parser(subparser):
    p = subparser.add_parser('importreport',
                             help='Report on errors from all known datasets')
    p.set_defaults(func=_importreport)

    p = subparser.add_parser('csvimport',
                             help='Load a CSV dataset',
                             description='You must specify one of --model OR (--mapping AND --metadata).',
                             parents=[import_parser])
    p.add_argument('--model', action="store", dest='model',
                   default=None, metavar='url',
                   help="URL of JSON format model (metadata and mapping).")
    p.add_argument('--metadata', action="store", dest='metadata',
                   default=None, metavar='URL',
                   help="URL of JSON format metadata.")
    p.add_argument('dataset_url', help="Dataset file URL")
    p.set_defaults(func=_csvimport)

    p = subparser.add_parser('ckanimport',
                             help='Load a dataset from CKAN',
                             parents=[import_parser])
    p.add_argument('--resource', action="store",
                   dest='resource', default=None, metavar="UUID",
                   help="UUID of CKAN resource containing entry data.")
    p.add_argument('--use-ckan-tags', action="store_true",
                   dest='use_ckan_tags', default=True,
                   help="Use CKAN to find resource UUID and mapping URL.")
    p.add_argument('pkgname', help="CKAN package name")
    p.set_defaults(func=_ckanimport)
