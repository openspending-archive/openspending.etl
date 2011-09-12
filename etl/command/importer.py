from __future__ import print_function

import sys
import logging
import urllib2

from openspending.etl.importer import ckan
from openspending.lib import json

from openspending.etl import util
from openspending.etl.command.base import OpenSpendingETLCommand
from openspending.etl.importer import CSVImporter, CKANImporter, ImporterError

log = logging.getLogger(__name__)

class ImportCommand(OpenSpendingETLCommand):

    @classmethod
    def standard_parser(cls, *args, **kwargs):
        parser = OpenSpendingETLCommand.standard_parser(*args, **kwargs)

        parser.add_option('-n', '--dry-run',
                          action="store_true", dest='dry_run', default=False,
                          help="Perform a dry run, don't load any data.")

        parser.add_option('--no-index', action="store_true", dest='no_index',
                          default=False, help='Suppress Solr index build.')

        parser.add_option('--mapping', action="store", dest='mapping',
                          default=None, metavar='URL',
                          help="URL of JSON format mapping.")

        parser.add_option('--max-errors', action="store", dest='max_errors',
                          type=int, default=None, metavar='N',
                          help="Maximum number of import errors to tolerate before giving up.")

        parser.add_option('--max-lines', action="store", dest='max_lines',
                          type=int, default=None, metavar='N',
                          help="Number of lines to import.")

        parser.add_option('--raise-on-error', action="store_true",
                          dest='raise_on_error', default=False,
                          help='Get full traceback on first error.')

        return parser

    def get_args(self):
        return {
            "dry_run": self.options.dry_run,
            "build_indices": not self.options.no_index,
            "raise_errors": self.options.raise_on_error,
            "max_lines": self.options.max_lines,
            "max_errors": self.options.max_errors
        }

class CSVImportCommand(ImportCommand):
    summary = "Load a CSV dataset"
    usage = "<dataset_url>"
    description = """\
                  You must specify one of --model OR (--mapping AND --metadata).
                  """

    parser = ImportCommand.standard_parser()

    parser.add_option('--model', action="store", dest='model',
                      default=None, metavar='url',
                      help="URL of JSON format model (metadata and mapping).")

    parser.add_option('--metadata', action="store", dest='metadata',
                      default=None, metavar='URL',
                      help="URL of JSON format metadata.")

    def command(self):
        super(CSVImportCommand, self).command()
        self._check_args_length(1)

        def json_of_url(url):
            return json.load(urllib2.urlopen(url))

        csv_data_url = self.args.pop(0)

        have_model = self.options.model or (self.options.mapping and self.options.metadata)

        if not have_model:
            print("You must provide --model OR (--mapping AND --metadata)!",
                  file=sys.stderr)
            return 1

        if self.options.model:
            model = json_of_url(self.options.model)
        else:
            model = {}

            from openspending.ui.lib.mappingimporter import MappingImporter
            mi = MappingImporter()
            model["mapping"] = mi.import_from_url(self.options.mapping)
            model["dataset"] = json_of_url(self.options.metadata)

        csv = util.urlopen_lines(csv_data_url)
        importer = CSVImporter(csv, model, csv_data_url)

        importer.run(**self.get_args())

class CKANImportCommand(ImportCommand):
    summary = "Load a dataset from CKAN"
    usage = "<package_name>"

    parser = ImportCommand.standard_parser()

    parser.add_option('--resource', action="store",
                      dest='resource', default=None, metavar="UUID",
                      help="UUID of CKAN resource containing entry data.")

    parser.add_option('--use-ckan-tags', action="store_true",
                      dest='use_ckan_tags', default=True,
                      help="Use CKAN to find resource UUID and mapping URL.")

    def command(self):
        super(CKANImportCommand, self).command()
        self._check_args_length(1)

        package = ckan.Package(self.args[0])

        if not self.options.use_ckan_tags:
            if not self.options.mapping:
                print("You must specify metadata mapping URL (--mapping)!", file=sys.stderr)
                return 1
            if not self.options.resource:
                print("You must specify the resource UUID (--resource)!", file=sys.stderr)
                return 1

        if self.options.use_ckan_tags:
            importer = CKANImporter(package)
        else:
            importer = CKANImporter(package,
                                    self.options.mapping,
                                    self.options.resource)

        importer.run(**self.get_args())

class ImportReportCommand(OpenSpendingETLCommand):
    summary = "Report on errors from all known datasets"

    parser = OpenSpendingETLCommand.standard_parser()

    def command(self):
        super(ImportReportCommand, self).command()
        self._check_args_length(0)

        print("-- Finding OpenSpending packages on CKAN...", file=sys.stderr)

        packages = [ p for p in ckan.openspending_packages() ]
        packages = filter(lambda x: x.is_importable(), packages)

        kwargs = {
            'build_indices': False,
            'max_lines': 30,
            'max_errors': 1
        }

        def first_error(package):
            importer = CKANImporter(package)
            try:
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
