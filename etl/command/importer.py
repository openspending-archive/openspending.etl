from __future__ import print_function

import sys

from openspending.etl.command.base import OpenSpendingETLCommand
from openspending.etl.ckan_import import ckan_import

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

    def report_errors(self, errors):
        print("There were %d errors:" % len(errors), file=sys.stderr)
        for err in errors:
            print("Line %s: %s" % (err.line_number, err.message), file=sys.stderr)

    def get_args(self):
        return {
            "dry_run": self.options.dry_run,
            "progress_callback": lambda msg: print(msg, file=sys.stderr),
            "do_index": not self.options.no_index,
            "reraise_errors": self.options.raise_on_error,
            "max_lines": self.options.max_lines,
            "max_errors": self.options.max_errors
        }

class CSVImportCommand(ImportCommand):
    summary = "Load a CSV dataset."
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

        from openspending.etl.csvimport import load_dataset
        from openspending.lib import json

        def json_of_url(url):
            import urllib2
            return json.load(urllib2.urlopen(url))

        self._load_config()

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

        kwargs = self.get_args()

        _, _, errors = load_dataset(csv_data_url, model, **kwargs)

        self.report_errors(errors)

class CKANImportCommand(ImportCommand):
    summary = "Load a dataset from CKAN."
    usage = "<package_name>"

    parser = ImportCommand.standard_parser()

    parser.add_option('--resource', action="store",
                      dest='resource', default=None, metavar="UUID",
                      help="UUID of CKAN resource containing entry data.")

    parser.add_option('--use-ckan-tags', action="store_true",
                      dest='use_ckan_tags', default=False,
                      help="Use CKAN to find resource UUID and mapping URL.")

    def command(self):
        super(CKANImportCommand, self).command()
        self._check_args_length(1)

        from openspending.lib import ckan, json

        package = ckan.Package(self.args[0])

        if not self.options.use_ckan_tags:
            if not self.options.mapping:
                print("You must specify metadata mapping URL (--mapping)!", file=sys.stderr)
                return 1
            if not self.options.resource:
                print("You must specify the resource UUID (--resource)!", file=sys.stderr)
                return 1

        kwargs = self.get_args()

        if self.options.use_ckan_tags:
            errors = ckan_import(package, **kwargs)
        else:
            errors = ckan_import(package,
                                 self.options.mapping,
                                 self.options.resource,
                                 **kwargs)

        self.report_errors(errors)

class ImportReportCommand(OpenSpendingETLCommand):
    summary = "Report on errors from all known datasets."

    parser = OpenSpendingETLCommand.standard_parser()

    def command(self):
        super(ImportReportCommand, self).command()
        self._check_args_length(0)

        from openspending.lib import ckan
        from openspending.lib import json

        self._load_config()

        print("-- Finding OpenSpending packages on CKAN...", file=sys.stderr)

        packages = [ p for p in ckan.openspending_packages() ]
        packages = filter(lambda x: x.is_importable(), packages)

        kwargs = {
            'do_index': False,
            'max_lines': 30,
            'max_errors': 1
        }

        def first_error(package):
            try:
                errors = ckan_import(package, **kwargs)
            except Exception as e:
                return str(e)

            if len(errors) == 0:
                return "No errors found"
            else:
                return "Line %d: %s" % (errors[0].line_number, errors[0].message)

        import_errors = {}

        for p in packages:
            print("-- Starting import of '%s'" % p.name, file=sys.stderr)
            import_errors[p.name] =  first_error(p)

        print(json.dumps(import_errors, indent=2))