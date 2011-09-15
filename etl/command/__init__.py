from . import ckan, importer

def configure_parsers(subparser):
    for mod in (ckan, importer):
        mod.configure_parser(subparser)