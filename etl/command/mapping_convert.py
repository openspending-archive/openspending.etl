from openspending.etl.command.base import OpenSpendingETLCommand

class MappingConvertCommand(OpenSpendingETLCommand):
    summary = "Convert mapping file/url to JSON."
    usage = "<mapping_url>"

    parser = OpenSpendingETLCommand.standard_parser()

    def command(self):
        super(MappingConvertCommand, self).command()
        self._check_args_length(1)

        from openspending.ui.lib.mappingimporter import MappingImporter
        from openspending.ui.lib import json

        mapping_url = self.args[0]
        importer = MappingImporter()
        mapping = importer.import_from_url(mapping_url)
        print json.dumps(mapping, indent=2)