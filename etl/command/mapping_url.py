from openspending.etl.command.base import OpenSpendingETLCommand

class MappingUrlCommand(OpenSpendingETLCommand):
    summary = "Reveal the actual URL used for metadata."
    usage = "<mapping_url>"

    parser = OpenSpendingETLCommand.standard_parser()

    def command(self):
        super(MappingUrlCommand, self).command()
        self._check_args_length(1)

        from openspending.lib import json
        from openspending.mappingimporter import MappingImporter

        mapping_url = self.args[0]
        importer = MappingImporter()
        print importer.csv_url(mapping_url)