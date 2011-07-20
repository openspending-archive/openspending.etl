from openspending.command.base import OpenSpendingCommand

class MappingUrlCommand(OpenSpendingCommand):
    summary = "Reveal the actual URL used for metadata."
    usage = "<mapping_url>"

    parser = OpenSpendingCommand.standard_parser()

    def command(self):
        super(MappingUrlCommand, self).command()
        self._check_args_length(1)

        from openspending.ui.lib.mappingimporter import MappingImporter
        from openspending.ui.lib.helpers import json

        mapping_url = self.args[0]
        importer = MappingImporter()
        print importer.csv_url(mapping_url)