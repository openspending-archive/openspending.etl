from openspending.command.base import OpenSpendingCommand

class LoadCommand(OpenSpendingCommand):
    summary = "Load external data into domain model."
    usage = "<dataset>"
    description = """\
                  Recognized datasets:
                      cra:        Country & Regional Analysis
                      cra2010:    Country & Regional Analysis 2010
                      cofog:      COFOG
                      barnet:     Barnet Council budget
                      gla:        GLA (untested)
                  """

    parser = OpenSpendingCommand.standard_parser()

    def command(self):
        super(LoadCommand, self).command()
        self._check_args_length(1)

        cmd = self.args[0]
        import pkg_resources
        loader_method = None
        for entry_point in pkg_resources.iter_entry_points('openspending.ui.load'):
            if entry_point.name == cmd:
                loader_method = entry_point.load()
        if loader_method:
            loader_method(*self.args[1:])
        else:
            print 'Loader not recognized'