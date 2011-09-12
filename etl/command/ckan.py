from __future__ import print_function

import sys

from openspending.etl.command.base import OpenSpendingETLCommand
from openspending.etl.importer import ckan

class CkanCommand(OpenSpendingETLCommand):
    summary = "Interface to OpenSpending-specific CKAN operations"
    usage = "<subcommand> [args, ...]"
    description = """\
                  Recognized subcommands:
                    show <pkgname>:                     Pretty-print package.
                    hintadd <pkgname> <uuid> <hint>:    Add hint on resource by UUID.
                    hintrm <pkgname> <uuid>:            Remove any hint on resource by UUID.
                  """

    parser = OpenSpendingETLCommand.standard_parser()

    def command(self):
        super(CkanCommand, self).command()

        if len(self.args) < 2:
            CkanCommand.parser.print_help()
            return 1

        self.c = ckan.get_client()

        cmd = self.args[0]

        if cmd == 'show':
            self._cmd_show()
        elif cmd == 'check':
            self._cmd_check()
        elif cmd == 'hintadd':
            self._cmd_hintadd()
        elif cmd == 'hintrm':
            self._cmd_hintrm()
        else:
            raise self.BadCommand("Subcommand '%s' not recognized " \
                                  "by 'ckan' command!" % cmd)

    def _cmd_show(self):
        if len(self.args) != 2:
            raise self.BadCommand("Usage: paster ckan show <pkgname>")

        package_name = self.args[1]

        from pprint import pprint

        pprint(self.c.package_entity_get(package_name))

    def _cmd_check(self):
        if len(self.args) != 2:
            raise self.BadCommand("Usage: paster ckan check <pkgname>")

        package_name = self.args[1]
        from openspending.lib import json

        p = ckan.Package(package_name)

        def _get_url_or_error(resource_name):
            try:
                r = p.openspending_resource(resource_name)
                if r:
                    return r["url"]
                else:
                    return r
            except ckan.ResourceError as e:
                return str(e)

        res = {
            "is_importable": p.is_importable(),
            "data": _get_url_or_error('data'),
            "model": _get_url_or_error('model'),
            "model:mapping": _get_url_or_error('model:mapping')
        }

        print(json.dumps(res, indent=2, sort_keys=True))

    def _cmd_hintadd(self):
        if len(self.args) != 4:
            raise self.BadCommand("Usage: paster ckan hintadd <pkgname> <uuid> <hint>")

        package_name = self.args[1]
        resource_uuid = self.args[2]
        hint = self.args[3]

        p = ckan.Package(package_name)

        print("Adding hint on %s of %s ('%s')..."
              % (resource_uuid, package_name, hint),
              file=sys.stderr)

        p.add_hint(resource_uuid, hint)

        print("Done!", file=sys.stderr)

    def _cmd_hintrm(self):
        if len(self.args) != 3:
            raise self.BadCommand("Usage: paster ckan hintrm <pkgname> <uuid>")

        package_name = self.args[1]
        resource_uuid = self.args[2]

        p = ckan.Package(package_name)

        print("Removing hint from %s of %s..."
              % (resource_uuid, package_name),
              file=sys.stderr)

        p.remove_hint(resource_uuid)

        print("Done!", file=sys.stderr)