from __future__ import print_function

from pprint import pprint
import sys

from openspending.lib import json
from openspending.etl.importer import ckan

def show(package_name):
    pprint(ckan.get_client().package_entity_get(package_name))
    return 0

def check(package_name):
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
    }

    print(json.dumps(res, indent=2, sort_keys=True))
    return 0

def hintadd(package_name, resource_uuid, hint):
    p = ckan.Package(package_name)

    print("Adding hint on %s of %s ('%s')..."
          % (resource_uuid, package_name, hint),
          file=sys.stderr)

    p.add_hint(resource_uuid, hint)

    print("Done!", file=sys.stderr)
    return 0

def hintrm(package_name, resource_uuid):
    p = ckan.Package(package_name)

    print("Removing hint from %s of %s..."
          % (resource_uuid, package_name),
          file=sys.stderr)

    p.remove_hint(resource_uuid)

    print("Done!", file=sys.stderr)
    return 0

def _show(args):
    return show(args.pkgname)

def _check(args):
    return check(args.pkgname)

def _hintadd(args):
    return hintadd(args.pkgname, args.uuid, args.hint)

def _hintrm(args):
    return hintrm(args.pkgname, args.uuid)

def configure_parser(subparser):
    parser = subparser.add_parser('ckan',
                                  help='OpenSpending-specific CKAN operations')
    sp = parser.add_subparsers(title='subcommands')

    p = sp.add_parser('show', help='Pretty-print CKAN package')
    p.add_argument('pkgname', help='CKAN package name')
    p.set_defaults(func=_show)

    p = sp.add_parser('check', help='Check importability of CKAN package')
    p.add_argument('pkgname', help='CKAN package name')
    p.set_defaults(func=_check)

    p = sp.add_parser('hintadd', help='Add hint on resource by UUID')
    p.add_argument('pkgname', help='CKAN package name')
    p.add_argument('uuid', help='Resource UUID')
    p.add_argument('hint', help='Hint content')
    p.set_defaults(func=_hintadd)

    p = sp.add_parser('hintrm', help='Remove hint on resource by UUID')
    p.add_argument('pkgname', help='CKAN package name')
    p.add_argument('uuid', help='Resource UUID')
    p.set_defaults(func=_hintrm)
