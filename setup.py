import os

from setuptools import setup, find_packages
from etl import __version__

PKG_ROOT = '.packageroot'

def files_in_pkgdir(pkg, dirname):
    pkgdir = os.path.join(PKG_ROOT, *pkg.split('.'))
    walkdir = os.path.join(pkgdir, dirname)
    walkfiles = []
    for dirpath, _, files in os.walk(walkdir):
        fpaths = (os.path.relpath(os.path.join(dirpath, f), pkgdir)
                  for f in files)
        walkfiles += fpaths
    return walkfiles

setup(
    name='openspending.etl',
    version=__version__,
    description='OpenSpending Extract/Transform/Load tools',
    author='Open Knowledge Foundation',
    author_email='okfn-help at lists okfn org',
    url='http://github.com/okfn/openspending.etl',

    install_requires=[
        "python-daemon==1.5.5",
        "openspending"
    ],
    setup_requires=[
        "PasteScript==1.7.3"
    ],

    packages=find_packages(PKG_ROOT),
    package_dir={'': PKG_ROOT},
    namespace_packages = ['openspending'],
    package_data={
        'openspending.etl': [
            'validation/currencies.xml'
        ],
        'openspending.etl.ui': (
            files_in_pkgdir('openspending.etl.ui', 'public') +
            files_in_pkgdir('openspending.etl.ui', 'templates')
        )
    },

    test_suite='nose.collector',

    zip_safe=False,

    paster_plugins=['PasteScript', 'Pylons'],

    entry_points={
        'paste.app_factory': [
            'main = openspending.etl.ui.config.middleware:make_app'
        ],
        'paste.app_install': [
            'main = pylons.util:PylonsInstaller'
        ],
        'paste.paster_command': [
            'csvimport = openspending.etl.command:CSVImportCommand',
            'ckanimport = openspending.etl.command:CKANImportCommand',
            'mappingconvert = openspending.etl.command:MappingConvertCommand',
            'mappingurl = openspending.etl.command:MappingUrlCommand',
            'importreport = openspending.etl.command:ImportReportCommand',
            'ckan = openspending.etl.command:CkanCommand'
        ],
        'console_scripts': [
            'openspendingetld = openspending.etl.command.daemon:main'
        ]
    }
)
