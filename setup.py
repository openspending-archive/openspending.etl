from setuptools import setup, find_packages
from etl import __version__

setup(
    name='openspending.etl',
    version=__version__,
    description='OpenSpending Extract/Transform/Load tools',
    author='Open Knowledge Foundation',
    author_email='okfn-help at lists okfn org',
    url='http://github.com/okfn/openspending.etl',

    install_requires=[
        "celery-pylons==2.1.4dev",
        "openspending"
    ],
    setup_requires=[
        "PasteScript==1.7.3"
    ],

    packages=find_packages('.packageroot'),
    package_dir={'': '.packageroot'},
    namespace_packages = ['openspending'],

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
        ]
    }
)
