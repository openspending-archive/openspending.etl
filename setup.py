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
        "Pylons==1.0",
        "Genshi==0.6",
        "pymongo==1.11",
        "repoze.who==2.0b1",
        "repoze.who-friendlyform==1.0.8",
        "Unidecode==0.04.7",
        "python-dateutil==1.5",
        "solrpy==0.9.4",
        "pyutilib.component.core==4.3.1",
        "celery-pylons==2.1.4dev",
        "Babel==0.9.6",
        "ckanclient==0.7",
        "colander==0.9.3",
        "distribute==0.6.19",
        "mock==0.7.2",
        "openspending"
    ],

    packages=find_packages('.packageroot'),
    package_dir={'': '.packageroot'},
    namespace_packages = ['openspending'],

    test_suite='nose.collector',

    zip_safe=False,

    entry_points={
        'paste.app_factory': [
            'main = openspending.etl.ui.config.middleware:make_app'
        ],
        'paste.app_install': [
            'main = pylons.util:PylonsInstaller'
        ]
    }
)
