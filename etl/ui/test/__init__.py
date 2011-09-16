"""\
OpenSpending ETL UI test module
===============================

Run the OpenSpending test suite by running

    nosetests

in the root of the repository, while in an active virtualenv. See
doc/install.rst for more information.
"""

from paste.script.appinstall import SetupCommand
from pylons import url, config
from routes.util import URLGenerator
from webtest import TestApp
import pylons.test

from openspending.etl.test import TestCase, DatabaseTestCase, setup_package as root_setup_package
from openspending.etl.ui.test.helpers import clean_all

__all__ = [
    'environ', 'url', 'TestCase', 'DatabaseTestCase', 'ControllerTestCase'
]

environ = {}

# Clear everything before any tests are run.
def setup_package():
    root_setup_package()
    # Invoke websetup with the current config file
    SetupCommand('setup-app').run([config['__file__']])
    clean_all()

class ControllerTestCase(DatabaseTestCase):
    def __init__(self, *args, **kwargs):
        wsgiapp = pylons.test.pylonsapp
        self.app = TestApp(wsgiapp)
        url._push_object(URLGenerator(config['routes.map'], environ))
        super(DatabaseTestCase, self).__init__(*args, **kwargs)
