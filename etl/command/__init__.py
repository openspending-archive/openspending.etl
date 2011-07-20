# command.py
#
# The leaf classes in this module implement Paste Script commands; they
# are runnable with "paster thing", where thing is one of the commands
# listed in the openspending.ui section of "paster help".
#
# They are registered in openspending.ui's setup.py.
#
# See http://pythonpaste.org/script/developer.html for documentation of
# Paste Script.
from __future__ import absolute_import

from .load import LoadCommand
from .mapping_convert import MappingConvertCommand
from .mapping_url import MappingUrlCommand
from .ckan import CkanCommand
from .importer import CKANImportCommand, CSVImportCommand, ImportReportCommand