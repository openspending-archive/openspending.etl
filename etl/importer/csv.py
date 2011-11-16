from openspending.lib import unicode_dict_reader as udr

from openspending.etl.importer.base import BaseImporter, ImporterError

class LineImportError(ImporterError):
    def __init__(self, field, exc):
        self.field = field
        self.exc = exc

    def __str__(self):
        return "Column `%s': %s" % (self.field, repr(self.exc))

class CSVImporter(BaseImporter):

    @property
    def lines(self):
        try:
            return udr.UnicodeDictReader(self.data)
        except udr.EmptyCSVError as e:
            self.add_error(e)
            return ()

