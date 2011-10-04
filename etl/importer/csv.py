from datetime import datetime

from openspending.lib import unicode_dict_reader as udr

from openspending.etl import util
from openspending.etl.times import for_datestrings, EMPTY_DATE
from openspending.etl.validation.entry import PLACEHOLDER
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

    def import_line(self, line):
        entry = {
            'provenance': {
                "dataset": self.loader.dataset['name'],
                "source_file": self.source_file,
                "line": self.line_number,
                "timestamp": datetime.utcnow()
            },
            "_csv_import_fp": "%s:%s:%d" % (self.loader.dataset['name'],
                                            self.source_file,
                                            self.line_number)
        }

        for dimension, description in self.mapping.iteritems():
            try:
                self._load_cell(entry, line, dimension, description)
            except Exception as e:
                if self.raise_errors:
                    print(line)
                    raise
                else:
                    raise LineImportError(dimension, repr(e))

        self.loader.create_entry(**entry)

    def _load_cell(self, entry, line, dimension, description):
        dimension = str(dimension)
        dimension_type = description.get('type', 'value')
        dimension_value = None

        if dimension_type == 'value':
            value = self._convert_type(line, description)
            entry[dimension] = value
            return

        dimension_value = {}
        for field in description['fields']:
            fieldname = str(field['name'])
            dimension_value[fieldname] = self._convert_type(line, field)

        assert (('name' in dimension_value) or ('label' in dimension_value))

        if 'name' in dimension_value:
            name = dimension_value['name']
        else:
            name = dimension_value['label']

        dimension_value['name'] = util.slugify(name)

        if dimension_type == 'entity':
            match_keys = description.get('match_keys', ('name',))
            entity = self.loader.create_entity(dimension_value.pop('name'),
                                               match_keys=match_keys,
                                               **dimension_value)
            self.loader.entitify_entry(entry, entity, dimension)

        elif dimension_type == 'classifier':
            dimension_value['taxonomy'] = util.slugify(description.get('taxonomy'))
            classifier = self.loader.create_classifier(dimension_value)
            self.loader.classify_entry(entry, classifier, dimension)

    def _convert_type(self, line, description):
        type_string = description.get('datatype', 'value')
        value = line.get(description.get('column'))

        if type_string == "constant":
            return description.get('constant')

        if not value:
            if description.get('default_value', '').strip():
                value = description.get('default_value').strip()
        if value is None:
            return value

        if type_string == "date":
            default = description.get('default_value')
            if not value or value == PLACEHOLDER:
                if not default:
                    return EMPTY_DATE
                else:
                    value = default
            end_value = line.get(description.get('end_column'))
            return for_datestrings(value, end_value)
        elif type_string == "string":
            return value
        elif type_string == "float":
            return float(unicode(value).replace(",", ""))
        elif type_string == "id":
            return util.slugify(value)
        else:
            return value
