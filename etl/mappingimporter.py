import StringIO
import urllib
import urlparse

from openspending.etl.ui.forms.sources import DATATYPE_NAMES, DIMENSION_TYPES
from openspending.lib.unicode_dict_reader import UnicodeDictReader

COMPLEX_TYPES = ['classifier', 'entity']

# columns
ORIGINAL_FIELD = 'Original Field'
OPENSPENDING_FIELD = 'OpenSpending Field'
DEFAULT_VALUE = 'Default value'
LABEL = 'Label'
DESCRIPTION = 'Description'
DATATYPE = 'DataType'
OBJECTTYPE = 'ObjectType'
CLASSIFIER_TAXONOMY = 'Classifier taxonomy'
COMMENTARY = 'Commentary'

COLUMNS = [ORIGINAL_FIELD, OPENSPENDING_FIELD, DEFAULT_VALUE,
           LABEL, DESCRIPTION, DATATYPE, OBJECTTYPE,
           CLASSIFIER_TAXONOMY, COMMENTARY]


def concat(columns):
    columns = ['"%s"' % column for column in columns]
    return ', '.join(columns)


class MappingFieldsConstructor(object):
    '''
    Construct a mapping `dict` for a data
    :class:`openspending.etl.ui.lib.model.Model` to use with the
    :class:`openspending.etl.ui.lib.csv_importer.DatasetImporter`.

    It expects that you feed in dicts with :meth:`add`.

    fixme: document
    '''

    def __init__(self):
        self.fields = {}
        self.fields_info = {}
        self.errors = []
        self.line = 1

    def get_mapping(self):
        if self.errors:
            return None
        else:
            return self.fields

    def add_error(self, msg):
        self.errors.append({'line': self.line,
                            'message': msg})

    def insert_default_values(self, row):
        row[DATATYPE] = row.get(DATATYPE) or 'string'
        row[OBJECTTYPE] = row.get(OBJECTTYPE) or 'value'
        return row

    def datatype(self, row):
        datatype = row[DATATYPE]
        if datatype not in DATATYPE_NAMES:
            self.add_error('Value in column "%s" is "%s". Allowed values: %s' %
                           (DATATYPE, datatype, concat(DATATYPE_NAMES)))
        return row

    def objecttype(self, row):
        objecttype = row[OBJECTTYPE]
        if objecttype not in DIMENSION_TYPES:
            self.add_error('Value in column "%s" is "%s". Allowed values: %s' %
                           (OBJECTTYPE, objecttype, concat(DIMENSION_TYPES)))
        return row

    def openspending_field(self, row):
        fieldname = row[OPENSPENDING_FIELD]
        segments = len(fieldname.split('.'))
        if segments > 2:
            self.add_error(u'The fieldname in the column "OpenSpending Field" '
                           u'can\'t contain more than one "." (dot). It is: '
                           u'%s' % fieldname)
        if (segments > 1):
            objecttype = row[OBJECTTYPE]
            if objecttype not in COMPLEX_TYPES:
                self.add_error(u'The fieldname in the column "OpenSpending '
                               u'Field" can only contain a "." (dot) if '
                               u'the ObjectType is "classifier" or "entity".')
        return row

    def add_simple_type(self, row):
        fieldname = row[OPENSPENDING_FIELD]
        if fieldname in self.fields:
            self.add_error('The "%s" "%s" was already defined before.' %
                           (OPENSPENDING_FIELD, fieldname))
            return
        field = {'column': row[ORIGINAL_FIELD],
                 'datatype': row[DATATYPE],
                 'type': row[OBJECTTYPE],
                 'label': row[LABEL],
                 'description': row[DESCRIPTION],
                 'default_value': row[DEFAULT_VALUE]}
        self.fields[fieldname] = field

    def complex_field_names(self, raw_fieldname):
        segments = raw_fieldname.split('.')
        fieldname = segments.pop(0)
        subfieldname = segments and segments[0] or 'label'
        return fieldname, subfieldname

    def add_complex_field(self, row):
        raw_fieldname = row[OPENSPENDING_FIELD]
        fieldname, subfieldname = self.complex_field_names(raw_fieldname)
        objecttype = row[OBJECTTYPE]
        subfield = {'column': row[ORIGINAL_FIELD],
                    'datatype': row[DATATYPE],
                    'constant': '',
                    'name': subfieldname,
                    'default_value': row[DEFAULT_VALUE]}
        field = {'type': objecttype,
                 'label': row[LABEL],
                 'description': row[DESCRIPTION],
                 'fields': [subfield]}

        if objecttype == 'classifier':
            taxonomy = row[CLASSIFIER_TAXONOMY]
            if not taxonomy:
                self.add_error(('You need to specify a "%s" as it is a'
                                'classifier.') % CLASSIFIER_TAXONOMY)
            field['taxonomy'] = taxonomy

        if fieldname in self.fields:
            existing = self.fields[fieldname]
            if existing['type'] != field['type']:
                error = ('The "%s" for "%s" is "%s", but was defined '
                         'earlier as "%s"') % (OBJECTTYPE, fieldname,
                                               objecttype, existing['type'])
                self.add_error(error)

            for existing_subfield in existing['fields']:
                if existing_subfield['name'] == subfieldname:
                    error = ('The "%s" for the "%s" "%s" was defined earlier' %
                             (subfieldname, OPENSPENDING_FIELD, fieldname))
                    self.add_error(error)

            if (objecttype == 'classifier' and
                (existing.get('taxonomy') != field.get('taxonomy'))):
                error = ('The classifier "%s" is defined for the taxonomy '
                         '"%s", but was defined earlier for the taxonomy '
                         '"%s".') % (fieldname, field['taxonomy'],
                                     existing['taxonomy'])
                self.add_error(error)
            existing['fields'].append(subfield)
        else:
            self.fields[fieldname] = field

    def add_field(self, row):
        objecttype = row[OBJECTTYPE]
        if objecttype in COMPLEX_TYPES:
            self.add_complex_field(row)
        else:
            self.add_simple_type(row)

    def add(self, row):
        self.line = self.line + 1
        if not row[OPENSPENDING_FIELD]:
            return
        row = self.insert_default_values(row)
        row = self.datatype(row)
        row = self.objecttype(row)
        row = self.openspending_field(row)
        row = self.add_field(row)


class MappingImporter(object):

    def check_columns(self, row):
        missing = []
        for column in COLUMNS:
            if column not in row:
                missing.append(column)
        if missing:
            msg = ('The Metadata document must have the columns '
                   '%s. The column(s) %s are missing.' % (concat(COLUMNS),
                                                          concat(missing)))
            raise AssertionError(msg)

    def get_mapping(self, data):
        data = StringIO.StringIO(data)
        reader = UnicodeDictReader(data)

        self.fields = MappingFieldsConstructor()
        columns_checked = False
        for row in reader:
            if not columns_checked:
                self.check_columns(row)
                columns_checked = True

            self.fields.add(row)

        mapping = self.fields.get_mapping()
        if mapping is None:
            raise ValueError(self.fields.errors)
        return mapping

    def import_from_string(self, data):
        return self.get_mapping(data)

    def csv_url(self, url):
        if 'google.com/spreadsheet/ccc' in url:
            q = urlparse.urlparse(url)[4]
            q_parsed = urlparse.parse_qs(q)
            gdocs_key = q_parsed['key'][0]
            return ('https://spreadsheets.google.com/spreadsheet/pub'
                    '?key=%s&output=csv') % gdocs_key
        else:
            return url

    def import_from_url(self, url):
        url = self.csv_url(url)

        data = urllib.urlopen(url).read()

        if '<html' in data[:100]:
            raise ValueError("Webpage gave HTML, not CSV; did you "
                             "publish with correct permissions?")

        return self.import_from_string(data)

    def import_from_file(self, filename):
        with file(filename) as f:
            return self.import_from_string(f.read())
