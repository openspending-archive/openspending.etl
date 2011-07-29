from os.path import dirname, join
from StringIO import StringIO
from urlparse import urlunparse

from openspending import model
from openspending.lib import json
from openspending.model import Dataset
from openspending.etl.test import DatabaseTestCase, helpers as h

from openspending.etl.importer import CSVImporter
from openspending.etl.mappingimporter import MappingImporter

def csvimport_fixture_file(name, path):
    try:
        fp = h.fixture_file('csv_import/%s/%s' % (name, path))
    except IOError:
        if name == 'default':
            fp = None
        else:
            fp = csvimport_fixture_file('default', path)

    return fp

def csvimport_fixture(name):
    data_fp = csvimport_fixture_file(name, 'data.csv')
    model_fp = csvimport_fixture_file(name, 'model.json')
    mapping_fp = csvimport_fixture_file(name, 'mapping.json')

    model = json.load(model_fp)

    if mapping_fp:
        model['mapping'] = json.load(mapping_fp)

    return data_fp, model

class TestCSVImporter(DatabaseTestCase):

    def test_successful_import(self):
        data, dmodel = csvimport_fixture('successful_import')
        importer = CSVImporter(data, dmodel)
        importer.run()
        dataset = Dataset.find_one()
        h.assert_true(dataset is not None, "Dataset should not be None")
        h.assert_equal(dataset.name, "test-csv")
        entries = model.entry.find({"dataset.name": dataset.name})
        h.assert_equal(entries.count(), 4)
        entry = model.entry.find_one({"provenance.line": 2})
        h.assert_true(entry is not None,
                      "Entry with name could not be found")
        h.assert_equal(entry['amount'], 130000.0)

    def test_successful_import_with_simple_testdata(self):
        data, dmodel = csvimport_fixture('simple')
        importer = CSVImporter(data, dmodel)
        importer.run()
        h.assert_equal(importer.errors, [])

        dataset = Dataset.find_one()
        h.assert_true(dataset is not None, "Dataset should not be None")

        entries = model.entry.find({"dataset.name": dataset.name})
        h.assert_equal(entries.count(), 5)

        entry = entries[0]
        h.assert_equal(entry['from']['label'], 'Test From')
        h.assert_equal(entry['to']['label'], 'Test To')
        h.assert_equal(entry['time']['unparsed'], '2010-01-01')
        h.assert_equal(entry['amount'], 100.00)

    def test_import_errors(self):
        data, model = csvimport_fixture('import_errors')

        importer = CSVImporter(data, model)
        importer.run(dry_run=True)
        h.assert_true(len(importer.errors) > 1, "Should have errors")
        h.assert_equal(importer.errors[0].line_number, 1,
                       "Should detect missing date colum in line 1")

    def test_empty_csv(self):
        empty_data = StringIO("")
        _, model = csvimport_fixture('default')
        importer = CSVImporter(empty_data, model)
        importer.run(dry_run=True)

        h.assert_equal(len(importer.errors), 2)

        h.assert_equal(importer.errors[0].line_number, 0)
        h.assert_equal(importer.errors[1].line_number, 0)

        h.assert_true("Didn't read any lines of data" in str(importer.errors[1].message))

    def test_malformed_csv(self):
        data, model = csvimport_fixture('malformed')
        importer = CSVImporter(data, model)
        importer.run(dry_run=True)
        h.assert_equal(len(importer.errors), 1)

    def test_erroneous_values(self):
        data, model = csvimport_fixture('erroneous_values')
        importer = CSVImporter(data, model)
        importer.run(dry_run=True)
        h.assert_equal(len(importer.errors), 1)
        h.assert_true("date" in importer.errors[0].message,
                      "Should find badly formatted date")
        h.assert_equal(importer.errors[0].line_number, 5)

    def test_error_with_empty_additional_date(self):
        data, model = csvimport_fixture('empty_additional_date')
        importer = CSVImporter(data, model)
        importer.run()
        # We are currently not able to import date cells without a value. See:
        # http://trac.openspending.org/ticket/170
        h.assert_equal(len(importer.errors), 1)

    def test_currency_sane(self):
        h.skip("Not yet implemented")

class TestCSVImportDatasets(DatabaseTestCase):

    datasets_to_test = ('lbhf', 'mexico', 'sample', 'uganda')

    def count_lines_in_stream(self, f):
        try:
            return len(f.read().splitlines())
        finally:
            f.seek(0)

    def _test_import(self, name):
        data, dmodel = csvimport_fixture(name)
        lines = self.count_lines_in_stream(data) - 1 # -1 for header row

        importer = CSVImporter(data, dmodel)
        importer.run()

        h.assert_equal(len(importer.errors), 0)

        # check correct number of entries
        entries = model.entry.find({"dataset.name": dmodel['dataset']['name']})
        h.assert_equal(entries.count(), lines)

    def _test_mapping(self, name):
        mapping_csv = csvimport_fixture_file(name, 'mapping.csv').read()
        mapping_json = csvimport_fixture_file(name, 'mapping.json').read()

        expected_mapping = json.loads(mapping_json)

        importer = MappingImporter()
        observed_mapping = importer.import_from_string(mapping_csv)

        h.assert_equal(observed_mapping, expected_mapping)

    def test_all_mappings(self):
        for dir in self.datasets_to_test:
            yield self._test_mapping, dir

    def test_all_imports(self):
        for dir in self.datasets_to_test:
            yield self._test_import, dir


class TestMappingImporter(DatabaseTestCase):

    def test_empty_mapping(self):
        mapping_csv = csvimport_fixture_file('simple', 'mapping.csv').read()
        importer = MappingImporter()
        mapping = importer.import_from_string(mapping_csv)
        h.assert_equal(sorted(mapping.keys()),
                         [u'amount', u'currency', u'from', u'time', u'to'])
        h.assert_equal(mapping['amount'],
                         {'column': u'amount',
                          'datatype': u'float',
                          'default_value': u'x',
                          'description': u'z',
                          'label': u'y',
                          'type': u'value'})
        h.assert_equal(mapping['currency'],
                         {'column': u'currency',
                          'datatype': u'string',
                          'default_value': u'GBP',
                          'description': u'z',
                          'label': u'y',
                          'type': u'value'})
        h.assert_equal(mapping['from'],
                         {'description': u'z',
                          'fields': [{'column': u'paid_by',
                                      'datatype': u'string',
                                      'default_value': u'x',
                                      'name': 'label'}],
                          'label': u'y',
                          'type': u'entity'})
        h.assert_equal(mapping['to'],
                         {'description': u'z',
                          'fields': [{'column': u'paid_to',
                                      'datatype': u'string',
                                      'default_value': u'x',
                                      'name': 'label'}],
                          'label': u'y',
                          'type': u'entity'})
        h.assert_equal(mapping['time'],
                         {'column': u'date',
                          'datatype': u'date',
                          'default_value': u'x',
                          'description': u'z',
                          'label': u'y',
                          'type': u'value'})

    def test_missing_columns(self):
        data = ("Uninteresting,Columns\n"
                "Uninteresting,Values")
        importer = MappingImporter()
        try:
            importer.import_from_string(data)
        except AssertionError, E:
            h.assert_true(
                'The Metadata document must have the columns "Original Field",'
                in str(E))
            h.assert_true('The column(s)' in str(E))
            h.assert_true('are missing.' in str(E))
            return
        raise AssertionError('Missing Exception')

    def test_nested_classifier_columns(self):
        mapping_csv = csvimport_fixture_file('nested', 'mapping.csv').read()
        importer = MappingImporter()
        mapping = importer.import_from_string(mapping_csv)
        to_fields = mapping['to']['fields']
        h.assert_equal(len(to_fields), 2)
        h.assert_equal(to_fields[0]['column'], u'paid_to')
        h.assert_equal(to_fields[0]['name'], 'label')
        h.assert_equal(to_fields[1]['column'], u'paid_to_identifier')
        h.assert_equal(to_fields[1]['name'], 'identifier')

    def test_line_in_error(self):
        importer = MappingImporter()
        mapping_csv = csvimport_fixture_file('wrong_object_type', 'mapping.csv').read()
        try:
            importer.import_from_string(mapping_csv)
        except ValueError, E:
            errors = E.args[0]
            h.assert_equal(len(errors), 1)
            h.assert_equal(errors[0]['line'], 2)
            return
        raise AssertionError('Missing Exception')

    def test_wrong_objecttype(self):
        importer = MappingImporter()
        mapping_csv = csvimport_fixture_file('wrong_object_type', 'mapping.csv').read()
        try:
            importer.import_from_string(mapping_csv)
        except ValueError, E:
            errors = E.args[0]
            h.assert_equal(len(errors), 1)
            h.assert_equal(errors[0]['line'], 2)
            h.assert_equal(errors[0]['message'],
                             (u'Value in column "ObjectType" is "entit". '
                              u'Allowed values: "classifier", "entity", '
                              u'"value"'))
            return
        raise AssertionError('Missing Exception')
