from os.path import dirname, join
from StringIO import StringIO
from urlparse import urlunparse

from openspending.lib import json
from openspending.model import Dataset, Entry
from openspending.etl.test import TestCase, DatabaseTestCase, helpers as h

from openspending.etl.importer import CSVImporter
from openspending.etl.mappingimporter import MappingImporter

def check_throws_one_error(self, importer):
    h.assert_equal(len(importer.errors), 1)

def csv_fixture(name):
    return h.fixture_file("csv_import/%s.csv" % name)

def csv_fixture_mapping(name):
    f = h.fixture_file("csv_import/%s-mapping.json" % name)
    return json.load(f)

def csv_fixture_model(dataset=None, name='default'):
    if not dataset:
        dataset = {
            "name": u"test-csv",
            'unique_keys': [],
            "label": u"Label for Test CSV Import",
            "description": u"Description for Test CSV Import",
            "currency": "EUR"
        }

    return {
        'dataset': dataset,
        'mapping': csv_fixture_mapping(name)
    }

class TestCSVImporter(DatabaseTestCase):

    def test_successful_import(self):
        data = csv_fixture('successful_import')
        model = csv_fixture_model()
        importer = CSVImporter(data, model)
        importer.run()
        dataset = Dataset.find_one()
        h.assert_true(dataset is not None, "Dataset should not be None")
        h.assert_equal(dataset.name, "test-csv")
        entries = list(Entry.find({"dataset.name": dataset.name}))
        h.assert_equal(len(entries), 4)
        entry = Entry.find_one({"provenance.line": 2})
        h.assert_true(entry is not None,
                      "Entry with name could not be found")
        h.assert_equal(entry.amount, 130000.0)

    def test_successful_import_with_simple_testdata(self):
        data = csv_fixture('simple')
        model = csv_fixture_model(name='simple')
        importer = CSVImporter(data, model)
        importer.run()
        h.assert_equal(importer.errors, [])

        dataset = Dataset.find_one()
        h.assert_true(dataset is not None, "Dataset should not be None")

        entries = list(Entry.find({"dataset.name": dataset.name}))
        h.assert_equal(len(entries), 5)

        entry = entries[0]
        h.assert_equal(entry['from']['label'], 'Test From')
        h.assert_equal(entry['to']['label'], 'Test To')
        h.assert_equal(entry['time']['unparsed'], '2010-01-01')
        h.assert_equal(entry['amount'], 100.00)

    def test_import_errors(self):
        data = csv_fixture('import_errors')
        model = csv_fixture_model()

        importer = CSVImporter(data, model)
        importer.run(dry_run=True)
        h.assert_true(len(importer.errors) > 1, "Should have errors")
        h.assert_equal(importer.errors[0].line_number, 1,
                       "Should detect missing date colum in line 1")

    def test_empty_csv(self):
        empty_data = StringIO("")
        model = csv_fixture_model()
        importer = CSVImporter(empty_data, model)
        importer.run(dry_run=True)

        h.assert_equal(len(importer.errors), 2)

        h.assert_equal(importer.errors[0].line_number, 0)
        h.assert_equal(importer.errors[1].line_number, 0)

        h.assert_true("Didn't read any lines of data" in str(importer.errors[1].message))

    def _test_file_with_model(self, data_filename, model, checks):
        data = csv_fixture(data_filename)
        importer = CSVImporter(data, model)
        importer.run(dry_run=True)

        for check in checks:
            check(self, importer)

    def test_malformed_csv(self):
        data_filename = 'malformed_csv'
        model = csv_fixture_model()

        checks = [check_throws_one_error]

        self._test_file_with_model(data_filename, model, checks)

    def test_erroneous_values(self):
        data = csv_fixture('erroneous_values')
        model = csv_fixture_model()
        importer = CSVImporter(data, model)
        importer.run(dry_run=True)
        h.assert_equal(len(importer.errors), 1)
        h.assert_true("date" in importer.errors[0].message,
                      "Should find badly formatted date")
        h.assert_equal(importer.errors[0].line_number, 5)

    def test_error_with_empty_additional_date_column(self):
        name = 'empty_additional_date_column'
        data = csv_fixture(name)
        model = csv_fixture_model(name=name)
        importer = CSVImporter(data, model)
        importer.run()
        # We are currently not able to import date cells without a value. See:
        # http://trac.openspending.org/ticket/170
        h.assert_equal(len(importer.errors), 1)

    def test_currency_sane(self):
        h.skip("Not yet implemented")

class TestCSVImportDatasets(TestCase):

    def count_lines_in_stream(self, f):
        try:
            return len(f.read().splitlines())
        finally:
            f.seek(0)

    def _test_dataset_dir(self, dir):
        data_csv = h.fixture_file('csv_import/%s/data.csv' % dir)
        mapping_json = h.fixture_file('csv_import/%s/mapping.json' % dir)

        dataset_name = unicode(dir)

        model = csv_fixture_model()
        model['mapping'] = json.load(mapping_json)
        model['dataset']['name'] = dataset_name

        lines = self.count_lines_in_stream(data_csv) - 1

        importer = CSVImporter(data_csv, model)
        importer.run()

        assert len(importer.errors) == 0, "Import should not throw errors"

        # check correct number of entries
        entries = Entry.find({"dataset.name": dataset_name})
        assert entries.count() == lines

        # TODO: check correct dimensions

    def _test_mapping(self, dir):
        mapping_csv = h.fixture_file('csv_import/%s/mapping.csv' % dir)
        mapping_json = h.fixture_file('csv_import/%s/mapping.json' % dir)

        csv = mapping_csv.read()
        expected_mapping_data = json.load(mapping_json)

        importer = MappingImporter()
        observed_mapping_data = importer.import_from_string(csv)

        assert observed_mapping_data == expected_mapping_data

    @property
    def testdata_dirs(self):
        from pkg_resources import resource_isdir as isdir, resource_listdir as listdir
        base = 'test/fixtures/csv_import'
        return filter(
            lambda e: isdir('openspending.etl', join(base, e)),
            listdir('openspending.etl', base)
        )

    def test_all_mappings(self):
        for dir in self.testdata_dirs:
            yield self._test_mapping, dir

    def test_all_imports(self):
        for dir in self.testdata_dirs:
            yield self._test_dataset_dir, dir


class TestMappingImporter(DatabaseTestCase):

    def test_empty_mapping(self):
        data = csv_fixture('simple-mapping').read()
        importer = MappingImporter()
        mapping = importer.import_from_string(data)
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
                                      'constant': '',
                                      'datatype': u'string',
                                      'default_value': u'x',
                                      'name': 'label'}],
                          'label': u'y',
                          'type': u'entity'})
        h.assert_equal(mapping['to'],
                         {'description': u'z',
                          'fields': [{'column': u'paid_to',
                                      'constant': '',
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
        data = csv_fixture('nested-mapping').read()
        importer = MappingImporter()
        mapping = importer.import_from_string(data)
        to_fields = mapping['to']['fields']
        h.assert_equal(len(to_fields), 2)
        h.assert_equal(to_fields[0]['column'], u'paid_to')
        h.assert_equal(to_fields[0]['name'], 'label')
        h.assert_equal(to_fields[1]['column'], u'paid_to_identifier')
        h.assert_equal(to_fields[1]['name'], 'identifier')

    def test_line_in_error(self):
        importer = MappingImporter()
        data = csv_fixture('wrong-objecttype-mapping').read()
        try:
            importer.import_from_string(data)
        except ValueError, E:
            errors = E.args[0]
            h.assert_equal(len(errors), 1)
            h.assert_equal(errors[0]['line'], 2)
            return
        raise AssertionError('Missing Exception')

    def test_wrong_objecttype(self):
        importer = MappingImporter()
        data = csv_fixture('wrong-objecttype-mapping').read()
        try:
            importer.import_from_string(data)
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
