import logging
from datetime import datetime
from urllib import urlopen
from itertools import islice

import colander

from openspending.lib import util
from openspending.lib import unicode_dict_reader as udr
from openspending.lib.solr_util import build_index, drop_index
from openspending.model import Dataset, Classifier, Entity
from openspending.ui.lib.times import for_datestrings, EMPTY_DATE, GRANULARITY

from openspending.etl.ilines import ilines
from openspending.etl.loader import Loader
from openspending.etl.ui.forms.sources import Mapping as MappingForm
from openspending.etl.ui.forms.sources import Dataset as DatasetForm
from openspending.etl.ui.forms.entry import make_validator, PLACEHOLDER

log = logging.getLogger(__name__)

ENTITY_TYPES = {
    "dataset": Dataset,
    "entity": Entity,
    "classifier": Classifier
}

DRY_RUN_LINES = 100

# TODO: import some sort of local caching
def resource_lines(resource_url):
    return ilines(urlopen(resource_url))

def load_dataset(resource_url, model,
                 dry_run=False, do_index=True,
                 progress_callback=lambda msg: None,
                 **kwargs):
    '''\
    Load a dataset given path to csv and model or model-like object.

    :return: tuple (True, Dataset object, errors)
    '''

    importer = DatasetImporter(resource_lines(resource_url),
                               model,
                               source_file=resource_url)
    importer.validate_model()
    progress_callback('Validated model OK')

    if dry_run:
        progress_callback('Starting import of data')
        importer.import_data(**kwargs)
        progress_callback('Dry run of import OK')
    else:
        importer.describe_dimensions()
        progress_callback('Described dimensions')
        progress_callback('Starting import of data')
        importer.import_data(**kwargs)
        progress_callback('Completed import of data')
        progress_callback('Now generating aggregates and views')
        importer.generate_views()
        # TODO: probably want this as separate
        if do_index:
            drop_index(model['dataset'].get('name'))
            build_index(model['dataset'].get('name'))
            progress_callback('Building indexes')
        else:
            progress_callback('Skipping indexes')
    return True, importer.loader.dataset, importer.errors


class DatasetImportError(Exception):
    def __init__(self, exception, line_number=None):
        log.warn("DatasetImportError init with: %s, %s" % (exception, line_number))
        self.exception = exception
        self.line_number = line_number
        if isinstance(exception, colander.Invalid):
            self.message = exception.message
        elif isinstance(exception, Exception):
            # The message attribute is deprecated for Python 2.6 BaseExceptions.
            self.message = str(exception)
        else:
            self.message = repr(exception)

    def __repr__(self):
        return "Line %s: %s" % (self.line_number, self.message)

    def __str__(self):
        return self.__repr__()

class LineImportError(Exception):
    def __init__(self, field, exc):
        self.field = field
        self.exc = exc

    def __str__(self):
        return "Column `%s': %s" % (self.field, str(self.exc))


def validate_model(model):
    DatasetForm().deserialize(model.get('dataset'))
    MappingForm().deserialize(model.get('mapping'))

class DatasetImporter(object):
    def __init__(self, fileobj, model, source_file='<stream>'):
        self.fileobj = fileobj
        self.errors = []
        self.source_file = source_file
        self.model = model
        self.mapping = self.model.get('mapping')
        self.views = self.model.get('views', [])
        self.fields = self._fields(self.mapping)

    def validate_model(self):
        DatasetForm().deserialize(self.model.get('dataset'))
        MappingForm().deserialize(self.model.get('mapping'))

    def _fields(self, mapping):

        def _field(dimension, mapping, column_name, is_end=False):
            return dict(dimension=dimension,
                        field=mapping.get(column_name),
                        datatype=mapping.get('datatype'),
                        is_end=is_end,
)
        fields = []
        for dimension, mapping in mapping.items():
            if mapping.get('type') == 'value':
                fields.append(_field(dimension, mapping, 'column'))
                if mapping.get('end_column'):
                    fields.append(_field(dimension, mapping, 'end_column',
                                         True))
            else:
                for field in mapping.get('fields', []):
                    fields.append(_field(dimension, field, 'column'))
        return fields

    @property
    def loader(self):
        if not hasattr(self, '_loader'):
            dataset = self.model.get('dataset').copy()
            time_axis = GRANULARITY.get(dataset.pop('temporal_granularity',
                                                    'year'))
            unique_keys = dataset.pop('unique_keys', ['_csvimport_fp'])
            self._loader = Loader(
                dataset.pop('name'),
                unique_keys,
                dataset.pop('label'),
                description=dataset.pop('description'),
                currency=dataset.pop('currency'),
                time_axis=time_axis,
                metadata=dataset)
        return self._loader

    @property
    def reader(self):
        return udr.UnicodeDictReader(self.fileobj)

    def sanity_check(self):
        """Check to see if there are any obvious errors in the CSV source"""
        try:
            rows = []
            for i, row in enumerate(self.reader):
                if i > 4:
                    return rows
                if len(row.keys()) < 2:
                    raise ValueError("Less than two columns per row")
                for key in row.keys():
                    if not len(key):
                        raise ValueError("A column has no header")
                rows.append(row)
            raise ValueError("Less than 5 lines. Why bother?")
        except Exception as e:
            self.errors.append(e)
            return False

    def import_data(self, dry_run=False, progress_callback=lambda x: None,
                    max_errors=None, reraise_errors=False, max_lines=None):

        class TooManyErrors(Exception): pass

        self.errors = []

        def store_error(e, line_number):
            if reraise_errors:
                raise
            else:
                self.errors.append(DatasetImportError(e, line_number=line_number))

            if max_errors and len(self.errors) > max_errors:
                all_errors = "".join(map(lambda x: "\n  " + str(x), self.errors))
                raise TooManyErrors("The following errors occurred:" + all_errors)

        # should move out from under import_data(), and have all its
        # variables passed by ref
        def process_line(line_number, line):
            if line_number % 1000 == 999:
                progress_callback('Lines imported so far: %s' %
                                  line_number)
            try:
                _line = validator.deserialize(line)
                if not dry_run:
                    self.load_line(_line, (line_number+1), reraise_errors)
            except colander.Invalid as e:
                e.message = "; ".join(map(lambda e: "%s: %s" % e,
                                          e.asdict().items()))
                store_error(e, line_number=(line_number+1))
            except Exception as e:
                # This is a malformed or non utf-8 CSV
                store_error(e, line_number=(line_number+1))

        validator = make_validator(self.fields)

        if dry_run and (max_lines is None):
            max_lines = DRY_RUN_LINES

        if max_lines is None:
            lines = enumerate(self.reader)
        else:
            lines = islice(enumerate(self.reader), max_lines)

        line_number = 0

        try:
            for line_number, line in lines:
                process_line(line_number, line)

        # NB: Please do not replace this with Exception, as it makes it
        # exceedingly hard to tell what went wrong when something truly
        # exceptional happens. Instead only add errors that we know can be
        # caused by bad user input.
        except udr.EmptyCSVError as e:
            store_error(e, line_number=(line_number+1))

        if line_number == 0:
            store_error("Didn't read any lines of CSV input", line_number=(line_number+1))
        elif max_lines and (line_number + 1 > max_lines):
            store_error(
                "Read more than max_lines of input (%d > %d)" %
                ((line_number+1), max_lines),
                line_number=(line_number+1)
            )

    def load_cell(self, entry, line, line_number, dimension, description):
        dimension = str(dimension)
        dimension_type = description.get('type', 'value')
        dimension_value = None

        if dimension_type == 'value':
            value = self.convert_type(line, description)
            entry[dimension] = value
            return

        # 'classifier', 'entity'
        dimension_value = {}
        for field in description['fields']:
            fieldname = str(field['name'])
            dimension_value[fieldname] = self.convert_type(line, field)

        if 'name' in dimension_value:
            name = dimension_value.pop('name')
        else:
            name = dimension_value['label']
        name = util.slugify(name)

        if dimension_type == 'entity':
            match_keys = description.get('match_keys', ('name',))
            entity = self.loader.create_entity(
                name, match_keys=match_keys, **dimension_value)
            self.loader.entitify_entry(entry, entity, dimension)
        elif dimension_type == 'classifier':
            taxonomy = util.slugify(description.get('taxonomy'))
            classifier = self.loader.create_classifier(name, taxonomy,
                                                       **dimension_value)
            self.loader.classify_entry(entry, classifier, dimension)

    def load_line(self, line, line_number, reraise_errors):
        entry = {
                'provenance': {
                    "dataset": self.loader.dataset.name,
                    "source_file": self.source_file,
                    "line": line_number,
                    "timestamp": datetime.utcnow()
                    },
                "_csvimport_fp": self.loader.dataset.name + ":" + \
                    self.source_file + ":" + str(line_number)
                }

        for dimension, description in self.mapping.items():
            try:
                self.load_cell(entry, line, line_number, dimension, description)
            except Exception as e:
                if reraise_errors:
                    raise
                raise LineImportError(dimension, e)

        self.loader.create_entry(**entry)

    def describe_dimensions(self):
        for dimension, mapping in self.mapping.items():
            self.loader.create_dimension(dimension,
                    mapping.get("label"),
                    type=mapping.get('type'),
                    datatype=mapping.get('datatype'),
                    fields=mapping.get('fields', []),
                    facet=mapping.get('facet'),
                    description=mapping.get("description"))

    def generate_views(self):
        self.loader.flush_aggregates()
        for view in self.views:
            entity = ENTITY_TYPES.get(view.get('entity'))
            self.loader.create_view(entity,
                view.get('filters', {}),
                name=view.get('name'),
                label=view.get('label'),
                dimension=view.get('dimension'),
                breakdown=view.get('breakdown'),
                view_filters=view.get('view_filters', {}))
        self.loader.compute_aggregates()

    def convert_type(self, line, description):
        type_string = description.get('datatype', 'value')
        value = line.get(description.get('column'))

        if not value:
            if description.get('default_value', '').strip():
                value = description.get('default_value').strip()
        if type_string == "constant":
            return description.get('constant')
        if value is None:
            return
        if type_string == "date":
            default = description.get('default_value')
            if not value or value == PLACEHOLDER:
                if not default:
                    return EMPTY_DATE
                else:
                    value = default
            end_value = line.get(description.get('end_column'))
            return for_datestrings(value, end_value)
        if type_string == "string":
            return value
        elif type_string == "float":
            return float(unicode(value).replace(",", ""))
        elif type_string == "id":
            return util.slugify(value)
        return value
