import logging

from unidecode import unidecode

from openspending.lib import solr_util as solr
from openspending import model

from openspending.etl import times
from openspending.etl import validation
from openspending.etl.loader import Loader

log = logging.getLogger(__name__)

class ImporterError(Exception):
    pass


class ModelValidationError(ImporterError):
    def __init__(self, colander_exc):
        self.colander_exc = colander_exc

    def __str__(self):
        msg = []
        msg.append("These errors were found when attempting to validate your " \
                   "model:")
        for k, v in self.colander_exc.asdict().iteritems():
            msg.append("  - '%s' field had error '%s'" % (unidecode(k), unidecode(v)))

        return "\n".join(msg)


class DataError(ImporterError):
    def __init__(self, exception, line_number=None, source_file=None):
        self.exception = exception
        self.line_number = line_number
        self.source_file = source_file

        if isinstance(exception, validation.Invalid):
            msg = ["Validation error:"]
            for k, v in exception.asdict().iteritems():
                msg.append("  - '%s' field had error '%s'" % (unidecode(k), unidecode(v)))
            self.message = "\n".join(msg)
        elif isinstance(exception, Exception):
            # The message attribute is deprecated for Python 2.6 BaseExceptions.
            self.message = str(exception)
        else:
            self.message = repr(exception)

    def __str__(self):
        return "Line %s: %s" % (self.line_number, self.message)

    def __repr__(self):
        return "<DataError (message='%s', file=%s, line=%d)>" \
            % (self.message, self.source_file, self.line_number)

class TooManyErrorsError(ImporterError):
    pass


class BaseImporter(object):

    dimension_types = ('entity', 'classifier')

    def __init__(self, data, model, source_file="<stream>"):
        self.data = data
        self.model = model
        self.model_valid = None
        self.source_file = source_file
        self.errors = []
        self.on_error = lambda e: log.warn(e)
        self._generate_fields()

    def run(self,
            dry_run=False,
            max_errors=None,
            max_lines=None,
            raise_errors=False,
            build_indices=True):

        self.dry_run = dry_run
        self.max_errors = max_errors
        self.do_build_indices = build_indices
        self.raise_errors = raise_errors

        self.validate_model()
        self.describe_dimensions()

        self.validator = validation.entry.make_validator(self.fields)

        self.line_number = 0

        for line_number, line in enumerate(self.lines, start=1):
            if max_lines and line_number > max_lines:
                break

            self.line_number = line_number
            self.process_line(line)

        if self.line_number == 0:
            self.add_error("Didn't read any lines of data")

        self.generate_views()
        self.build_indices()

        if self.errors:
            log.error("Finished import with %d errors:", len(self.errors))
            for err in self.errors:
                log.error(" - %s", err)
        else:
            log.info("Finished import with no errors!")

    @property
    def lines(self):
        raise NotImplementedError("lines not implemented in BaseImporter")

    @property
    def mapping(self):
        return self.model['mapping']

    @property
    def views(self):
        return self.model.get('views', [])

    def validate_model(self):
        if self.model_valid:
            return

        log.info("Validating model")
        try:
            model_validator = validation.model.make_validator()
            self.model = model_validator.deserialize(self.model)
            self.model_valid = True
        except validation.Invalid as e:
            raise ModelValidationError(e)

    def describe_dimensions(self):
        if self.dry_run:
            return False

        log.info("Describing dimensions")
        for dimension, mapping in self.mapping.iteritems():
            # Don't describe "measures"
            if mapping.get('type') not in self.dimension_types:
                continue

            self.loader.create_dimension(
                dimension,
                mapping.get("label"),
                type=mapping.get('type'),
                datatype=mapping.get('datatype'),
                fields=mapping.get('fields', []),
                facet=mapping.get('facet'),
                description=mapping.get("description")
            )

    def generate_views(self):
        if self.dry_run:
            return False

        log.info("Generating aggregates and views")
        self.loader.flush_aggregates()
        for view in self.views:
            entity_cls = getattr(model, view.get('entity'))
            self.loader.create_view(
                entity_cls,
                view.get('filters', {}),
                name=view.get('name'),
                label=view.get('label'),
                dimension=view.get('dimension'),
                breakdown=view.get('breakdown'),
                view_filters=view.get('view_filters', {})
            )
        self.loader.compute_aggregates()

    def build_indices(self):
        if self.dry_run or not self.do_build_indices:
            return False

        log.info("Building search indices")
        solr.drop_index(self.model['dataset']['name'])
        solr.build_index(self.model['dataset']['name'])

    @property
    def loader(self):
        if not hasattr(self, '_loader'):
            dataset = self.model.get('dataset').copy()

            self._loader = Loader(
                dataset_name=dataset.get('name'),
                unique_keys=dataset.get('unique_keys', ['_csv_import_fp']),
                label=dataset.get('label'),
                description=dataset.pop('description'),
                currency=dataset.pop('currency'),
                time_axis=times.GRANULARITY.get(dataset.get(
                    'temporal_granularity',
                    'year'
                )),
                metadata=dataset
            )
        return self._loader

    def process_line(self, line):
        if self.line_number % 1000 == 0:
            log.info('Imported %s lines' % self.line_number)

        try:
            _line = self.validator.deserialize(line)
            if not self.dry_run:
                self.import_line(_line)
        except (validation.Invalid, ImporterError) as e:
            if self.raise_errors:
                raise
            else:
                self.add_error(e)

    def import_line(self, line):
        raise NotImplementedError("import_line not implemented in BaseImporter")

    def add_error(self, exception):
        err = DataError(exception=exception,
                        line_number=self.line_number,
                        source_file=self.source_file)

        self.on_error(err)
        self.errors.append(err)

        if self.max_errors and len(self.errors) >= self.max_errors:
            all_errors = "".join(map(lambda x: "\n  " + str(x), self.errors))
            raise TooManyErrorsError("The following errors occurred:" + all_errors)

    def _generate_fields(self):
        def _field(dimension, mapping, column_name, is_end=False):
            return {
                'dimension': dimension,
                'field': mapping.get(column_name),
                'datatype': mapping.get('datatype'),
                'is_end': is_end
            }

        fields = []

        for dimension, mapping in self.mapping.items():
            if mapping.get('type') == 'value':
                fields.append(_field(dimension, mapping, 'column'))

                if mapping.get('end_column'):
                    fields.append(_field(dimension,
                                         mapping,
                                         'end_column',
                                         True))
            else:
                for field in mapping.get('fields', []):
                    fields.append(_field(dimension, field, 'column'))

        self.fields = fields
