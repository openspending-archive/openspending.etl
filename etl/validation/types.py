import re
from datetime import datetime

from colander import SchemaNode, String, Invalid, Mapping

from openspending.etl.util import slugify
from openspending.etl.times import for_datestrings, EMPTY_DATE

class AttributeType(object):
    """ A attribute type maintains information about the parsing
    and conversion operations possible on the attribute, providing
    methods to check if a type is applicable to a given value and
    to convert a value to the type. """

    def test(self, row, meta):
        """ Test if the value is of the given type. The
        default implementation calls ``cast`` and checks if
        that throws an exception. If the conversion passes, True
        is returned. Otherwise, a message is given back. """
        try:
            self.cast(row, meta)
            return True
        except Exception, e:
            return repr(e)

    def cast(self, row, meta):
        """ Convert the value to the type. This may throw
        a quasi-random exception if conversion fails (i.e. it is
        assumed that validation was performed before and errors
        were already handled. """
        raise TypeError("No casting method defined!")

    def _column_or_default(self, row, meta, fallback):
        """ Utility function to handle using either the column 
        field or the default value specified. """
        value = row.get(meta.get('column'))
        if not value and meta.get('default_value', '').strip():
            value = meta.get('default_value').strip()
        elif not value:
            value = fallback
        return value

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __hash__(self):
        return hash(self.__class__)

    def __repr__(self):
        return self.__class__.__name__.rsplit('Type', 1)[0]

class ConstantAttributeType(AttributeType):
    """ Constant values come from the model rather than from 
    the actual source data. """

    def test(self, row, meta):
        return True

    def cast(self, row, meta):
        return meta.get('constant')

class StringAttributeType(AttributeType):
    """ Test if the given values can be represented as a 
    string. """

    def test(self, row, meta):
        return True

    def cast(self, row, meta):
        value = self._column_or_default(row, meta, "")
        return unicode(value)

class IdentifierAttributeType(StringAttributeType):
    """ Type for slug fields, i.e. attributes that will be 
    converted to a URI-compatible representation. """

    def cast(self, row, meta):
        value = self._column_or_default(row, meta, "")
        if not len(value):
            raise ValueError()
        return slugify(value)

class FloatAttributeType(AttributeType):
    """ Accept floating point values with commas as thousands
    delimiters (anglo-saxon style). """

    RE = re.compile(r'^[0-9-\.,]+$')

    def test(self, row, meta):
        value = unicode(self._column_or_default(row, meta, "0.0"))
        if not self.RE.match(value):
            return ("Numbers must only contain digits, periods, "
                    "dashes and commas: '%s'" % value)
        try:
            self.cast(row, meta)
            return True
        except:
            return "Could not coerce value into a number: '%s'" \
                    % value

    def cast(self, row, meta):
        value = unicode(self._column_or_default(row, meta, "0.0"))
        return float(value.replace(",", ""))

class DateAttributeType(AttributeType):
    """ Date parsing. """
    # TODO: simplify this, its hell!
    SUFFIX = ('in the format "yyyy-mm-dd", "yyyy-mm" or "yyyy", '
              'e.g. "2011-12-31".')

    def test(self, row, meta):
        # version with end_column: https://gist.github.com/1261320
        try:
            self.cast(row, meta)
            return True
        except ValueError:
            value = unicode(self._column_or_default(row, meta, ""))
            if meta['dimension'] != 'time':
                if not value:
                    return True
                return '"%s" can be empty or a value %s' % (
                        meta.get('column'), self.SUFFIX)
            return '"time" (here "%s") has to be %s.' % (value, self.SUFFIX)

    def cast(self, row, meta):
        value = unicode(self._column_or_default(row, meta, ""))
        if value:
            for format in ["%Y-%m-%d", "%Y-%m", "%Y"]:
                try:
                    return datetime.strptime(value, format).date()
                except ValueError: pass
        elif meta['dimension'] != 'time':
            # ugly logic rule #3983:
            return None
        raise ValueError()


ATTRIBUTE_TYPES = {
    'constant': ConstantAttributeType(),
    'string': StringAttributeType(),
    'id': IdentifierAttributeType(),
    'float': FloatAttributeType(),
    'date': DateAttributeType()
    }

def attribute_type_by_name(name):
    """ Get an attribute type by its name. """
    name = name.lower().strip()
    return ATTRIBUTE_TYPES.get(name, StringAttributeType())

def make_validator(fields):
    """ Create a data validator, working on a per-row basis. """
    fields = [(f, attribute_type_by_name(f['datatype'])) \
              for f in fields]

    def collector(node, value):
        # the collector will call each cell validator with the whole
        # row as its input, so validators can use multiple columns as
        # input.
        errors = Invalid(node)
        for field, type_ in fields:
            result = type_.test(value, field)
            if result is not True:
                child = SchemaNode(String(), name=field.get('column'))
                errors.add(Invalid(child, result))
        if len(errors.children):
            raise errors

    schema = SchemaNode(Mapping(unknown='preserve'),
            validator=collector)
    return schema
