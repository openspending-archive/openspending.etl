import re

from colander import SchemaNode, String, Invalid

from openspending.etl.validation.base import Mapping, SchemaNode, String, Invalid
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

    def _column_or_default(self, row, meta):
        """ Utility function to handle using either the column 
        field or the default value specified. """
        value = row.get(meta.get('column'))
        if not value and meta.get('default_value', '').strip():
            value = meta.get('default_value').strip()
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
        value = self._column_or_default(row, meta) or ""
        return unicode(value)

class IdentifierAttributeType(StringAttributeType):
    """ Type for slug fields, i.e. attributes that will be 
    converted to a URI-compatible representation. """

    def cast(self, row, meta):
        value = self._column_or_default(row, meta)
        return slugify(value)

class FloatAttributeType(AttributeType):
    """ Accept floating point values with commas as thousands
    delimiters (anglo-saxon style). """

    RE = re.compile(r'^[0-9-\.,]+$')

    def test(self, row, meta):
        value = unicode(self._column_or_default(row, meta))
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
        value = unicode(self._column_or_default(row, meta)) or "0.0"
        return float(value.replace(",", ""))

class DateAttributeType(AttributeType):
    """ Date parsing. """
    # TODO: simplify this, its hell!
    SUFFIX = ('in the format "yyyy-mm-dd", "yyyy-mm" or "yyyy", '
              'e.g. "2011-12-31".')

    def test(self, row, meta):
        value = unicode(self._column_or_default(row, meta))
        end_value = row.get(meta.get('end_column'))
        for value, is_end in [(value, False), (end_value, True)]:
            column = meta.get('end_column' if is_end else 'column')
            if meta['dimension'] != 'time':
                msg = '"%s" can be empty or a value %s' % (column, self.SUFFIX)
                if not value:
                    continue
            else:
                if is_end and not value:
                    continue
            msg = ('"time" (here "%s") has to be %s. The "end_column", if specified, '
                   'might be empty') % (value, self.SUFFIX)
            try:
                for_datestrings(value)
            except:
                return msg
        return True

    
    def cast(self, row, meta):
        value = unicode(self._column_or_default(row, meta))
        end_value = row.get(meta.get('end_column'))
        # TODO: former implementation had the following logic which 
        # this does not fully reproduce:
        #if not value or value == PLACEHOLDER:
        #        if not default:
        #            return EMPTY_DATE
        #        else:
        #            value = default
        if not value:
            value = EMPTY_DATE
        return for_datestrings(value, end_value)


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
