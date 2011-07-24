import re

from .base import (Boolean, OneOf, Function, PreservingMappingSchema,
                   SchemaNode, SequenceSchema, String)

DATATYPE_NAMES = ['id', 'string', 'float', 'constant', 'date', 'currency']
DIMENSION_TYPES = ['classifier', 'entity', 'value']

def _dimension_name(name):
    if name in ['_id', 'classifiers', 'classifier_ids']:
        return u"Reserved dimension name: %s" % name
    if not re.match(r"\w+", name):
        return u"Invalid dimension name: %s" % name
    return True

def _specific_type(t):
    def _check(n):
        if not t == n:
            return u"'%s' is not required type '%s'" % (n, t)
        return True
    return _check

class Dimension(PreservingMappingSchema):
    label = SchemaNode(String(), missing=None)
    description = SchemaNode(String(), missing=None)
    type = SchemaNode(String(), validator=OneOf(DIMENSION_TYPES))

class DateDimension(Dimension):
    column = SchemaNode(String())
    datatype = SchemaNode(String(), validator=Function(_specific_type('date')))
    type = SchemaNode(String(),
                      missing='value',
                      validator=Function(_specific_type('value')))


class AmountDimension(Dimension):
    column = SchemaNode(String())
    datatype = SchemaNode(String(), validator=Function(_specific_type('float')))
    type = SchemaNode(String(),
                      missing='value',
                      validator=Function(_specific_type('value')))


class DimensionAttribute(PreservingMappingSchema):
    name = SchemaNode(String(), validator=Function(_dimension_name))
    column = SchemaNode(String(), missing=None)
    end_column = SchemaNode(String(), missing=None)
    facet = SchemaNode(Boolean(), missing=False)
    constant = SchemaNode(String(), missing=None)
    datatype = SchemaNode(String(), validator=OneOf(DATATYPE_NAMES))


class Attributes(SequenceSchema):
    attribute = DimensionAttribute()


class EntityDimension(Dimension):
    fields = Attributes()


class Mapping(PreservingMappingSchema):
    amount = AmountDimension()
    time = DateDimension()
    to = EntityDimension()

# 'from' is a reserved word
setattr(Mapping, 'from', EntityDimension())