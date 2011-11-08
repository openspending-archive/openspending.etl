import re

from .base import (Boolean, OneOf, Invalid, Function,
                   PreservingMappingSchema, SchemaNode, SequenceSchema,
                   String)

DATATYPE_NAMES = ['id', 'string', 'float', 'constant', 'date', 'currency']
DIMENSION_TYPES = ['classifier', 'entity', 'value', 'measure', 'date']

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
    

class Field(PreservingMappingSchema):
    name = SchemaNode(String(), validator=Function(_dimension_name))
    column = SchemaNode(String(), missing=None)
    end_column = SchemaNode(String(), missing=None)
    facet = SchemaNode(Boolean(), missing=False)
    constant = SchemaNode(String(), missing=None)
    datatype = SchemaNode(String(), validator=OneOf(DATATYPE_NAMES))


class Fields(SequenceSchema):
    field = Field()


# TODO: We really want to ensure that a dimension has one and only one of
# {column, constant, fields}. Colander may not be up to this, as validators can
# only know about their nodes, and not their nodes' siblings.
class Dimension(PreservingMappingSchema):
    label = SchemaNode(String(), missing=None)
    description = SchemaNode(String(), missing=None)
    type = SchemaNode(String(), validator=OneOf(DIMENSION_TYPES))
    fields = Fields(missing=None)


class AmountDimension(PreservingMappingSchema):
    label = SchemaNode(String(), missing=None)
    description = SchemaNode(String(), missing=None)
    column = SchemaNode(String())
    datatype = SchemaNode(String(),
                          missing='float',
                          validator=Function(_specific_type('float')))
    type = SchemaNode(String(),
                      missing='value',
                      validator=Function(_specific_type('value')))


class DateDimension(PreservingMappingSchema):
    label = SchemaNode(String(), missing=None)
    description = SchemaNode(String(), missing=None)
    column = SchemaNode(String())
    datatype = SchemaNode(String(),
                          missing='date',
                          validator=Function(_specific_type('date')))
    type = SchemaNode(String(),
                      missing='value',
                      validator=Function(_specific_type('value')))

class Mapping(PreservingMappingSchema):
    amount = AmountDimension()
    time = DateDimension()
    to = Dimension()
    from_ = Dimension() # `from' is a reserved word

# This cannot be set in the definition, because colander is a bit too clever
# for its own good and will reset the "name" attribute when the MappingSchema
# metaclass ctor is called.
#
# See colander._SchemaMeta.__init__
Mapping.from_.name = "from"

def make_validator():
    return Mapping(validator=Function(_validate))

def _validate(mapping):

    # Ensure that all classifiers possess a taxonomy
    for key, value in mapping.iteritems():
        if value['type'] == 'classifier':
            t = value.get('taxonomy', None)
            if not t:
                return '"%s" (a classifier dimension) must have a "taxonomy" field' % key

    return True
