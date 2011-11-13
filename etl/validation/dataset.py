import re

from .base import (PreservingMappingSchema, Function, SchemaNode,
                   SequenceSchema, String)
from .currency import CURRENCIES


RESERVED_TERMS = ['entry', 'entries', 'dataset', 'datasets', 'dimension',
                  'dimensions', 'editor', 'meta', 'id', 'login', 'logout',
                  'settings', 'browser', 'explorer', 'member', 'register',
                  'after_login', 'after_logout', 'locale', 'reporterror',
                  'getinvolved', 'api', '500', 'error']

def reserved_name(name):
    if name.lower() in RESERVED_TERMS:
        return "'%s' is a reserved word and cannot be used here" % name
    return True

def _dataset_name(name):
    if not re.match(r"^[\w\-\_]+$", name):
        return ("Dataset name must include only "
                "letters, numbers, dashes and underscores")
    return True

def _unique_keys(keys):
    if not keys or not len(keys) > 0:
        return "You must specify at least one unique key!"

    return True

def _valid_currency(code):
    if code.upper() not in CURRENCIES:
        return "%s is not a valid currency code." % code
    return True

class UniqueKeys(SequenceSchema):
    key = SchemaNode(String())

class Dataset(PreservingMappingSchema):
    name = SchemaNode(String(), validator=Function(_dataset_name))
    label = SchemaNode(String())
    description = SchemaNode(String())
    currency = SchemaNode(String(), validator=Function(_valid_currency))
    unique_keys = UniqueKeys(validator=Function(_unique_keys))


from common import mapping, sequence, key, chained

def nonempty_string(text):
    if not isinstance(text, basestring):
        return "Must be text, not %s" % type(text)
    if not len(text.strip()):
        return "Must have at least one non-whitespace character."
    return True

def no_double_underscore(name):
    if '__' in name:
        return "Double underscores are not allowed in dataset names."
    return True

def valid_currency(code):
    if code.upper() not in CURRENCIES:
        return "%s is not a valid currency code." % code
    return True

def unique_keys_are_attributes(state):
    attributes = list(state.attributes)
    def _check(value):
        if value not in attributes:
            return "Invalid attribute in unique keys: %s" % value
        return True
    return _check

def dataset_schema(dataset, state):
    schema = mapping('dataset')
    schema.add(key('name', validator=chained(
            nonempty_string,
            reserved_name,
            _dataset_name,
            no_double_underscore
        )))
    schema.add(key('currency', validator=chained(
            valid_currency
        )))
    schema.add(key('label', validator=chained(
            nonempty_string,
        )))
    schema.add(key('description', validator=chained(
            nonempty_string,
        )))
    schema.add(sequence('unique_keys',
        key('key', validator=chained(
            unique_keys_are_attributes(state),
        )), missing=[]))
    return schema
