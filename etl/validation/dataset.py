import re

from .base import (PreservingMappingSchema, Function, SchemaNode,
                   SequenceSchema, String)
from .currency import CurrencyCode

def _dataset_name(name):
    if not re.match(r"^[\w\-\_]+$", name):
        return ("Dataset name must include only "
                "letters, numbers, dashes and underscores")
    return True

def _unique_keys(keys):
    if not keys or not len(keys) > 0:
        return "You must specify at least one unique key!"

    return True

class UniqueKeys(SequenceSchema):
    key = SchemaNode(String())

class Dataset(PreservingMappingSchema):
    name = SchemaNode(String(), validator=Function(_dataset_name))
    label = SchemaNode(String())
    description = SchemaNode(String())
    currency = SchemaNode(CurrencyCode())
    unique_keys = UniqueKeys(validator=Function(_unique_keys))