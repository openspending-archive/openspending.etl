import re

from .base import PreservingMappingSchema, Function, SchemaNode, String
from .currency import CurrencyCode

def _dataset_name(name):
    if not re.match(r"^[\w\-\_]+$", name):
        return ("Dataset name must include only "
                "letters, numbers, dashes and underscores")
    return True


class Dataset(PreservingMappingSchema):
    name = SchemaNode(String(), validator=Function(_dataset_name))
    label = SchemaNode(String())
    description = SchemaNode(String())
    currency = SchemaNode(CurrencyCode())