import re

from .base import PreservingMappingSchema, Function, SchemaNode, String
from .currency import CurrencyCode

def _dataset_name(name):
    if not name:
        return "Dataset name must not be empty"

    if not re.match(r"[\w\-]+", name):
        return ("Dataset name must include only "
                "letters, numbers and dashes")
    return True


class Dataset(PreservingMappingSchema):
    name = SchemaNode(String(), validator=Function(_dataset_name))
    label = SchemaNode(String())
    description = SchemaNode(String())
    currency = SchemaNode(CurrencyCode())