import colander

from .base import Function, PreservingMappingSchema, SequenceSchema

from .dataset import Dataset
from . import mapping

class View(PreservingMappingSchema):
    pass

class Views(SequenceSchema):
    view = View()

class Model(PreservingMappingSchema):
    dataset = Dataset()
    mapping = mapping.make_validator()
    views = Views(missing=[])

def make_validator():
    return Model(name='model', validator=Function(_validate))

def _validate(model):
    # Ensure that all unique_keys' first components (i.e. 'section' for
    # a unique_keys entry of 'section.label') are fields in the mapping.
    unique_keys = set(m.split('.')[0] for m in model['dataset']['unique_keys'])
    dimensions = set(model['mapping'].keys())
    if not unique_keys <= dimensions:
        return ("Invalid unique keys: %s -- unique keys "
                "must be dimension names") % \
               list(unique_keys - dimensions)

    return True
