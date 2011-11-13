from .base import Function, PreservingMappingSchema, SequenceSchema
from . import mapping

class View(PreservingMappingSchema):
    pass

class Views(SequenceSchema):
    view = View()

class Model(PreservingMappingSchema):
    #dataset = Dataset()
    mapping = mapping.make_validator()
    views = Views(missing=[])

def make_validator():
    return Model(name='model', validator=Function(_validate))

def _validate(model):
    # Ensure that all unique_keys' first components (i.e. 'section' for
    # a unique_keys entry of 'section.label') are fields in the mapping.
    unique_keys = set(m.split('.')[0] for m in model['dataset']['unique_keys'])
    fields = set(model['mapping'].keys())

    if not unique_keys <= fields:
        return ("Invalid unique keys: %s -- unique keys "
                "must be field names") % \
               list(unique_keys - fields)

    # Ensure that any breakdown or dimension keys in views refer to
    # dimension types.
    def _is_dimension(f):
        return (model['mapping'][f]['type'] in ('entity', 'classifier'))

    dimensions = set(filter(_is_dimension, fields))
    dimensions.add('dataset')

    for view in model['views']:
        if view['dimension'] not in dimensions:
            return ("View cannot have dimension key '%s' (it's not "
                    "a dimension)" % view['dimension'])

        if view['breakdown'] not in dimensions:
            return ("View cannot have breakdown key '%s' (it's not "
                    "a dimension)" % view['breakdown'])

    return True
