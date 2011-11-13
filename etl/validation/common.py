from colander import SchemaNode, Function, String, Mapping, Sequence
from colander import MappingSchema, SequenceSchema


class ValidationState(object):
    """ ValidationState is carried through the validation system to
    allow different parts of the validators to have access to parts
    of the model which are out of their scope. """

    def __init__(self, model):
        self.model = model

    @property
    def attributes(self):
        """ Return all attributes (including measures, atteribute 
        dimensions and compound dimension attributes) of the model. 
        """
        for prop, meta in self.model.get('mapping', {}).items():
            yield prop
            for field in meta.get('fields', []):
                yield prop + '.' + field['name']

def _node(schema, name, *children, **kw):
    if 'validator' in kw:
        kw['validator'] = Function(kw['validator'])
    return SchemaNode(schema,
                      *children,
                      name=name,
                      **kw)

def mapping(name, **kw):
    return _node(Mapping(unknown='preserve'),
                 name=name, **kw)

def sequence(name, *children, **kw):
    return _node(Sequence(), name, 
                 *children, **kw)

def key(name, **kw):
    return _node(String(), name, **kw)

def chained(*validators):
    def _validator(value):
        for validator in validators:
            res = validator(value)
            if res is not True:
                return res
        return True
    return _validator
