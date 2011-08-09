from colander import (Boolean, Invalid, Function, Mapping, MappingSchema,
                      OneOf, Regex, SchemaNode, SequenceSchema, String,
                      required, null)

PLACEHOLDER = "(Empty)"

class PreservingMappingSchema(MappingSchema):
    @classmethod
    def schema_type(cls):
        return Mapping(unknown='preserve')