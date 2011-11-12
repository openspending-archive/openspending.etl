from colander import (Boolean, Invalid, Function, Mapping, MappingSchema,
                      OneOf, Regex, SchemaNode, SequenceSchema, String,
                      required, null)

RESERVED_TERMS = ['entry', 'entries', 'dataset', 'datasets', 'dimension',
                  'dimensions', 'editor', 'meta', 'id', 'login', 'logout',
                  'settings', 'browser', 'explorer', 'member', 'register',
                  'after_login', 'after_logout', 'locale', 'reporterror',
                  'getinvolved', 'api', '500', 'error']


class PreservingMappingSchema(MappingSchema):
    @classmethod
    def schema_type(cls):
        return Mapping(unknown='preserve')
