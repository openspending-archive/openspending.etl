import colander

from openspending.etl.ui.forms.sources import DATATYPE_NAMES, DIMENSION_TYPES


class FieldDetails(colander.MappingSchema):
    datatype = colander.SchemaNode(colander.String(),
                                   validator=colander.OneOf(DATATYPE_NAMES))
    default = colander.SchemaNode(colander.String())
    objecttype = colander.SchemaNode(colander.String(),
                                     validator=colander.OneOf(DIMENSION_TYPES))
    label = colander.SchemaNode(colander.String())


class Field(colander.MappingSchema):
    field_name = colander.SchemaNode(colander.String())
    field_details = FieldDetails()


class Mapping(colander.SequenceSchema):
    field = Field()


def validate_mapping(mapping):
    schema = Mapping()
    try:
        schema.deserialize(mapping)
    except colander.Invalid, e:
        errors = e.asdict()
        print errors
        raise
