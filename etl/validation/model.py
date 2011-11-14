from openspending.etl.validation.common import mapping, \
        ValidationState
from openspending.etl.validation.dataset import dataset_schema
from openspending.etl.validation.mapping import mapping_schema
from openspending.etl.validation.views import views_schema

def model_schema(state):
    schema = mapping('model')
    schema.add(dataset_schema(state))
    schema.add(mapping_schema(state))
    schema.add(views_schema(state))
    return schema

def validate_model(model):
    """ Apply model validation. """
    state = ValidationState(model)
    schema = model_schema(state)
    return schema.deserialize(model)
