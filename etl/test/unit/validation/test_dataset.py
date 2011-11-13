from openspending.etl.validation.dataset import Dataset, dataset_schema
from colander import Invalid 

from ... import TestCase, helpers as h
from . import validation_errors

from openspending.etl.validation.common import ValidationState

class TestDatasetNew(TestCase):

    def setup(self):
        self.model = h.model_fixture('default')
        self.state = ValidationState(self.model)

    def test_basic_validate(self):
        try:
            ds = self.model['dataset']
            schema = dataset_schema(ds, self.state)
            out = schema.deserialize(ds)
            assert out.keys()==ds.keys(), out
        except Invalid, i:
            assert False, i.asdict()
    
    @h.raises(Invalid)
    def test_underscore_validate(self):
        ds = self.model['dataset'].copy()
        ds['name'] = 'test__'
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)
    
    @h.raises(Invalid)
    def test_reserved_name_validate(self):
        ds = self.model['dataset'].copy()
        ds['name'] = 'entry'
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)
    
    @h.raises(Invalid)
    def test_invalid_currency(self):
        ds = self.model['dataset'].copy()
        ds['currency'] = 'glass pearls'
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)
    
    @h.raises(Invalid)
    def test_no_label(self):
        ds = self.model['dataset'].copy()
        del ds['label']
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)

    @h.raises(Invalid)
    def test_empty_label(self):
        ds = self.model['dataset'].copy()
        ds['label'] = '  '
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)
    
    @h.raises(Invalid)
    def test_no_description(self):
        ds = self.model['dataset'].copy()
        del ds['description']
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)
    
    @h.raises(Invalid)
    def test_empty_description(self):
        ds = self.model['dataset'].copy()
        ds['description'] = '  '
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)
    
    @h.raises(Invalid)
    def test_invalid_unique_key(self):
        ds = self.model['dataset'].copy()
        ds['unique_keys'].append("banana")
        schema = dataset_schema(ds, self.state)
        schema.deserialize(ds)

class TestDataset(TestCase):
    def test_empty(self):
        errs = validation_errors(Dataset, '{}')
        h.assert_equal(errs, {
            'name': 'Required',
            'label': 'Required',
            'description': 'Required',
            'currency': 'Required',
            'unique_keys': 'Required'
        })

    def test_name_badcharacters(self):
        errs = validation_errors(Dataset, '{"name": "Not Valid"}')
        h.assert_true(
            "Dataset name must include only" in errs.get('name'),
            "'Dataset name must include only' not in validation errors!"
        )

    def test_currency_valid(self):
        errs = validation_errors(Dataset, '{"currency": "GBP"}')
        h.assert_false(
            errs.get('currency'),
            "A valid currency raised a validation error!"
        )

    def test_currency_invalid(self):
        errs = validation_errors(Dataset, '{"currency": "bad-currency-code"}')
        h.assert_true(
            "currency" in errs.get('currency'),
            "'currency' not in validation errors!"
        )

    def test_unique_keys_length(self):
        errs = validation_errors(Dataset, '{"unique_keys": []}')
        h.assert_true(
            "at least one" in errs.get('unique_keys'),
            "'at least one' not in validation errors!"
        )

    def test_valid(self):
        # This will raise if any errors are found.
        Dataset().deserialize({
            "name": "valid-name_for-dataset123",
            "label": "Dataset label",
            "description": "Some description",
            "currency": "CAD",
            "unique_keys": ['one']
        })
