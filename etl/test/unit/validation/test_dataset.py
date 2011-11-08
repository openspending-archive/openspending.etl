from openspending.etl.validation.dataset import Dataset

from ... import TestCase, helpers as h
from . import validation_errors

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