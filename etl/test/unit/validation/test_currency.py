from openspending.etl.validation import Invalid, currency
from openspending.etl.validation.dataset import _valid_currency

from ... import TestCase, helpers as h

class TestCurrency(TestCase):
    def test_currency_constant(self):
        h.assert_equal(currency.CURRENCIES['EUR'], 'Euro')
        h.assert_equal(currency.CURRENCIES['USD'], 'US Dollar')

    def test_currency_type_raises_invalid(self):
        assert _valid_currency('not-a-code') is not True

    def test_currency_type_returns_valid(self):
        assert _valid_currency('usd') is True
