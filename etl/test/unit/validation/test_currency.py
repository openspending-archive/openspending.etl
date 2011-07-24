from openspending.etl.test import TestCase, helpers as h
from openspending.etl.validation import Invalid, currency

class TestCurrency(TestCase):
    def test_currency_constant(self):
        h.assert_equal(currency.CURRENCIES['EUR'],
                       {'code': 'EUR', 'name': 'Euro'})
        h.assert_equal(currency.CURRENCIES['USD'],
                       {'code': 'USD', 'name': 'US Dollar'})

    @h.raises(Invalid)
    def test_currency_type_raises_invalid(self):
        currency.CurrencyCode().deserialize(None, 'not-a-code')

    def test_currency_type_returns_valid(self):
        res = currency.CurrencyCode().deserialize(None, 'usd')
        h.assert_equal(res, "USD")