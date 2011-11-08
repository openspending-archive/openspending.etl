from openspending.etl.validation.mapping import (
    Dimension, AmountDimension, DateDimension, Field, Fields, Mapping
)


from ... import TestCase, helpers as h
from . import validation_result, validation_errors

class TestField(TestCase):
    def test_blank_errors(self):
        errs = validation_errors(Field, '{}')
        h.assert_equal(errs, {
            'name': 'Required',
            'datatype': 'Required'
        })

class TestFields(TestCase):
    def test_blank(self):
        res = validation_result(Fields, '[]')
        h.assert_equal(res, [])

class TestDimension(TestCase):
    def test_blank_errors(self):
        errs = validation_errors(Dimension, '{}')
        h.assert_equal(errs, {
            'type': 'Required'
        })

class TestAmountDimension(TestCase):
    def test_blank_errors(self):
        errs = validation_errors(AmountDimension, '{}')
        h.assert_equal(errs, {
            'column': 'Required'
        })

class TestDateDimension(TestCase):
    def test_blank_errors(self):
        errs = validation_errors(DateDimension, '{}')
        h.assert_equal(errs, {
            'column': 'Required'
        })

class TestMapping(TestCase):
    def test_blank_errors(self):
        errs = validation_errors(Mapping, '{}')
        h.assert_equal(errs, {
            'from': 'Required',
            'to': 'Required',
            'time': 'Required',
            'amount': 'Required'
        })

