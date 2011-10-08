from openspending.etl.validation.mapping import (
    Dimension, AmountDimension, DateDimension, Field, Fields, Mapping
)


from ... import TestCase, helpers as h
from openspending.etl.validation import types

class TestTypes(TestCase):

    def test_convert_types_value(self):
        mapping = {
                    "foo": {"column": "foo", 
                           "datatype": "string"}
                  }
        row = {"foo": "bar"}
        out = types.convert_types(mapping, row)
        assert isinstance(out, dict), out
        assert 'foo' in out, out
        assert out['foo']=='bar'

    def test_convert_types_compound(self):
        mapping = {
                    "foo": {"fields": [
                        {"name": "name", "column": "foo_name", 
                            "datatype": "string"},
                        {"name": "label", "column": "foo_label", 
                            "datatype": "string"}
                        ]
                    }
                  }
        row = {"foo_name": "bar", "foo_label": "qux"}
        out = types.convert_types(mapping, row)
        assert isinstance(out, dict), out
        assert 'foo' in out, out
        assert isinstance(out['foo'], dict), out
        assert out['foo']['name']=='bar'
        assert out['foo']['label']=='qux'

    def test_convert_types_casting(self):
        mapping = {
                    "foo": {"column": "foo", 
                           "datatype": "float"}
                  }
        row = {"foo": "5.0"}
        out = types.convert_types(mapping, row)
        assert isinstance(out, dict), out
        assert 'foo' in out, out
        assert out['foo']==5.0

    def test_convert_types_compound_no_name(self):
        mapping = {
                    "foo": {"fields": [
                        {"name": "label", "column": "foo_label", 
                            "datatype": "string"}
                        ]
                    }
                  }
        row = {"foo_label": "My Label"}
        out = types.convert_types(mapping, row)
        assert isinstance(out, dict), out
        assert 'foo' in out, out
        assert isinstance(out['foo'], dict), out
        assert out['foo']['name']=='my-label'

