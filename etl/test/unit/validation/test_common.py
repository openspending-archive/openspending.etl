
from ... import TestCase, helpers as h
from openspending.etl.validation.common import ValidationState




class TestValidationState(TestCase):

    def setup(self):
        self.state = ValidationState(h.model_fixture('default'))

    def test_list_attributes(self):
        attributes = list(self.state.attributes)
        assert len(attributes)==4, attributes
        assert 'amount' in attributes, attributes
        assert 'function.label' in attributes, attributes
        assert not 'foo' in attributes, attributes
