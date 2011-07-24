import colander

from .base import PreservingMappingSchema, SequenceSchema

from .dataset import Dataset
from .mapping import Mapping

class View(PreservingMappingSchema):
    pass

class Views(SequenceSchema):
    view = View()

class Model(PreservingMappingSchema):
    dataset = Dataset()
    mapping = Mapping()
    views = Views(missing=[])
