from openspending.lib import json
from openspending.etl.validation import Invalid

from ... import helpers as h

def _load_json(fp_or_str):
    try:
        return json.load(fp_or_str)
    except AttributeError:
        return json.loads(fp_or_str)

def validation_result(cls, fp_or_str):
    obj = _load_json(fp_or_str)
    return cls().deserialize(obj)

def validation_errors(cls, fp_or_str):
    obj = _load_json(fp_or_str)

    try:
        cls().deserialize(obj)
    except Invalid as e:
        return e.asdict()

    h.assert_true(
        False,
        "Expected validation to throw errors, but none thrown."
    )