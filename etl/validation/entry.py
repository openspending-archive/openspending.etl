import re

from .. import times
from .base import Mapping, Regex, SchemaNode, String, Invalid, Function
from .base import PLACEHOLDER


def make_date_validator(dimension, is_end):
    '''
    Factory for a context sensitive validator for dates.
    If the field is 'time', it must match :const:`DATE_RE`,
    unless it is the end date. Then it can be empty.
    For any other field, it can also be emtpy
    '''

    def _validator(node, value):
        msg_suffix = ('in the format "yyyy-mm-dd", "yyyy-mm" or "yyyy", '
                      'e.g. "2011-12-31".')
        if dimension != 'time':
            msg = '"%s" can be empty or a value %s' % (node.name, msg_suffix)
            if not value:
                return
        else:
            if is_end and not value:
                return
            msg = ('"time" (here "%s") has to be %s. The "end_column", if specified, '
                   'might be empty') % (value, msg_suffix)
        try:
            times.for_datestrings(value)
        except (times.ParseError, ValueError):
            raise Invalid(node, msg)

    return _validator

FLOAT_RE = re.compile(r'^[0-9-\.,]+$')

def _validate_float(value):
    if not FLOAT_RE.match(value):
        return ("Numbers must only contain digits, periods, dashes and commas")
    try:
        val = float(value.replace(",",""))
        return True
    except:
        return ("Could not coerce value into a number")

def make_validator(fields):
    seen = []
    schema = SchemaNode(Mapping(unknown='ignore'))
    for field_ in fields:
        field = field_['field']
        datatype = field_['datatype']
        is_end = field_['is_end']
        dimension = field_['dimension']
        if field in seen:
            continue
        seen.append(field)
        if datatype == 'constant':
            continue
        elif datatype == 'float':
            schema.add(
                SchemaNode(
                    String(),
                    name=field,
                    missing="0.0",
                    validator=Function(_validate_float)
                )
            )
        elif datatype == 'date':
            schema.add(
                SchemaNode(
                    String(),
                    name=field,
                    validator=make_date_validator(dimension, is_end)
                )
            )
        else:
            schema.add(SchemaNode(String(), name=field, missing=""))
    return schema
