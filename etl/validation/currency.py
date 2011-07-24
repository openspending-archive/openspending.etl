import pkg_resources
import xml.etree.ElementTree as ET

from .base import Invalid, null

CURRENCIES = {}

class CurrencyParseError(Exception):
    pass

def load_currencies():
    # We load currencies from the raw XML file downloaded from
    #   http://www.currency-iso.org/dl_iso_table_a1.xml
    #
    # In this file, each currency may be defined multiple times, once for each
    # region in which it's used. We discard region information and just load
    # one entry in CURRENCIES for each unique 3-letter currency code.
    doc = ET.parse(pkg_resources.resource_stream(__name__, "currencies.xml"))

    for currency in doc.getroot():
        code = currency.find("ALPHABETIC_CODE")
        name = currency.find("CURRENCY")

        if code is not None and code.text is not None:
            code = code.text.strip()

            if name is None or name.text is None:
                raise CurrencyParseError(
                    "No name defined for currency code '%s'" % code
                )
            else:
                name = name.text.strip()

            CURRENCIES[code] = {
                'code': code,
                'name': name
            }

class CurrencyCode(object):
    def serialize(self, node, appstruct):
        if appstruct is null:
            return null
        if not isinstance(appstruct, basestring):
            raise Invalid(node, '%r is not a string, thus cannot be a currency' % appstruct)
        if not appstruct.upper() in CURRENCIES.keys():
            raise Invalid(node, '%r is not a recognized currency' % appstruct)
        return appstruct.upper

    def deserialize(self, node, cstruct):
        if cstruct is null:
           return null
        if not isinstance(cstruct, basestring):
            raise Invalid(node, '%r is not a string, thus cannot be a currency' % cstruct)
        if not cstruct.upper() in CURRENCIES.keys():
            raise Invalid(node, '%r is not a recognized currency' % cstruct)
        return cstruct.upper()

if not CURRENCIES:
    load_currencies()