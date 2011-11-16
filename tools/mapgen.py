# Mapping generator for OpenSpending
import pkg_resources
pkg_resources.require("unidecode")
pkg_resources.require("messytables")

try:
    from openspending.lib.util import slugify
except ImportError:
    import re
    from unidecode import unidecode
    def slugify(text, delimiter='-'):
        result = []
        for word in re.split(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+', unicode(text).lower()):
            result.extend(unidecode(word).split())
        return unicode(delimiter.join(result))

#from pprint import pprint
from json import dumps
from collections import defaultdict
from messytables import CSVRowSet, type_guess


def frequent_values(sample):
    values = defaultdict(lambda: defaultdict(int))
    for row in sample:
        for i, value in enumerate(row):
            values[i][value.value] += 1
    sorted_values = []
    for idx, column in values.items():
        frequent = sorted(column.items(), key=lambda (v,c): c, reverse=True)
        sorted_values.append(frequent[:5])
    return sorted_values


def generate_mapping(fileobj, sample=2000):
    row_set = CSVRowSet('data', fileobj, window=sample)
    sample = list(row_set.sample)
    headers, sample = sample[0], sample[1:]
    values = frequent_values(sample)
    types = type_guess(sample)
    mapping = {}
    for header, type_, value in zip(headers, types, values):
        type_ = repr(type_).lower()
        name = slugify(header.value).lower()
        meta = {
            'label': header.value,
            'column': header.value,
            'common_values': value,
            'datatype': type_
            }
        if type_ in ['decimal', 'integer', 'float']:
            meta['type'] = 'measure'
            meta['datatype'] = 'float'
        elif type_ in ['date']:
            meta['type'] = 'date'
            meta['datatype'] = 'date'
        else:
            meta['type'] = 'value'
        mapping[name] = meta
    return mapping


if __name__ == '__main__':
    import sys
    file_name = sys.argv[1]
    fh = open(file_name, 'rb')
    mapping = generate_mapping(fh)
    print dumps(mapping, indent=2)
