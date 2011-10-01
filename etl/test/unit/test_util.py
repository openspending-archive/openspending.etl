from StringIO import StringIO

from openspending.etl import util

from .. import TestCase, helpers as h


@h.patch('openspending.etl.util.urlopen')
def test_urlopen_lines(urlopen_mock):
    urlopen_mock.return_value = StringIO("line one\nline two\r\nline three")

    lines = [line for line in util.urlopen_lines("http://none")]

    h.assert_equal(lines,
                   ["line one\n", "line two\n", "line three"])

def test_hash():
    h.assert_equal(util.hash_values(["foo", "bar", "baz"]),
                   '976cbe6da83475797cbb55f3fc50bf174b138a60')

