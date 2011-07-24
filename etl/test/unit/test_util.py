from StringIO import StringIO

from openspending.etl.test import TestCase, helpers as h
from openspending.etl import util

DATA_FP = StringIO("line one\nline two\r\nline three")

class TestUtil(TestCase):

    @h.patch('openspending.etl.util.urlopen')
    def test_urlopen_lines(self, urlopen_mock):
        urlopen_mock.return_value = DATA_FP

        lines = [line for line in util.urlopen_lines("http://none")]

        h.assert_equal(lines,
                       ["line one\n", "line two\n", "line three"])
