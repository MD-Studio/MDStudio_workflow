# -*- coding: utf-8 -*-

"""
file: unittest_baseclass.py

Python 2/3 unittest compatibility class
"""

import unittest
import sys

version = sys.version_info
MAJOR_PY_VERSION = sys.version_info.major
PY_VERSION = '{0}.{1}'.format(version.major, version.minor)

# Unicode test
STRING_TYPES = str
if MAJOR_PY_VERSION == 2:
    STRING_TYPES = (str, unicode)


class UnittestPythonCompatibility(unittest.TestCase):

    def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
        """
        Universal assertItemsEqual method.

        Python 2.x has assertItemsEqual but it is assertCountEqual in 3.x.
        """

        if MAJOR_PY_VERSION == 2:
            return super(UnittestPythonCompatibility, self).assertItemsEqual(expected_seq, actual_seq, msg=msg)
        return super(UnittestPythonCompatibility, self).assertCountEqual(expected_seq, actual_seq, msg=msg)
