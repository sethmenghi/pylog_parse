#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_pylog_parse
----------------------------------

Tests for `pylog_parse` module.
"""

import unittest
import pandas

from pylog_parse import pylog_parse

from ..pylog_parse import LogFile

class TestPylog_parse(unittest.TestCase):

    def setUp(self):
        test_file = ('/Users/seth/Dropbox (Optimus)/0ptimus - ',
                     'IJ Review/Data processing/',
                     'logs-2015_07_14-15_43_00_sample.log')
        log = LogFile(test_file)
        pass

    def test_something(self):
        pass

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
