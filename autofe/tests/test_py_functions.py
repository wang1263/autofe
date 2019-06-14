# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division,absolute_import
import os
import sys
import functools
import unittest

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ..)))
from ..autofe import utils, CONSTANTS
from ..autofe.legacy.py_functions import *

class FunctionTestCase(unittest.TestCase):
    def setUp(self):
        self.list = [3, 5, 2, -6, 3, 5, 2, 3]

    def _test_if_return_na(self, function, NA=CONSTANTS.NA):
        self.assertEqual(
            function(self.list, query=None, left=None, right=None, transformer=identity(), rtype=None),
            NA
            )
        self.assertEqual(
            function(self.list, query=None, left=None, right=6, transformer=identity(), rtype=None),
            NA
            )
        self.assertEqual(
            function(self.list, query=None, left=3, right=None, transformer=identity(), rtype=None),
            NA
            )
        self.assertEqual(
            function(self.list, query=9, left=3, right=None, transformer=identity(), rtype=None),
            NA
            )

    def test_fsum(self):
        # different transformer and different boundary
        self._test_if_return_na(fsum)
        self.assertEqual(fsum(self.list, left=0, right=7, transformer=identity()), 17.0)
        self.assertEqual(fsum(self.list, left=3, right=5, transformer=identity()), 2.0)

    def test_fmax(self):
        self._test_if_return_na(fmax)
        self.assertEqual(fmax(self.list, left=0, right=7, transformer=identity()), 5.0)
        self.assertEqual(fmax(self.list, left=3, right=5, transformer=identity()), 5.0)

    def test_ifexist(self):
        self._test_if_return_na(ifexist)
        self.assertEqual(ifexist(self.list, left=0, right=7, query=2), 1)
        self.assertEqual(ifexist(self.list, left=0, right=7, query=-2), 0)
        self.assertEqual(ifexist(self.list, left=3, right=5, query=-6), 1)
        self.assertEqual(ifexist(self.list, left=3, right=5, query=2), 0)
        with self.assertRaises(TypeError):
            ifexist(self.list, left=3, right=5, query=2, transformer=identity())

    def test_existcount(self):
        self._test_if_return_na(existcount)
        self.assertEqual(existcount(self.list, left=0, right=7, query=3, transformer=identity()), 3)
        self.assertEqual(existcount(self.list, left=0, right=7, query=-3, transformer=identity()), 0)

    def test_countdistinct(self):
        self._test_if_return_na(countdistinct)
        self.assertEqual(countdistinct(self.list, left=0, right=7, transformer=identity()), 4)
        self.assertEqual(countdistinct(self.list, left=3, right=5, transformer=identity()), 3)
        self.assertEqual(countdistinct(self.list, left=7, right=7, transformer=identity()), 1)

    def test_last(self):
        self._test_if_return_na(last)
        self.assertEqual(last(self.list, left=0, right=7), 3)
        self.assertEqual(last(self.list, left=0, right=0), 3)

    def test_ratio(self):
        self._test_if_return_na(ratio)
        with self.assertRaises(TypeError):
            ratio(self.list, left=0, right=7)
        self.assertEqual(ratio(self.list, query=2, left=0, right=7, transformer=identity()), 0.25)
        self.assertEqual(ratio(self.list, query=2, left=3, right=3, transformer=identity()), 0.0)
        self.assertEqual(ratio(self.list, query=2, left=2, right=5, prior=0.5, transformer=identity()), 0.3)

    def test_entropy(self):
        self._test_if_return_na(entropy)
        self.assertEqual(entropy(self.list, left=0, right=7, transformer=discrete_NINF_INF(5)), 0.84195)
        self.assertEqual(entropy(self.list, left=0, right=2, transformer=discrete_NINF_INF(5)), 0.74128)

if __name__ == '__main__':
    unittest.main()
