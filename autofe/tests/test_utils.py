# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division
import os
import sys
import functools
import unittest

from ..autofe import utils, CONSTANTS
from ..autofe.window import window_aggregate
from ..autofe.functions import *
from .prepare_sample_data import initialize
from ..autofe.aggregate import groupby_aggregate

class UtilityTestCase(unittest.TestCase):
    def setUp(self):
        sc, self.sql_context, self.t = initialize()
        self.a = [1,3,5,7,9,12]

    def test_get_hot_spot_values(self):
        self.assertEqual(utils.get_hot_spot_values(self.t, 'C_MAC_ADDR', 2), set(['a']))
        self.assertEqual(utils.get_hot_spot_values(self.t, 'C_MAC_ADDR', 5), set())
        self.assertEqual(utils.get_hot_spot_values(self.t, 'C_MAC_ADDR', 0), set(['a','b','c']))

        groupby_aggregator = groupby_aggregate.GroupbyAggregator(self.sql_context)
        self.assertEqual(utils.get_hot_spot_values(self.t, 'C_MAC_ADDR', 2, groupby_aggregator), set(['a']))
        self.assertEqual(utils.get_hot_spot_values(self.t, 'C_MAC_ADDR', 5, groupby_aggregator), set())
        self.assertEqual(utils.get_hot_spot_values(self.t, 'C_MAC_ADDR', 0, groupby_aggregator), set(['a','b','c']))

    def test_find_ge(self):
        self.assertEqual(utils.find_ge(self.a, 5), 2)
        self.assertEqual(utils.find_ge(self.a, 4), 2)
        self.assertEqual(utils.find_ge(self.a, 15), None)
        self.assertEqual(utils.find_ge(self.a, 5, lo=3), 3)

    def test_find_le(self):
        self.assertEqual(utils.find_le(self.a, 5), 2)
        self.assertEqual(utils.find_le(self.a, 4), 1)
        self.assertEqual(utils.find_le(self.a, -1), None)
        self.assertEqual(utils.find_le(self.a, 5, lo=1, hi=2), 1)

    def test_sorted(self):
        a = [(4,None),(2,1),(4,5),(None,9),(3,None)]
        res_0 = [(2,1),(3,None),(4,None),(4,5),(None,9)]
        res_1 = [(2,1),(4,5),(None,9),(4,None),(3,None)]
        self.assertEqual(utils.sorted_handle_none(a, key=lambda x:x[0], reverse=False), res_0)
        self.assertEqual(utils.sorted_handle_none(a, key=lambda x:x[1], reverse=False), res_1)
        res_0 = [(None, 9), (4, None), (4, 5), (3, None), (2, 1)]
        res_1 = [(3, None), (4, None), (None, 9), (4, 5), (2, 1)]
        self.assertEqual(utils.sorted_handle_none(a, key=lambda x:x[0], reverse=True), res_0)
        self.assertEqual(str(utils.sorted_handle_none(a, key=lambda x:x[1], reverse=True)), '[(4, None), (3, None), (None, 9), (4, 5), (2, 1)]')

    def test_gen_random_str(self):
        res = utils.gen_random_str(n=10, seed=10)
        self.assertEqual(res, 'KcBE9anD0F')

    def test_min(self):
        a = [(4,None),(2,1),(4,5),(None,9),(3,None)]
        self.assertEqual(utils.min_handle_none(a, key=lambda x:x[0], reverse=False), (2,1))
        self.assertEqual(utils.min_handle_none(a, key=lambda x:x[0], reverse=True), (None,9))
        self.assertEqual(utils.min_handle_none(a, key=lambda x:x[1], reverse=False), (2,1))
        self.assertEqual(utils.min_handle_none(a, key=lambda x:x[1], reverse=True), (4,None))
        with self.assertRaises(ValueError):
            utils.min_handle_none([], reverse=False)
        with self.assertRaises(ValueError):
            utils.min_handle_none([], reverse=True)
        self.assertEqual(utils.min_handle_none([None], reverse=False), None)

if __name__ == '__main__':
    unittest.main()
