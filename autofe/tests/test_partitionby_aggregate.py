# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division
import os
import sys
import functools
import unittest

from ..autofe import utils, CONSTANTS
from ..autofe.window import partitionby_aggregate
from ..autofe.functions.spark_functions import SqlAggregateFunction
from .prepare_sample_data import initialize
from .compare_tables import compare

class GroupbyTestCase(unittest.TestCase):
    def setUp(self):
        self.sc, self.sqlContext, self.t = initialize()
        self.aggregator = partitionby_aggregate.PartitionbyAggregator(self.sqlContext)

        class MyFunction(SqlAggregateFunction):
            @classmethod
            def name(self):
                return 'cnt_le_300'

            def callable(self):
                return 'count(if({target_col} <= 300, 1, null))'

        self.sql_func = MyFunction()

    def test_spark(self):
        """
        schema = StructType([
            StructField('C_CUSTOMER_ID', StringType(), True),
            StructField('C_MAC_ADDR', StringType(), True),
            StructField('N_TXN_AMT_RMB', DoubleType(), True),
            StructField('D_TXN_TIME_std', IntegerType(), True),
            StructField('TARGET', IntegerType(), True),
            ])
        """

        # test one to many
        config = {
            'windows': [{'name':'100s','type':'range','val':[-100,0]}],
            'partition_cols':['C_CUSTOMER_ID'],
            'target_col':[
                          (['cnt','sum'], ['N_TXN_AMT_RMB'])
                         ],
            'order_col': ['D_TXN_TIME_std']
        }

        res = self.aggregator.aggregate(self.t, **config)
        ref = "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=1, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=2, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=150.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=3, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=200.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=3, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=250.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=3, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=180.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=4, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=1230.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=4, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=1230.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, N_TXN_AMT_RMB_cnt_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=1, N_TXN_AMT_RMB_sum_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=800.0)]"
        self.assertEqual(str(res.collect()), ref)

    
    def test_register_sql_func(self):
        self.aggregator.register_sql_func([self.sql_func])
        with self.assertRaises(AssertionError):
            self.aggregator.register_sql_func([self.sql_func]) 

    def test_spark_and_sql(self):
        self.aggregator.register_sql_func([self.sql_func])
        # test one to many
        config = {
            'windows': [{'name':'100s','type':'range','val':[-100,0]}],
            'partition_cols':['C_CUSTOMER_ID'],
            'target_col':[
                          (['avg','med'], ['N_TXN_AMT_RMB'])
                         ],
            'order_col': ['D_TXN_TIME_std']
        }

        res = self.aggregator.aggregate(self.t, **config)
        # res.show()
        # print(res.collect())
        ref = "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=50.0, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=75.0, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=66.66666666666667, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=83.33333333333333, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=100.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=60.0, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=307.5, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=100.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=307.5, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=100.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, N_TXN_AMT_RMB_avg_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=800.0, N_TXN_AMT_RMB_med_100s_by_C_CUSTOMER_ID_D_TXN_TIME_std=800.0)]"
        self.assertEqual(str(res.collect()), ref)

    def test_row(self):
        self.aggregator.register_sql_func([self.sql_func])
        # test one to many
        config = {
            'windows': [{'name':'2r','type':'row','val':[-2,0]}],
            'partition_cols':['C_CUSTOMER_ID'],
            'target_col':[
                          (['avg','med'], ['N_TXN_AMT_RMB'])
                         ],
            'order_col': ['D_TXN_TIME_std','C_MAC_ADDR']
        }

        res = self.aggregator.aggregate(self.t, **config)
        res.show()
    
    def test_corner_case(self):
        self.aggregator.register_sql_func([self.sql_func])
        
        config = {
            'windows': [{'name':'2r','type':'row','val':[-2,0]}],
            'partition_cols':['C_CUSTOMER_ID'],
            'target_col':[
                          (['avg','med'], ['N_TXN_AMT_RMB'])
                         ],
            'order_col': ['D_TXN_TIME_std','C_MAC_ADDR']
        }

        res = self.aggregator.aggregate(self.t, **config)
        res.show()

        config = {
            'windows': [{'name':'2r','type':'row','val':[-2,0]}],
            'partition_cols':['C_CUSTOMER_ID'],
            'target_col':[
                          (['avg','med'], ['N_TXN_AMT_RMB'])
                         ],
            # 'order_col': ['D_TXN_TIME_std','C_MAC_ADDR']
        }

        res = self.aggregator.aggregate(self.t, **config)
        res.show()

        config = {
            'windows': [{'name':'2r','type':'row','val':[-2,0]}],
            # 'partition_cols':['C_CUSTOMER_ID'],
            'target_col':[
                          (['avg','med'], ['N_TXN_AMT_RMB'])
                         ],
            'order_col': ['D_TXN_TIME_std','C_MAC_ADDR']
        }

        res = self.aggregator.aggregate(self.t, **config)
        res.show()

        config = {
            'windows': [{'name':'2r','type':'row','val':[-2,0]}],
            'partition_cols':[['C_CUSTOMER_ID','C_CUSTOMER_ID']],
            'target_col':[
                          (['avg','med'], ['N_TXN_AMT_RMB'])
                         ],
            'order_col': ['D_TXN_TIME_std','C_MAC_ADDR']
        }

        res = self.aggregator.aggregate(self.t, **config)
        res.show()

    def test_skewed_case(self):
        enable_support_skew = True
        num_subsets = 1
        hot_spot_threshold = 3
        windows = [
            {'name':'200s', 'type':'range', 'val':(-200,0)}
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
            (['avg'],['N_TXN_AMT_RMB']),
            ]
        order_col = ['D_TXN_TIME_std']

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
        }
        res = self.aggregator.aggregate(self.t, **config)
        self.assertEqual(str(res.collect()), "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1000.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=40.0)]")

        partition_cols = ['C_CUSTOMER_ID','C_MAC_ADDR']
        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'enable_support_skew': False,
            'hot_spot_threshold': 100,
        }
        res_off = self.aggregator.aggregate(self.t, **config)

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'enable_support_skew': True,
            'hot_spot_threshold': 100,
        }
        res_on = self.aggregator.aggregate(self.t, **config)

        # check 
        self.assertEqual(compare(res_off, res_on, ['C_MAC_ADDR','D_TXN_TIME_std'], rnd=10), True)
    
if __name__ == '__main__':
    unittest.main()
