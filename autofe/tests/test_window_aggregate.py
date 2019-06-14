# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division
import os
import sys
import copy
import functools
import unittest

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql import functions as F

from ..autofe import functions, utils
from ..autofe.window import window_aggregate
from ..autofe.legacy import window_aggregate as window_aggregate_legacy
from . import compare_tables
from .prepare_sample_data import initialize

class WindowAggregateTestCase(unittest.TestCase):
    def setUp(self):
        self.sc, self.sqlContext, self.t = initialize()
        self.aggregator = window_aggregate.WindowAggregator(self.sqlContext)
        self.aggregator_legacy = window_aggregate_legacy.WindowAggregator(self.sqlContext)
    
    def test_windows(self):
        windows_list = [
            [{'name':'12h','range':(None,0),'row':(2,None)}],
            [{'name':'12h','range':(None,0),'row':(2,0)}],
            [{'name':'12h','range':(None,0),'row':(None,1)}],
            [{'name':'12h','range':(None,0),'row':(None,None)}],
            [{'name':'12h','range':(None,0),'row':(10000,None)}],
            [{'name':'12h','range':(100,0),'row':(None,None)}],
            [{'name':'12h','range':(100,0),'row':(2,None)}],
            [{'name':'12h','range':(100,0),'row':(10,None)}],
            [{'name':'12h','range':(100000,0),'row':(None,None)}],
            [{'name':'12h','range':(None,1000000),'row':(None,None)}],
            [{'name':'12h','range':(None,0),'row':(None,100)}],
            [{'name':'12h','range':(None,10000),'row':(None,100)}],
            ]

        partition_cols = ['C_MAC_ADDR']
        target_col = [(['avg'],['N_TXN_AMT_RMB','TARGET'])]
        order_col = 'D_TXN_TIME_std'
        for windows in windows_list:
            config = {
                'windows': windows,
                'partition_cols': partition_cols,
                'target_col': target_col,
                'order_col': order_col,
                'partition_num': 10,
                'enable_support_skew': False,
                'hot_spot_threshold': 10000,
                'num_subsets': 1
            }
            res = self.aggregator.aggregate(self.t, **config)
            res_ref = self.aggregator_legacy.aggregate(self.t, **config)
            self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
    
    def test_correctness(self):
        partition_num = 10
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 100
        windows = [
            {'name':'w1','range':(None,0),'row':(2,None)},
            {'name':'w2','range':(300,0),'row':(2,None)},
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
            (['min','max','cnt','dcnt','exist','ecnt','last','etrp','avg','rto'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
            (['sum'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
            ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
    
    def test_corner_case(self):
        partition_num = 1
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 100
        windows = [
            {'name':'200s','range':(200,0),'row':(2,None)}
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
            (['etrp'],['N_TXN_AMT_RMB']),
            (['last'],['N_TXN_AMT_RMB','TARGET']),
            (['avg','min','max'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
            (['sum'],[('N_TXN_AMT_RMB','sum_rmb'),('D_TXN_TIME_std','sum_std')]),
            ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            # 'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)

        config = {
            'windows': windows,
            'partition_cols': [['C_MAC_ADDR','C_CUSTOMER_ID']],
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
    
    def test_truncate(self):
        aggregator_tr = window_aggregate.WindowAggregator(self.sqlContext, NO_DISCARD_TRUNCATE_THRESHOLD=2)
        partition_num = 1
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 100
        windows = [
            {'name':'200s','range':(None,0),'row':(None,None)}
            ]
    
        partition_cols = ['TARGET']
        target_col = [
            (['etrp'],['N_TXN_AMT_RMB']),
            (['last'],['N_TXN_AMT_RMB','TARGET']),
            (['avg','min','max'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
            (['sum'],[('N_TXN_AMT_RMB','sum_rmb'),('D_TXN_TIME_std','sum_std')]),
            ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            # 'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res_tr = aggregator_tr.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res_tr, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
    
    def test_truncate(self):
        path = "./autofe/tests/sample.parquet"
        t = self.sqlContext.read.parquet(path).where(F.col('cardno').isin(['020000001043361']))
        aggregator_tr = window_aggregate.WindowAggregator(self.sqlContext, NO_DISCARD_TRUNCATE_THRESHOLD=55)

        partition_num = 10
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 10000
        n_rows = None
        windows = [
            # {'name':'10s','range':(10,0),'row':(n_rows,None)},
            # {'name':'60s','range':(60,0),'row':(n_rows,None)},
            # {'name':'5m','range':(60*5,0),'row':(n_rows,None)},
            # {'name':'30m','range':(60*30,0),'row':(n_rows,None)},
            # {'name':'2h','range':(3600*2,0),'row':(n_rows,None)},
            # {'name':'12h','range':(3600*12,0),'row':(n_rows,None)},
            # {'name':'24h','range':(86400,0),'row':(n_rows,None)},
            # {'name':'2d','range':(86400*2,0),'row':(n_rows,None)},
            {'name':'5d','range':(86400*5,0),'row':(n_rows,None)},
            ]
        partition_cols = ['cardno']
        target_col = [
            (['sum'],
             ['trx_time_diff']
            ),
            ]

        order_col = 'trx_time_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res_tr = aggregator_tr.aggregate(t, **config)
        res_ref = self.aggregator_legacy.aggregate(t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res_tr, ['trx_seq']), True)
    
    def test_skewed_case(self):
        partition_num = 10
        enable_support_skew = True
        num_subsets = 1
        hot_spot_threshold = 3
        windows = [
            {'name':'200s','range':(200,0),'row':(2,None)},
            {'name':'w2','range':(300,0),'row':(2,None)},
            ]
    
        partition_cols = ['C_MAC_ADDR',['TARGET'], ['C_MAC_ADDR', 'TARGET']]
        target_col = [
            (['avg'],['N_TXN_AMT_RMB']),
            ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
        # self.assertEqual(str(res.collect()), "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=50.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=40.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1000.0)]")
    
    def test_none_case(self):

        data = [
        ('001', 'a', 100., 200, 1),
        ('001', 'a', None, 300, 0),
        ]

        schema = StructType([
            StructField('C_CUSTOMER_ID', StringType(), True),
            StructField('C_MAC_ADDR', StringType(), True),
            StructField('N_TXN_AMT_RMB', DoubleType(), True),
            StructField('D_TXN_TIME_std', IntegerType(), True),
            StructField('TARGET', IntegerType(), True),
            ])

        sample_t = self.sc.parallelize(data).toDF(schema)

        partition_num = 1
        enable_support_skew = True
        num_subsets = 1
        hot_spot_threshold = 3
        windows = [
            {'name':'200s','range':(200,0),'row':(2,None)}
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
            (['avg','last','ecnt','dcnt','etrp','sum','min','max'],['N_TXN_AMT_RMB']),
            ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res = self.aggregator.aggregate(sample_t, **config)
        res_ref = self.aggregator_legacy.aggregate(sample_t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
        # self.assertEqual(str(res.collect()),"[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_last_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_ecnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0, N_TXN_AMT_RMB_dcnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0, N_TXN_AMT_RMB_etrp_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.0, N_TXN_AMT_RMB_sum_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_min_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_max_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=None, D_TXN_TIME_std=300, TARGET=0, N_TXN_AMT_RMB_avg_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_last_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_ecnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.0, N_TXN_AMT_RMB_dcnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0, N_TXN_AMT_RMB_etrp_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.0, N_TXN_AMT_RMB_sum_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_min_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0, N_TXN_AMT_RMB_max_200s_by_C_MAC_ADDR_D_TXN_TIME_std=100.0)]")
    
    def test_conditional_append_range(self):
        def condition(row):
            return row['TARGET'] == 1

        partition_num = 10
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 10
        windows = [
            {'name':'200s','range':(None,0),'row':(None,None)}
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
                    (['cnt'], ['N_TXN_AMT_RMB'])
                    ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets,
            'condition': condition
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
        # self.assertEqual(str(res.collect()), "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=2.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, N_TXN_AMT_RMB_cnt_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0)]")

    def test_conditional_append_row(self):
        def condition(row):
            return row['TARGET'] == 1

        partition_num = 10
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 10
        windows = [
            {'name':'200s','range':(None,0),'row':(3,0)}
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
                    (['cnt'], [('N_TXN_AMT_RMB','cnt')])
                    ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets,
            'condition': condition
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
        # self.assertEqual(str(res.collect()), "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, cnt=None), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, cnt=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, cnt=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, cnt=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, cnt=2.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, cnt=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, cnt=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, cnt=1.0)]")
    
    def test_extra_param(self):
        partition_num = 10
        enable_support_skew = False
        num_subsets = 1
        hot_spot_threshold = 3
        windows = [
            {'name':'200s','range':(None,0),'row':(None,None)}
            ]
    
        partition_cols = ['C_MAC_ADDR']
        target_col = [
                    ([('rto', {'prior':0.5})], [('N_TXN_AMT_RMB','rto_rmb')])
                    ]
        order_col = 'D_TXN_TIME_std'

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': target_col,
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
        # self.assertEqual(str(res_reg.collect()), "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, rto_rmb=0.75), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, rto_rmb=0.5), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, rto_rmb=0.625), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, rto_rmb=0.7000000000000001), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, rto_rmb=0.25), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, rto_rmb=0.75), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, rto_rmb=0.5), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, rto_rmb=0.75)]")

        config = {
            'windows': windows,
            'partition_cols': partition_cols,
            'target_col': [([('rto',{'query':1})], ['TARGET'])],
            'order_col': order_col,
            'partition_num': partition_num,
            'enable_support_skew': enable_support_skew,
            'hot_spot_threshold': hot_spot_threshold,
            'num_subsets': num_subsets
        }
        res = self.aggregator.aggregate(self.t, **config)
        res_ref = self.aggregator_legacy.aggregate(self.t, **config)
        self.assertEqual(compare_tables.compare(res_ref, res, ['C_MAC_ADDR','D_TXN_TIME_std']), True)
        # self.assertEqual(str(res.collect()), "[Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=150, TARGET=0, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=200, TARGET=1, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.5), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=300, TARGET=0, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.3333333333333333), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=100.0, D_TXN_TIME_std=350, TARGET=0, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.25), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='a', N_TXN_AMT_RMB=800.0, D_TXN_TIME_std=800, TARGET=1, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.4), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=50.0, D_TXN_TIME_std=210, TARGET=1, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='b', N_TXN_AMT_RMB=30.0, D_TXN_TIME_std=310, TARGET=0, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=0.5), Row(C_CUSTOMER_ID='001', C_MAC_ADDR='c', N_TXN_AMT_RMB=1000.0, D_TXN_TIME_std=350, TARGET=1, TARGET_rto_200s_by_C_MAC_ADDR_D_TXN_TIME_std=1.0)]")
    
if __name__ == '__main__':
    unittest.main()
    