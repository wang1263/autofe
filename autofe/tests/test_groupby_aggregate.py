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
from ..autofe.aggregate import groupby_aggregate
from ..autofe.functions.spark_functions import SqlAggregateFunction
from .prepare_sample_data import initialize

class GroupbyTestCase(unittest.TestCase):
    def setUp(self):
        self.sc, self.sqlContext, self.t = initialize()
        self.aggregator = groupby_aggregate.GroupbyAggregator(self.sqlContext)

        class MyFunction(SqlAggregateFunction):
            @classmethod
            def name(self):
                return 'cnt_le_300'

            def callable(self):
                return 'count(if({target_col} <= 300, 1, null))'

        self.sql_func = MyFunction()

    
    def test_register_sql_func(self):
        self.aggregator.register_sql_func([self.sql_func])
        with self.assertRaises(AssertionError):
            self.aggregator.register_sql_func([self.sql_func]) 
    
    
    def test_aggregate(self):
        """
        schema = StructType([
            StructField('C_CUSTOMER_ID', StringType(), True),
            StructField('C_MAC_ADDR', StringType(), True),
            StructField('N_TXN_AMT_RMB', DoubleType(), True),
            StructField('D_TXN_TIME_std', IntegerType(), True),
            StructField('TARGET', IntegerType(), True),
            ])
        """

        self.aggregator.register_sql_func([self.sql_func])

        # test one to many
        config = {
            'groupby_col':['C_CUSTOMER_ID'],
            'target_col':[
                (['med'],['N_TXN_AMT_RMB']),
                (['max'],['N_TXN_AMT_RMB','TARGET']),
                (['cnt_le_300'],['N_TXN_AMT_RMB'])
                ],
        }

        res = self.aggregator.aggregate(self.t, **config)
        self.assertEqual(str(res.collect()),"[Row(C_CUSTOMER_ID='001', max_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=1000.0, max_TARGET_by_C_CUSTOMER_ID=1, med_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=100.0, cnt_le_300_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=6)]")

        # test many to many
        config = {
            'groupby_col':['C_CUSTOMER_ID'],
            'target_col':[
                (['med','max','cnt_le_300'],['N_TXN_AMT_RMB']),
                (['max'],['TARGET'])
                ],
        }

        res = self.aggregator.aggregate(self.t, **config)
        self.assertEqual(str(res.collect()),"[Row(C_CUSTOMER_ID='001', max_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=1000.0, max_TARGET_by_C_CUSTOMER_ID=1, med_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=100.0, cnt_le_300_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=6)]")

        # test pyspark/sql
        for enable_sql in [True, False]:
            config = {
                'groupby_col':['C_CUSTOMER_ID'],
                'target_col':[
                    (['max','avg'],['N_TXN_AMT_RMB']),
                    (['max'],['TARGET'])
                    ],
            }
            res = self.aggregator.aggregate(self.t, **config)
            self.assertEqual(str(res.collect()),"[Row(C_CUSTOMER_ID='001', max_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=1000.0, avg_N_TXN_AMT_RMB_by_C_CUSTOMER_ID=278.75, max_TARGET_by_C_CUSTOMER_ID=1)]")

            with self.assertRaises(AssertionError):
                self.aggregator.aggregate(self.t, groupby_col=None, target_col={'max':['N_TXN_AMT_RMB']})
    
    def test_skewed(self):
        """
        schema = StructType([
            StructField('C_CUSTOMER_ID', StringType(), True),
            StructField('C_MAC_ADDR', StringType(), True),
            StructField('N_TXN_AMT_RMB', DoubleType(), True),
            StructField('D_TXN_TIME_std', IntegerType(), True),
            StructField('TARGET', IntegerType(), True),
            ])
        """

        self.aggregator.register_sql_func([self.sql_func])

        # test one to many
        config = {
            'groupby_col':['C_CUSTOMER_ID'],
            'target_col':[
                (['min','max','avg','sum','cnt','stddev','med','dcnt'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
                ],
            'is_skewed': False
        }

        res_nonsupport = self.aggregator.aggregate(self.t, **config)
        config['is_skewed'] = True
        res_support = self.aggregator.aggregate(self.t, **config)
        self.assertEqual(len(res_nonsupport.columns), len(res_support.columns))
        res_nonsupport = res_nonsupport.select(*res_support.columns)
        self.assertEqual(str(res_support.collect()), str(res_nonsupport.collect()))

        config = {
            'groupby_col':['C_CUSTOMER_ID'],
            'target_col':[
                (['stddev'],[('N_TXN_AMT_RMB','col_0'), ('D_TXN_TIME_std', 'col_1')]),
                ],
            'is_skewed': False
        }
        res_nonsupport = self.aggregator.aggregate(self.t, **config)
        config['is_skewed'] = True
        res_support = self.aggregator.aggregate(self.t, **config)
        self.assertEqual(len(res_nonsupport.columns), len(res_support.columns))
        res_nonsupport = res_nonsupport.select(*res_support.columns)
        self.assertEqual(str(res_support.collect()), str(res_nonsupport.collect()))

    def test_cmb_groupby(self):
        """
        schema = StructType([
            StructField('C_CUSTOMER_ID', StringType(), True),
            StructField('C_MAC_ADDR', StringType(), True),
            StructField('N_TXN_AMT_RMB', DoubleType(), True),
            StructField('D_TXN_TIME_std', IntegerType(), True),
            StructField('TARGET', IntegerType(), True),
            ])
        """

        self.aggregator.register_sql_func([self.sql_func])

        # test one to many
        config = {
            'groupby_col':['C_CUSTOMER_ID','TARGET'],
            'target_col':[
                (['min','max','avg','sum','cnt','stddev','med','dcnt'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
                ],
            'is_skewed': False
        }

        res_nonsupport = self.aggregator.aggregate(self.t, **config)
        config['is_skewed'] = True
        res_support = self.aggregator.aggregate(self.t, **config)
        self.assertEqual(len(res_nonsupport.columns), len(res_support.columns))
        res_nonsupport = res_nonsupport.select(*res_support.columns)
        self.assertEqual(str(res_support.collect()), str(res_nonsupport.collect()))

    def test(self):
        """
        schema = StructType([
            StructField('C_CUSTOMER_ID', StringType(), True),
            StructField('C_MAC_ADDR', StringType(), True),
            StructField('N_TXN_AMT_RMB', DoubleType(), True),
            StructField('D_TXN_TIME_std', IntegerType(), True),
            StructField('TARGET', IntegerType(), True),
            ])
        """

        self.aggregator.register_sql_func([self.sql_func])

        # test one to many
        config = {
            'groupby_col':['C_CUSTOMER_ID'],
            'target_col':[
                (['min','max','avg','med','sum','cnt'],['N_TXN_AMT_RMB','D_TXN_TIME_std']),
                (['avg'],['TARGET'])
                ],
        }

        res = self.aggregator.aggregate(self.t, **config)
        res.show()
    
if __name__ == '__main__':
    unittest.main()
