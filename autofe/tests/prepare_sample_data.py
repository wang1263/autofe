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

from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

from ..autofe import functions
from ..autofe.window import window_aggregate

def initialize():
    sc = SparkContext.getOrCreate()
    sqlContext = HiveContext(sc)

    data = [
        ('001', 'a', 100., 200, 1),
        ('001', 'a', 100., 300, 0),
        ('001', 'a', 100., 350, 0),
        ('001', 'b', 50., 210, 1),
        ('001', 'b', 30., 310, 0),
        ('001', 'a', 50., 150, 0),
        ('001', 'c', 1000., 350, 1),
        ('001', 'a', 800., 800, 1),
        ]

    schema = StructType([
        StructField('C_CUSTOMER_ID', StringType(), True),
        StructField('C_MAC_ADDR', StringType(), True),
        StructField('N_TXN_AMT_RMB', DoubleType(), True),
        StructField('D_TXN_TIME_std', IntegerType(), True),
        StructField('TARGET', IntegerType(), True),
        ])

    sample_t = sc.parallelize(data).toDF(schema)

    return sc, sqlContext, sample_t

def main():
    sc, sqlContext, t = initialize()
    windows = [
        {'name':'200s','range':(200,0),'row':(None, None)}
        ]
    fnames = ["countdistinct", "ratio", "entropy"]
    aggregate_functions = {}
    for fname in fnames:
        function = eval('functions.{}'.format(fname))
        if fname not in ['ifexist', 'last']:
            aggregate_functions[fname] = functools.partial(function, transformer=functions.identity())
        else:
            aggregate_functions[fname] = function

    partition_cols = ['C_MAC_ADDR']
    target_cols = ["N_TXN_AMT_RMB",]
    order_col = 'D_TXN_TIME_std'

    hot_spot_threshold = 2
    partition_num = 2
    num_subsets = 3
    res = window_aggregate.window_aggregate(t, windows, aggregate_functions, partition_cols, target_cols, order_col, partition_num, hot_spot_threshold, num_subsets, None)
    print("{'name':'200s','range':(200,0),'row':(None, None)}")
    print('hotspot')
    res.show()
    print("""+-------------+----------+-------------+--------------+------+----------------------------------------------+--------------------------------------+----------------------------------------+
|C_CUSTOMER_ID|C_MAC_ADDR|N_TXN_AMT_RMB|D_TXN_TIME_std|TARGET|N_TXN_AMT_RMB_countdistinct_200s_by_C_MAC_ADDR|N_TXN_AMT_RMB_ratio_200s_by_C_MAC_ADDR|N_TXN_AMT_RMB_entropy_200s_by_C_MAC_ADDR|
+-------------+----------+-------------+--------------+------+----------------------------------------------+--------------------------------------+----------------------------------------+
|          001|         c|       1000.0|           350|     1|                                           1.0|                                   1.0|                                     0.0|
|          001|         b|         50.0|           210|     1|                                           1.0|                                   1.0|                                     0.0|
|          001|         b|         30.0|           310|     0|                                           2.0|                                   0.5|                      0.6931471805599453|
|          001|         a|        100.0|           200|     1|                                           2.0|                                  0.25|                      0.5623351446188083|
|          001|         a|        100.0|           300|     0|                                           2.0|                                   0.5|                      0.6931471805599453|
|          001|         a|        800.0|           800|     1|                                           1.0|                                   1.0|                                     0.0|
|          001|         a|         50.0|           150|     0|                                           1.0|                                   1.0|                                     0.0|
|          001|         a|        100.0|           350|     0|                                           2.0|                    0.4444444444444444|                      0.6869615765973234|
+-------------+----------+-------------+--------------+------+----------------------------------------------+--------------------------------------+----------------------------------------+""")


if __name__ == '__main__':
    main()
