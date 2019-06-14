# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division
import sys
import copy
from math import log,ceil,floor
import functools
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,ArrayType
from pyspark.sql import functions as F
from pyspark.sql import window as W

from window_aggregate import window_aggregate
from window_aggregate_sql import window_aggregate_sql
from arithmetic import arithmetic
from join import join

if __package__ is None:
    # sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import utils
else:
    from . import utils

"""
two type of woe features transform:
"""

def woe(t, label_col, order_col, partition_cols, label_delay, window_lens=[2592000], expr = '{0}*1.0/{1}'):

    windows=[]
    for index, window_len in enumerate(window_lens):
        windows.append( {'name':'w'+str(index),'range':(-label_delay-window_len,-label_delay),'row':(None,None)} )
    aggregate_functions = [ 'existcount', 'fsum']
    target_cols = [label_col]
    extend_partition_cols = partition_cols[:]
    extend_partition_cols.append('1')
    t1 = window_aggregate_sql( t, windows,  aggregate_functions, extend_partition_cols, target_cols, order_col)

    cnt_cols = [ utils.name_new_col(label_col, 'existcount', name, partition_col) for (name, partition_col) in zip([window['name'] for window in windows], partition_cols)]
    pos_cols = [ utils.name_new_col(label_col, 'fsum', name, partition_col) for (name, partition_col) in zip([window['name'] for window in windows], partition_cols)]
    cnt_cols_total = [ utils.name_new_col(label_col, 'existcount', name, '1') for (name, partition_col) in zip([window['name'] for window in windows], partition_cols)]
    pos_cols_total = [ utils.name_new_col(label_col, 'fsum', name, '1') for (name, partition_col) in zip([window['name'] for window in windows], partition_cols)]

    # pos_rate_cols:adjust by prior positive rate
    t2 = arithmetic(t1, expr, 'pos_rate_', cnt_cols, pos_cols, cnt_cols_total, pos_cols_total,  partition_cols)

    return t2



