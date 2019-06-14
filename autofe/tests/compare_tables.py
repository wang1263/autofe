import sys
import time
import random
from math import log

import numpy as np
import pandas as pd

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext,HiveContext
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
from pyspark.sql import window as W

def compare(t1, t2, join_col, rnd=10):
    """
    Compare whether two ``DataFrame``s are equal.

    :param t1: DataFrame
    :param t2: DataFrame
    :param join_col: unique key(s) to join ``t1`` and ``t2``
    :type join_col: str or list of str
    :param int rnd: the number of digits to round

    :return: True if equal otherwise False
    :rtype: bool
    """

    succ = True
    fail = False
    join_col = join_col if isinstance(join_col, list) else [join_col] 
    # compare columns
    col_set_1 = set(t1.columns)
    col_set_2 = set(t2.columns)
    if col_set_1 <= col_set_2 and col_set_2 <= col_set_1:
        print("Columns match!")
    else:
        print("Columns do not match!")
        print("The following elements are in t1 but not in t2:")
        print(col_set_1 - col_set_2)
        print("The following elements are in t2 but not in t1:")
        print(col_set_2 - col_set_1)
        return fail
    
    # compare rows
    n_rows_1 = t1.count()
    n_rows_2 = t2.count()
    if n_rows_1 == n_rows_2:
        print("Row number match!")
    else:
        print("Row number does not match!")
        print("t1 has {} rows whereas t2 has {} rows!".format(n_rows_1, n_rows_2))
        return fail
    
    # join to make wide table
    def make_suffix(t, suffix, include=None, exclude=None):
        cols = t.columns
        exclude = [] if exclude is None else exclude
        if include is not None:
            cols = include
        exprs = []
        for col in cols:
            if col in exclude:
                expr = F.col(col).alias(col)
            else:
                expr = F.col(col).alias('_'.join([col, suffix]))
            exprs.append(expr)
        res = t.select(*exprs)

        return res
    
    t2 = make_suffix(t2, 't2', exclude=join_col)
    t_wide = t1.join(t2, join_col, 'inner')
    n_rows_wide = t_wide.count()
    if n_rows_wide != n_rows_1:
        print('Key(s) do(es) not match!')
    else:
        print('Key(s) match(es)!')
        
    exprs = list(t_wide.columns)
    for col_1 in t1.columns:
        if col_1 not in join_col:
            col_2 = '_'.join([col_1, 't2'])
            expr = F.when(F.round(F.col(col_1), rnd) != F.round(F.col(col_2), rnd), 1).otherwise(0).alias('_'.join(['cnt', 'diff', col_1]))
            exprs.append(expr)
    res_diff = t_wide.select(exprs)
    exprs = []
    for col in res_diff.columns:
        if 'cnt_diff_' in col:
            expr = F.sum(col).alias(col)
            exprs.append(expr)
    res_agg = res_diff.agg(*exprs).collect()[0].asDict()
    cols_diff = {k:v for k,v in res_agg.items() if v > 0}
    
    if not cols_diff:
        print("Contents match!")
        print('Comparison passed!')
        return succ
    else:
        print("The following columns do not match!")
        print(cols_diff)
        for col in cols_diff.keys():
            res = res_diff.where(F.col(col) > 0)
            print(res.collect()[:10])
        print('Comparison failed!')
        
        return fail