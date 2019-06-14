# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

# TODO: hot spot processing, repartition by joined partition_col+subset_id
# TODO: finall transform to DataFrame, use repartitionAndSortWithinPartitions

from __future__ import print_function, division
import sys
import copy
from math import log, ceil, floor
import functools
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql import functions as F
from pyspark.sql import window as W

if __package__ is None:
    # sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import utils
else:
    from . import utils


def arithmetic(t, expr, prefix, *col_list):
    """
    This function returns a function computing features from
    """

    procs = []

    for col in t.columns:
        procs.append(F.col(col))

    if col_list is not None:
        for index in range(len(col_list[0])):
            vals = []
            for cols in col_list:
                vals.append('F.col(\'{}\')'.format(cols[index]))
            new_col = prefix+str(index)
            expr = '(' + expr + ').alias(' + new_col +')'
            format_expr = expr.format(*vals)
            procs.append(eval(format_expr))
        t2 = t.select(procs)
        return t2
    else:
        return t