# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################
from functools import reduce
from pyspark.sql import DataFrame

def join(t1, t2, on, how, drop_cols):
    t3 = t1.join(t2,eval(on),how)
    if drop_cols is not None:
        reduce(DataFrame.drop, [drop_cols], t3)
    return t3

