# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################
from __future__ import print_function,division

import random
import string
import bisect
import functools

from pyspark.sql.functions import count,col
from pyspark.sql import functions as F

from . import CONSTANTS

def general_make_alias(*args, **kwargs):
    """
    Name new column based on ``*args``.

    Each element of ``*args`` would be truncated to ``max_len`` if their length exceeds the threshold. In addition, if any of them is ``None``, they 
    would be replaced with string ``'None'``.

    :param *args: ``str`` provided to make alias
    
    :param int max_len: maximum length of each component including ``base_col_name``, ``function_name``, ``window_name``, 
        ``partition_col_name``, and ``order_col_name``; default is 100
    :param str alias: pre-defined alias for the column. If not ``None``, it is the name of the new column; default is ``None``

    :return: name of the new column
    :rtype: str
    """

    max_len = 100 if 'max_len' not in kwargs else kwargs['max_len']
    alias = kwargs['alias'] if 'alias' in kwargs else None

    if alias is not None:
        return alias
    expr = list(args)
    for i in range(len(expr)):
        if expr[i] is None:
            expr[i] = 'None'
        if len(expr[i]) > max_len:
            expr[i] = expr[i][:max_len]

    return CONSTANTS.DELIMITER.join(expr)

def make_alias(base_col_name, function_name, window_name, partition_col_name, order_col_name, max_len=100, alias=None):
    """
    Name new column resulting from applying a function named ``function_name``, window named ``window_name``, grouping
    named ``partition_col_name`` and an order specified by ``order_col_name``.

    ``base_col_name``, ``function_name``, ``window_name``, ``partition_col_name``, and ``order_col_name`` would be 
    truncated to ``max_len`` if their length exceeds the threshold. In addition, if any of them is ``None``, they 
    would be replaced with string ``'None'``.

    :param str base_col_name: original column name
    :param str function_name: name of the function
    :param str window_name: name of the window
    :param str partition_col_name: name of the column for partitioning
    :param str order_col_name: name of the column for ordering
    :param int max_len: maximum length of each component including ``base_col_name``, ``function_name``, ``window_name``, 
        ``partition_col_name``, and ``order_col_name``
    :param str alias: pre-defined alias for the column. If not ``None``, it is the name of the new column

    :return: name of the new column
    :rtype: str
    """

    return general_make_alias(base_col_name, function_name, window_name, 'by', partition_col_name, order_col_name, max_len=max_len, alias=alias)

def truncate(seq, delta):
    """
    Truncate the elements in ``seq`` by ``abs(delta)``. The truncate is applied to left of ``seq`` if ``delta`` is negative; to right if positive.

    :param list seq: input sequence
    :param int delta: the number of elements to truncate. The truncate is applied to left of ``seq`` if ``delta`` is negative; to right if positive.

    :return: truncated sequence
    :rtype: list
    """
    if delta == 0:
        return seq
    elif delta > 0:
        return seq[delta:]
    else:
        return seq[:-delta]


def get_hot_spot_values(t, colname, threshold, groupby_aggregator=None):
    """
    Function to find hot spot values of ``colname`` of table ``t``. Return ``None`` if no hot spot exists.

    :param DataFrame t: input table
    :param str colname: the name of column to which the verification is performed
    :param float threshold: threshold above which the value of ``colname`` is considered hot spot
    :param groupby_aggregator: ``None`` or :py:class:`groupby_aggregate:GroupbyAggregator`; if not ``None``, ``groupby_aggregator`` would be used 
        to obtain hot-spot values with hot-spot optimization
    :return: hot spot values of the column named ``colname``
    :rtype: set or ``None``
    """

    n_colname = CONSTANTS.DELIMITER.join(['n',colname])
    if groupby_aggregator is None:
        res = t.select(colname).groupBy(colname).agg(
            count(colname).alias(n_colname)
            )
    else:
        config = {
            'groupby_col':[colname],
            'target_col':[
                (['cnt'],[(colname, n_colname)]),
                ],
            'is_skewed': True
        }
        res = groupby_aggregator.aggregate(t, **config)
    hot_spot_vals = res.where(col(n_colname) > threshold)\
                    .rdd\
                    .map(lambda x:list(x)[0])\
                    .collect()

    return set(hot_spot_vals)

def find_ge(a, x, lo=None, hi=None):
    """
    Find the index of leftmost item in ``a[lo:hi]`` greater than or equal to ``x``. If no valid index found, return ``None``

    :param list a: input sequence
    :param x: value to look for in ``a``
    :param int lo: lower bound
    :param int hi: upper bound

    :return: index of leftmost item in ``a[lo:hi]`` greater than or equal to ``x``. If no valid index found, return ``None``
    :rtype: int or None
    """

    lo = lo if lo is not None else 0
    hi = hi if hi is not None else len(a)

    i = bisect.bisect_left(a, x, lo, hi)
    if i < hi:
        return i
    else:
        return None

def find_le(a, x, lo=None, hi=None):
    """
    Find the index of rightmost item in ``a[lo:hi]`` less than or equal to ``x``. If no valid index found, return ``None``

    :param list a: input sequence
    :param x: value to look for in ``a``
    :param int lo: lower bound
    :param int hi: upper bound

    :return: index of rightmost item in ``a[lo:hi]`` less than or equal to ``x``. If no valid index found, return ``None``
    :rtype: int or None
    """

    lo = lo if lo is not None else 0
    hi = hi if hi is not None else len(a)

    i = bisect.bisect_right(a, x, lo, hi)
    if i > lo:
        return i-1
    else:
        return None

def sorted_handle_none(a, *args, **kwargs):
    """
    Extension of built-in :py:func:`sorted` function with special treatment of key equal to ``None``:
        
    - If sorted in asending order, ``None`` is located at the rightmost position;
    - otherwise, at the leftmost position.

    :param a: input sequence
    :param \*args: arguments passed to :py:func:`sorted`
    :param \*\*kwargs: keyword arguments passed to :py:func:`sorted`

    :return: sorted sequence
    """

    a_none = []
    a_other = []
    key = lambda x:x
    if 'key' in kwargs:
        key = kwargs['key']
    
    for x in a:
        if key(x) is None:
            a_none.append(x)
        else:
            a_other.append(x)
    a_other_sorted = sorted(a_other, *args, **kwargs)
    
    reverse = False if 'reverse' not in kwargs else kwargs['reverse']
    if reverse:
        return a_none+a_other_sorted
    else:
        return a_other_sorted+a_none

def min_handle_none(a, *args, **kwargs):
    """
    Extension of built-in :py:func:`min` function with special treatment of key equal to ``None``:
        
    - If ``reverse`` is ``False``, ``None`` is considered largest
    - otherwise, smallest

    :param a: input sequence
    :param \*args: arguments passed to :py:func:`min`
    :param \*\*kwargs: keyword arguments including ``reverse`` and those passed to :py:func:`min`

    :return: the smallest element
    """

    if not a:
        raise ValueError('min_handle_none() arg is an empty sequence')

    none_item = None
    exist_none = False
    a_non_none = []

    key = lambda x:x
    if 'key' in kwargs:
        key = kwargs['key']

    if 'reverse' in kwargs:
        reverse = kwargs['reverse']
        del kwargs['reverse']
    else:
        reverse = False
    
    for x in a:
        if key(x) is None:
            if exist_none is False:
                exist_none = True
                none_item = x
        else:
            a_non_none.append(x)

    if a_non_none:
        res = min(a_non_none, *args, **kwargs)
    else:
        res = None
    
    if (reverse and exist_none) or res is None:
        return none_item
    else:
        return res

def gen_random_str(n=10, seed=None):
    """
    Generate random string with length equal to ``n``, composed by ASCII letters and digits.

    :param int n: length of resulting string
    :param seed: seed passed to :py:func:`random`

    :return: random string
    """
    random.seed(seed)
    res = ''.join(random.sample(string.ascii_letters + string.digits, n))
    return res

def update_dict(target, update, allow_common_keys=False):
    """
    This function is an extension of built-in :py:func:`update` method of ``dict``. 
    It updates ``target`` with ``update`` with special treatment of common keys:

    - If common keys are allowed, the behavior is identical to the built-in ``dict.update`` method
    - Otherwise, it would explicitly raises ``AssertionError``.

    :param dict target: target ``dict`` to be updated
    :param dict update: ``dict`` to update target ``dict``
    :param bool allow_common_keys: set to ``True`` if common keys are allowed; otherwise set to ``False``

    :return: None
    """

    if not allow_common_keys:
        common_keys = set(target.keys()) & set(update.keys())
        assert len(common_keys) == 0, 'Common key(s) {} exist(s)!'.format(','.join(common_keys))

    target.update(update)

def assert_target_col(target_col, functions):
    """
    Utility function to assert target columns.

    :param target_col: target columns
    :param list functions: candidate functions
    """
    for func_keys, cols in target_col:
        for func_key in func_keys:
            func_key = func_key[0] if isinstance(func_key, tuple) else func_key
            if func_key not in functions:
                raise ValueError('{} is not supported!'.format(func_key))
            elif 'num' == func_key and len(func_keys) > 1:
                raise ValueError('Duplicated numerical functions!')

def normalize_target_col(target_col):
    """
    Normalize `target_col` so that each target column is assigned an alias

    :param target_col: target columns

    :return: normalized target columns with aliases
    """
    for func_keys, cols in target_col:
        for i in range(len(cols)):
            if isinstance(cols[i], str):
                cols[i] = (cols[i], None)

def process_hot_spot_fillna(t, cols, na=None):
    """
    Function to process hot spot DataFrame and fill ``cols`` with ``na``

    :param DataFrame t: input table
    :param cols: column names of new features
    :param na: null value to fill

    :return: DataFrame
    """

    updates = list(t.columns)
    for col in cols:
        updates.append(F.lit(na).alias(col))
    t = t.select(*updates)

    return t
