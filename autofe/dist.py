from __future__ import print_function,division
import bisect
from pyspark.sql.functions import count,col
from math import log
import functools
import sys
import copy
from math import log,ceil,floor
import functools
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,ArrayType
from pyspark.sql import functions as F
from pyspark.sql import window as W
import os
import sys
import copy
import functools
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

DELIMITER = '_'
NA = None
NULL = 'NULL'# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################


def name_new_col(base_col_name, function_name, window_name, partition_col_name):
    """
    Name new column
    """

    return DELIMITER.join([base_col_name, function_name, window_name, 'by', partition_col_name])

def truncate(seq, delta):
	"""
	Truncate the elements in `seq` by `abs(delta)`. The truncate applied to left `delta` is negative and right if positive.

	:param seq: list
	:param int delta: the number of truncate. The truncate applied left `delta` is negative and right if positive.
	"""
	if delta == 0:
		return seq
	elif delta > 0:
		return seq[delta:]
	else:
		return seq[:-delta]


def get_hot_spot_values(t, colname, threshold):
	"""
	Function to find hot spot values of `colname` of `t`. Return None if no hot spot exists.

	:param t: DataFrame
	:param str colname: the name of column to which the verification is performed
	:param float threshold: threshold above which the value of `colname` is considered hot spot
	:return: set of values or None
	"""
	n_colname = DELIMITER.join(['n',colname])
	hot_spot_vals = t.select(colname).groupBy(colname).agg(
		count(colname).alias(n_colname)
		)\
	.where(col(n_colname) > threshold)\
	.rdd\
	.map(lambda x:list(x)[0])\
	.collect()

	return set(hot_spot_vals)

def find_ge(a, x, lo=None, hi=None):
	"""
	Find the index of leftmost item in `a[lo:hi]` greater than or equal to `x`. If no valid index found, return None
	"""
	lo = lo if lo is not None else 0
	hi = hi if hi is not None else len(a)

	i = bisect.bisect_left(a, x, lo, hi)
	if i != len(a):
		return i
	else:
		return None

def find_le(a, x, lo=None, hi=None):
	"""
	Find the index of rightmost item in `a[lo:hi]` less than or equal to `x`. If no valid index found, return None
	"""

	lo = lo if lo is not None else 0
	hi = hi if hi is not None else len(a)

	i = bisect.bisect_right(a, x, lo, hi)
	if i:
		return i-1
	else:
		return None

def sorted_handle_none(a, *args, **kwargs):
    """
    Extension of built-in `sorted` function with special treatment of key equal to `None`:
        If sorted in asending order, None is located at the rightmost position; otherwise, at the leftmost position.
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


# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

# TODO: function as singleton class (match order_col_fine)
# TODO: add variance function

"""
This module contains functions including aggregate functions, bucketize functions, as well as fillna 
"""


# helper function
def _check_valid_boundary(left, right):
    """
    Function to check if left and right boundary is valid
    """
    if left is None or right is None or left>right:
        return False
    else:
        return True

# decorator to check boundary if valid
def check_boundary(val=NA):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            left = kwargs['left'] if 'left' in kwargs else None
            right = kwargs['right'] if 'right' in kwargs else None
            if not _check_valid_boundary(left, right):
                return val
            else:
                return function(*args, **kwargs)
        return wrapper
    return decorator

# decorator to cast result to string
def cast_to(rtype=float):
    """
    Cast result to `rtype` if it is not `None`
    """
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            if 'rtype' in kwargs:
                _rtype = kwargs['rtype'] 
                del kwargs['rtype']
            else:
                _rtype = rtype
            res = function(*args, **kwargs)
            try:
                return _rtype(res)
            except:
                return res
        return wrapper
    return decorator

# decorator to discretize result
def transform(transformer):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            transformer_key = 'transformer'
            if transformer_key in kwargs:
                _transformer = kwargs[transformer_key]
                del kwargs[transformer_key]
            else:
                _transformer = transformer
            res = function(*args, **kwargs)
            res = _transformer(res) if res != NA else res
            return res
        return wrapper
    return decorator

# functions to discretize
def identity():
    """
    It does nothing, just for purpose of testing
    """
    return lambda x:x

def discrete_0_INF(N = 2):
    

    return lambda x: round(log(x+1), N)

def discrete_0_1(N = 2):
    
    return lambda x: round(-log(1.0002/(0.0001+x)-1), N)

def discrete_NINF_INF(N = 2):
    
    return lambda x:round(log(x+1), N) if (x>=0) else -round(log(-x+1), N)

# functions to fill na
def fillna(x):
    """
    Function to fill na
    """
    if x is None or x == '':
        return NULL
    elif x == []:
        return NA
    else:
        return x

#aggregate function
@cast_to()
@transform(discrete_NINF_INF(1))
@check_boundary()
def fsum(a, query=None, left=None, right=None):
    """
    Compute sum of sub sequence `a[left:right+1]`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: preserved
    """

    return sum(float(a[i]) for i in range(left, right+1))

@cast_to()
@transform(discrete_NINF_INF(1))
@check_boundary()
def fmax(a, query=None, left=None, right=None):
    """
    Find maximum value in the sub sequence `a[left:right+1]`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: preserved
    """
    
    return max(float(a[i]) for i in range(left, right+1))

@cast_to()
@check_boundary()
def ifexist(a, query, left=None, right=None):
    """
    Check if `query` exists in the sub sequence ranging from `left` to `right` (inclusive) in `a`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: element in question
    """

    try:
        _ = a.index(query, left, right+1)
        return 1
    except ValueError:
        return 0

@cast_to()
@transform(discrete_0_INF(1))
@check_boundary()
def existcount(a, query, left=None, right=None):
    """
    Count the number of times `query` appearing in the sub-sequence `a[left:right+1]`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: element in question
    """

    return a[left:right+1].count(query)

@cast_to()
@transform(discrete_0_INF(1))
@check_boundary()
def countdistinct(a, query=None, left=None, right=None):
    """
    Count the number of distinct elements in sub-sequence of `a`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: preserved
    """

    return len(set(a[left:right+1]))

@cast_to(str)
@check_boundary()
def last(a, query=None, left=None, right=None):

    """
    Return the last element in the sub-sequence of `a`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: preserved
    """

    return a[right]

@cast_to()
@transform(discrete_0_1(1))
@check_boundary()
def ratio(a, query, left=None, right=None, prior=None):
    """
    Compute the (weighted) ratio between the number of times of `query` and the length of sub-sequence of `a`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: element in question
    :param prior: prior probability of `query`
    """

    sub_seq = a[left:right+1]
    res = 1.0*sub_seq.count(query)/len(sub_seq)
    if prior is not None:
        reg_coef = 1 - 1/(len(sub_seq)+1) # the more data, the more weight we put on the ratio
        res = reg_coef*res + (1 - reg_coef)*prior
    return res

@cast_to()
@transform(discrete_NINF_INF(2))
@check_boundary()
def entropy(a, query=None, left=None, right=None):
    """
    Compute the entropy of sub-sequence of `a`

    :param a: iterable
    :param left: int or None
    :param right: int or None
    :param query: preserved
    """

    count = {}
    for i in range(left, right+1):
        count[a[i]] = count.get(a[i], 0) + 1
        
    total = sum(count.values())
    probs = [1.0*v/total for v in count.values()]
    res = sum(prob*log(prob) for prob in probs)

    return abs(res)

# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

# TODO: fix bugs
# TODO: finall transform to DataFrame, use repartitionAndSortWithinPartitions



# hyperparameters
NO_DISCARD_TRUNCATE_THRESHOLD = 10000
# DISCARD_TRUNCATE_THRESHOLD = 100000

def window_aggregate_per_partition(windows, aggregate_functions, partition_col, target_cols, order_col, fields):
    """
    This function returns a function computing features from `target_cols` by applying `aggregate_functions` 
    to a set of `windows` partitioned by `partition_col`, ordered by `order_col`

    :param windows: list of `dict`, each of which contains keys being 'name', 'range', 'row'
        the value corresponding to `range` and `row` is supposed to be a binary `tuple`, where the first element is either non-negative or `None`
        while the second one is non-negative for `range`, and `None` or non-negative for `row`. When both elements in the `tuple` are non-negative, 
        the first element should be greater than the second one.
        When the first element in the `tuple` corresponding to `range` is `None`, the range is unconstrained for left side. The same interpretation
        works for `row`. The final window is the intersection of `range` and `row`.
    :param dict aggregate_functions: map function name to function
    :param partition_col: `string`
    :param target_cols: list of `string`
    :param order_col: `string`
    :param fields: list of `string`, field name of table

    """

    def _assert_window(window):
        assert window['range'][1] is not None and window['range'][1] >= 0, 'Right value must be non-negative!'
        if window['range'][0] is not None:
            assert window['range'][0] >= window['range'][1], 'Left value must be no less than right value!'
        if window['row'][0] is not None and window['row'][1] is not None:
            assert window['row'][0] >= window['row'][1], 'Left value must be no less than right value!'

    for window in windows:
        _assert_window(window)

    # sort windows with repect to left and right range boundaries
    window_indices = list(range(len(windows)))
    window_indices_left_sorted = sorted_handle_none(window_indices, key=lambda x:windows[x]['range'][0], reverse=True)
    window_indices_right_sorted = sorted_handle_none(window_indices, key=lambda x:windows[x]['range'][1], reverse=True)
    # window_indices_bisorted[i][j] for i = 0, 1, ... are indices of windows sorted 
    # w.r.t. left range boundary if j = 0 or right range boundary otherwise
    window_indices_bisorted = list(zip(window_indices_left_sorted, window_indices_right_sorted))

    def _initialize():
        """
        Initialize and return relevant variables
        """

        target_col_history = {target_col:[] for target_col in target_cols}
        timeline = []
        timeline_indices = [[None, None] for _ in range(len(windows))]

        return target_col_history, timeline, timeline_indices

    def _update_timeline_index(timeline, range_boundary, is_left, prev_timeline_index):
        """
        Function to update timeline index. Return None if no valid boundary found
        """

        def _conditional_flip(bool, condition):
            """
            Flip `bool` if `condition` is True
            """

            return not bool if condition else bool

        def _check_beyond_range(cur, timeline_boundary, range_boundary, is_left):
            """
            Check whether the `timeline_boundary` with respect to the current time is beyond the range boundary:

            If the boundary we check is left, when `cur - timeline_boundary > range_boundary`, the timeline_boundary 
                is beyond boundary; if the boundary we check is right, when `cur - timeline_boundary < range_boundary`, the timeline_boundary 
                is beyond boundary
            """

            inverse = 1 if is_left else -1
            check_result = inverse*(cur - timeline_boundary) > inverse*range_boundary

            return check_result

        # if boundary is undefined, we keep all history
        if range_boundary is None:
            return 0
        else:
            timeline_index = 0 if prev_timeline_index is None else prev_timeline_index
            while timeline_index < len(timeline) and (_conditional_flip(_check_beyond_range(timeline[-1], timeline[timeline_index], range_boundary, is_left), not is_left)):
                timeline_index += 1
            if not is_left:
                timeline_index -= 1
            # check if index is correct
            if not _check_beyond_range(timeline[-1], timeline[timeline_index], range_boundary, is_left):
                return timeline_index
            else:
                return None

    def _update_timeline_index_fast(timeline, range_boundary, is_left, prev_timeline_index):

        # if boundary is undefined, we keep all history
        if range_boundary is None:
            return 0

        val = timeline[-1] - range_boundary
        if is_left:
            # find the leftmost index of item in timeline greater than or equal to timeline[-1] - range_boundary
            return find_ge(timeline, val, lo=prev_timeline_index)
        else:
            # find the rightmost index of item in timeline less than or equal to timeline[-1] - range_boundary
            return find_le(timeline, val, lo=prev_timeline_index)

    def _window_aggregate_per_partition(iterator):
        """
        Window aggregate function
        """

        sentinel = None
        target_col_history, timeline, timeline_indices = _initialize()
        for row in iterator:
            item = row.asDict()

            if item[partition_col] != sentinel:
                sentinel = item[partition_col]
                target_col_history, timeline, timeline_indices = _initialize()

            # append new item
            timeline.append(item[order_col])
            for col in target_cols:
                target_col_history[col].append(item[col])

            # update indices for timeline
            for sorted_index in range(len(window_indices_bisorted)):
                for side in range(2):
                    window_idx = window_indices_bisorted[sorted_index][side]
                    window = windows[window_idx]
                    if sorted_index == 0:
                        prev_timeline_index = timeline_indices[window_idx][side]
                    else:
                        prev_timeline_index = timeline_indices[window_indices_bisorted[sorted_index-1][side]][side]
                    timeline_indices[window_idx][side] = _update_timeline_index_fast(timeline, 
                                                                                window['range'][side], 
                                                                                side == 0,
                                                                                prev_timeline_index
                                                                                )

                    # filter rows
                    if window['row'][side] is not None and timeline_indices[window_idx][side] is not None:
                        timeline_row_index = len(timeline) - 1 - window['row'][side]
                        if side == 0:
                            timeline_indices[window_idx][side] = max(timeline_row_index, timeline_indices[window_idx][side])
                        else:
                            timeline_indices[window_idx][side] = min(timeline_row_index, timeline_indices[window_idx][side])
                    if timeline_indices[window_idx][side] is not None \
                        and (timeline_indices[window_idx][side] < 0 or timeline_indices[window_idx][side] >= len(timeline)):
                        timeline_indices[window_idx][side] = None
            
            # truncate
            def _truncate(timeline, target_col_history, timeline_indices, delta):
                """
                Truncate `timeline` and `target_col_history` by `delta` and adjust `timeline_indices`
                """

                timeline = truncate(timeline, delta)
                for i in range(len(timeline_indices)):
                    for side in range(2):
                        if timeline_indices[i][side] is not None:
                            timeline_indices[i][side] -= delta
                for col in target_cols:
                    target_col_history[col] = truncate(target_col_history[col], delta)

                return timeline, target_col_history, timeline_indices

            # truncate no discard
            smallest_left_boundary = min(timeline_indices, key=lambda x:x[0])[0] # find smallest index corresponding to left boundary
            if smallest_left_boundary >= NO_DISCARD_TRUNCATE_THRESHOLD:
                timeline, target_col_history, timeline_indices = _truncate(timeline, target_col_history, timeline_indices, smallest_left_boundary)

            assert len(timeline) == len(target_col_history[target_cols[0]]), 'Inconsistent length!'
            for col in target_cols:
                for function_name, function in aggregate_functions.items():
                    for window_idx, window in enumerate(windows): # TODO: adjust indices!
                        item[name_new_col(col, function_name, window['name'], partition_col)] = function(
                            target_col_history[col], 
                            left=timeline_indices[window_idx][0],
                            right=timeline_indices[window_idx][1],
                            query=item[col])

            res = [item[colname] for colname in fields]

            yield res

    return _window_aggregate_per_partition

def map_partitions(t, windows, aggregate_functions, partition_col, target_cols, order_col, partition_num, new_cols, repartition_col=None):
    # update schema
    schema = copy.deepcopy(t.schema)
    for col in new_cols:
        schema.add(StructField(col,DoubleType(),True)) # double for gbdt
    fields = [x.name for x in schema.fields]
    repartition_col = repartition_col if repartition_col is not None else [partition_col]
    t = t.repartition(partition_num, *repartition_col)\
            .sortWithinPartitions([partition_col, order_col])\
            .rdd\
            .mapPartitions(
                window_aggregate_per_partition(windows, aggregate_functions, partition_col, target_cols, order_col, fields)
                )\
            .toDF(schema)

    return t

def process_hot_spot_fillna(t, cols, na=None):
    """
    Function to process hot spot DataFrame and fill `cols` with `na`

    :param t: DataFrame
    :param cols: column names of new features
    :param na: null value to fill
    """

    t = functools.reduce(lambda df,col: df.withColumn(col, F.lit(na)), cols, t)
    return t

def process_hot_spot(t, windows, partition_col, order_col, num_subsets, hot_spot_vals, sql_context):
    """
    Function ot process hot spot

    :param t: DataFrame
    :param num_subsets: must be greater than 1
    """

    def _process_hot_spot_single_val(t, num_subsets, max_range, max_row):
        """
        :param max_range: `max_range` and `max_row` must not be None at the same time
        :param max_row: `max_range` and `max_row` must not be None at the same time
        """

        num_rows = t.select(F.max(row_number_col))\
            .rdd\
            .map(lambda x:list(x)[0])\
            .collect()[0]
        if num_rows < num_subsets:
            num_subsets = num_rows
        subset_size = floor(num_rows/num_subsets)
        num_subsets = int(ceil(num_rows/subset_size)) # update num_subsets (e.g., 5 rows and initial num_subsets set to 4 -> subset_size is 1 -> num_subsets should be 5)
        left_row_nums = [1+subset_id*subset_size for subset_id in range(num_subsets)]
        right_row_nums = [(subset_id+1)*subset_size for subset_id in range(num_subsets)]
        boundary_order_vals = t.where(F.col(row_number_col).isin(left_row_nums))\
                                      .select(order_col)\
                                      .rdd\
                                      .map(lambda x:list(x)[0])\
                                      .collect()

        # for each subset, prepend dependent rows
        for subset_id in range(num_subsets):
            left_row_num = left_row_nums[subset_id]
            right_row_num = right_row_nums[subset_id]
            t = t.withColumn(subset_id_col, 
                F.when(
                    F.col(row_number_col).between(left_row_num, right_row_num), 
                    F.array(F.lit(subset_id)).cast(ArrayType(IntegerType(), True)).alias(subset_id_col)
                    ).otherwise(F.col(subset_id_col))
                )

            if subset_id > 0: # skip the first subset
                boundary_order_val = boundary_order_vals[subset_id]
                subset_condition = None
                if max_range is not None:
                    subset_condition = (F.col(row_number_col) < left_row_num) & (F.col(order_col) >= (boundary_order_val - max_range))
                if max_row is not None:
                    row_condition = F.col(row_number_col).between(left_row_num - max_row, left_row_num - 1)
                    if subset_condition is None:
                        subset_condition = row_condition
                    else:
                        subset_condition = subset_condition & row_condition

                t = t.withColumn(subset_id_col,
                                F.when(subset_condition, 
                                    F.array(F.col(subset_id_col).getItem(0), F.lit(subset_id)).cast(ArrayType(IntegerType(), True)).alias(subset_id_col)
                                    ).otherwise(F.col(subset_id_col).cast(ArrayType(IntegerType(), True)).alias(subset_id_col))
                                )

                t = t.withColumn(subset_id_col, F.explode(F.col(subset_id_col)))
                t = t.withColumn(row_number_col, 
                                F.when(subset_condition & (F.col(subset_id_col) == subset_id), F.lit(-1)).otherwise(F.col(row_number_col))
                                )
                t = t.withColumn(subset_id_col, F.array(F.col(subset_id_col)).cast(ArrayType(IntegerType(), True)).alias(subset_id_col))

        return t
    
    assert num_subsets > 1, 'Number of subsets must be greater than 1!'
    # find max range and row
    range_and_row = ['range', 'row']
    max_range_and_row = {k:0 for k in range_and_row}
    for window in windows:
        for k in range_and_row:
            if window[k][0] is None: # unbounded
                max_range_and_row[k] = None
            elif max_range_and_row[k] is not None:
                max_range_and_row[k] = max(max_range_and_row[k], window[k][0])


    # if both max range and row is None, return None
    if all([v is None for v in max_range_and_row.values()]):
        return None

    # add column of row number and tmp subset id
    row_number_col = 'row_n'
    subset_id_col = 'subset_id'
    if sql_context is None:
        t = t.withColumn(
                row_number_col, 
                F.row_number().over(W.Window.partitionBy(partition_col).orderBy(order_col)))
    else:
        sql_context.registerDataFrameAsTable(t, 't')
        t = sql_context.sql('select *, row_number() over (partition by {} order by {}) as {} from t'.format(partition_col, order_col, row_number_col))
    t = t.withColumn(subset_id_col, F.array(F.lit(-1)).cast(ArrayType(IntegerType(), True)).alias(subset_id_col))

    res = None
    for val in hot_spot_vals:
        res_single_val = _process_hot_spot_single_val(
            t.where(F.col(partition_col) == val),
            num_subsets,
            max_range_and_row[range_and_row[0]],
            max_range_and_row[range_and_row[1]],
            )
        if res is None:
            res = res_single_val
        else:
            res = res_single_val.unionAll(res)

    return res

def window_aggregate(t, windows, aggregate_functions, partition_cols, target_cols, order_col, partition_num, hot_spot_threshold, num_subsets, sql_context=None):
    """
    This function returns a function computing features from `target_cols` by applying `aggregate_functions` 
    to a set of `windows` partitioned by respectively a set of `partition_col`, ordered by `order_col`

    :param int num_subsets: number of subsets. When equal to 1, hot spot features are filled with None
    """

    row_number_col = 'row_n'
    subset_id_col = 'subset_id'
    
    for partition_col in partition_cols:
        new_cols = []
        for target_col in target_cols:
            for function_name in aggregate_functions.keys():
                for window in windows:
                    new_cols.append(name_new_col(target_col, function_name, window['name'], partition_col))
        
        # tackle hot spot
        hot_spot_vals = get_hot_spot_values(t,partition_col,hot_spot_threshold)
        t_non_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals) == False)
        t_non_hot_spot_res = map_partitions(t_non_hot_spot, windows, aggregate_functions, partition_col, target_cols, order_col, partition_num, new_cols)
        if hot_spot_vals:
            t_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals))
            t_hot_spot_overlap = None
            # either fillna or advanced processing
            if num_subsets > 1:
                t_hot_spot_overlap = process_hot_spot(t_hot_spot, windows, partition_col, order_col, num_subsets, hot_spot_vals, sql_context)
            
            if t_hot_spot_overlap is None:
                t_hot_spot_res = process_hot_spot_fillna(t_hot_spot, new_cols, None)
            else:
                t_hot_spot_res = map_partitions(
                    t_hot_spot_overlap, 
                    windows, 
                    aggregate_functions, 
                    partition_col, 
                    target_cols, 
                    order_col, 
                    partition_num, 
                    new_cols, 
                    repartition_col=[partition_col, subset_id_col])
                t_hot_spot_res = t_hot_spot_res.where(F.col(row_number_col) != -1).drop(subset_id_col).drop(row_number_col)
            
        
            t = t_non_hot_spot_res.unionAll(t_hot_spot_res)
        else:
            t = t_non_hot_spot_res

    return t
    
def _generate_functions(function_names):
    functions = {}
    for fname in set(function_names):
        function = eval(fname)
        if fname not in ['ifexist', 'last']:
            functions[fname] = functools.partial(function, transformer=identity(), rtype=float)
        else:
            functions[fname] = functools.partial(function, rtype=float)
    return functions

def run(t1, context_string):
    try:
        sc = SparkContext._active_spark_context
    except:
        sc = SparkContext()
    sqlContext = HiveContext(sc)

    NUM_SUBSETS_SHORT = 20
    NUM_SUBSETS_LONG = 1
    NUM_PARTITIONS = 200
    HOT_SPOT_THRESHOLD = 10000
    windows_short = [
        {'name':'10s','range':(10,0),'row':(None,None)},
        {'name':'1m','range':(60,0),'row':(None,None)},
        {'name':'5m','range':(60*5,0),'row':(None,None)},
        {'name':'30m','range':(60*30,0),'row':(None,None)},
        {'name':'2h','range':(3600*2,0),'row':(None,None)},
        {'name':'12h','range':(3600*12,0),'row':(None,None)},
        {'name':'1d','range':(3600*24,0),'row':(None,None)},
        {'name':'2d','range':(3600*24*2,0),'row':(None,None)},
        {'name':'5d','range':(3600*24*5,0),'row':(None,None)},
        ]

    windows_long = [{'name':'1mo','range':(3600*24*30,0),'row':(None,None)}]

    timeWindow_str_functions = ["ifexist","existcount","countdistinct","entropy"]
    timeWindow_num_functions = ['fsum','fmax','ifexist','existcount','countdistinct','entropy']
    timeWindow_long_functions = ["ifexist","existcount","countdistinct"]
    timeWindow_functions = ['fsum','fmax','ifexist','existcount','countdistinct','entropy']    

    order_col = "trx_time_std"
    partition_cols = ['cardno','umid']

    target_str_cols = ['cardno','umid','responsecode','amt_bin','isweekend','weekday','hour','trx_time_diff']#,'x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20']
    target_num_cols = ['amt']

    t1 = window_aggregate(t1, windows_short, _generate_functions(timeWindow_str_functions), partition_cols, target_str_cols, order_col, NUM_PARTITIONS, HOT_SPOT_THRESHOLD, NUM_SUBSETS_SHORT, sqlContext)
    t1 = window_aggregate(t1, windows_short, _generate_functions(timeWindow_num_functions), partition_cols, target_num_cols, order_col, NUM_PARTITIONS, HOT_SPOT_THRESHOLD, NUM_SUBSETS_SHORT, sqlContext)
    t1 = window_aggregate(t1, windows_long, _generate_functions(timeWindow_long_functions), partition_cols, target_str_cols, order_col, NUM_PARTITIONS, HOT_SPOT_THRESHOLD, NUM_SUBSETS_LONG, sqlContext)

    return [t1]

# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################




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
        function = eval('{}'.format(fname))
        if fname not in ['ifexist', 'last']:
            aggregate_functions[fname] = functools.partial(function, transformer=identity())
        else:
            aggregate_functions[fname] = function

    partition_cols = ['C_MAC_ADDR']
    target_cols = ["N_TXN_AMT_RMB",]
    order_col = 'D_TXN_TIME_std'

    hot_spot_threshold = 2
    partition_num = 2
    num_subsets = 3
    res = window_aggregate(t, windows, aggregate_functions, partition_cols, target_cols, order_col, partition_num, hot_spot_threshold, num_subsets, None)
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
