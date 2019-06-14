# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division
import sys
import copy
import inspect
from math import log,ceil,floor
import functools

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,ArrayType
from pyspark.sql import functions as F
from pyspark.sql import window as W

from .. import utils
from . import py_functions
from ..aggregate import groupby_aggregate

class WindowAggregator(object):
    """
    This class is deprecated and is the old version of :py:class:`autofe.window.window_aggregate.WindowAggregator`. 
    It provides the functionality of window-aware aggregate similar to ``partition by`` offered in ``SQL``. 
    The upper bound of time complexity could be O(N^2) whereas in practice it is rarely acheived. 
    Note that :py:class:`WindowAggregator` ignores ``None`` value. For example, it does not count ``None``. 
    In case that this behaviour is not desired, 
    it is recommended to replace ``None`` in relevant columns with non-empty value such as ``-1``.
    """

    def __init__(self, sql_context, NO_DISCARD_TRUNCATE_THRESHOLD=10000):
        """
        The constructor of :py:class:`WindowAggregator`.

        :param sql_context: should be ``pyspark.sql.HiveContext`` object
        :param int NO_DISCARD_TRUNCATE_THRESHOLD: the threshold to discard old history. Reset it only when you fully comprehend the code!!!
        """
        
        self.sql_context = sql_context
        self.NO_DISCARD_TRUNCATE_THRESHOLD = NO_DISCARD_TRUNCATE_THRESHOLD
        self._filter_history = self._make_filter()

        self.__functions = {}
        for name,member in inspect.getmembers(py_functions, inspect.isclass):
            mro = inspect.getmro(member)
            if mro[1] == py_functions.AggregateFunction:
                self.__functions[member.name()] = member()

    @property
    def functions(self):
        """
        Return supported functions. 

        :return: a ``dict`` mapping function name to function object
        """

        return self.__functions

    def _make_filter(self, condition=None):
        def filter_history(row_history, timeline_indices, window_idx, target_col_history, col):
            # handle case of ``None``
            if timeline_indices[window_idx][0] is None or timeline_indices[window_idx][1] is None:
                target_col_history_filtered = []
            else:
                target_col_history_filtered = target_col_history[col][timeline_indices[window_idx][0]:timeline_indices[window_idx][1]+1]
                row_history_filter = row_history[timeline_indices[window_idx][0]:timeline_indices[window_idx][1]+1]
                if condition is not None:
                    target_col_history_filtered = [v[0] for v in zip(target_col_history_filtered, row_history_filter) if condition(v[1])]
                target_col_history_filtered = [v for v in target_col_history_filtered if v is not None]
            
            return target_col_history_filtered

        return filter_history

    @staticmethod
    def _window_aggregate_per_partition(windows, partition_col, target_col, order_col, 
                                        fields, functions, NO_DISCARD_TRUNCATE_THRESHOLD, filter_history):
        """
        This function returns a function computing time-series features from ``target_col`` by applying ``aggregate_functions`` 
        to a set of ``windows`` partitioned by ``partition_col``, ordered by ``order_col``.

        Note that this function is not intended to be used directly. Use :py:func:`window_aggregate` instead to perform window-aware aggregate.

        :param windows: list of ``dict``, each of which contains keys being ``name``, ``range``, and ``row``.
            The value of ``name`` indicates the window name.
            The value corresponding to ``range`` and ``row`` is supposed to be a binary ``tuple``, 
            which specifies temporal interval similar to ``range between`` and ``rows between`` in ``SQL``.

            The first element of both ``tuple`` is either non-negative or ``None``,
            whereas the second one is non-negative for ``range``, and ``None`` or non-negative for ``row``. 
            When both elements in the ``tuple`` are non-negative, the first element should be greater than the second one.
            When the first element in the ``tuple`` corresponding to ``range`` is ``None``, the range is unconstrained for left side. The same interpretation
            works for ``row``.

            The final window is the intersection of `range` and `row`, which extends the functionality provided in ``SQL`` in that
            we are able to specify both range and rows.

            One constraint applied by this function is that the time window must be backward. In other words, this function does not allow
            to utilize information in future.

            An example of ``windows`` is as follows:

            .. code-block:: python

                [{'name':'1min','range':(60,0),'row':(10,None)}],

            which indicates a window named ``1min`` with left boundary being the minimum of one minute and 10 rows, and right boundary being current row.

        :param partition_col: str, or list of str, or ``None``. When it is ``None``, the aggregate would be performed without
            grouping. Otherwise, it contains column name(s) for partitioning by.
        :param target_col: list of binary tuples, each of which contains list of function name(s) and list of target column(s), for example, 
            the ``target_col`` could be:

            .. code-block:: python

                [
                    (['max','avg'],['N_TXN_AMT_RMB']),
                    (['max'],['TARGET'])
                ]

            which indicates that we intend to perform max and average aggregate on ``N_TXN_AMT_RMB`` and only max on ``TARGET``.

            Allowed function names can be found in the API documentation of :py:mod:`autofe.legacy.py_functions`. For 
            example, suppose that one user seeks to utilize :py:class:`autofe.legacy.py_functions.MaxFunction`. Then, the user should 
            refer to the table in :py:mod:`autofe.legacy.py_functions`, which indicates that the function name is `max`.

            It is also flexible to specify the alias (i.e., column name) of a computed feature as follows:

            .. code-block:: python

                [
                    (['avg'],[('N_TXN_AMT_RMB', 'avg_amt')]),
                    (['max'],[('TARGET', 'max_target')])
                ]

            Note that customized alias only supports **one** aggregate function to **many** target columns. 
            In other words, it does not support many-to-many configuration.

            In addition, extra parameters for one function can be specified through the following syntax:

            .. code-block:: python

                [
                    ([('rto',{'prior':0.5})],['N_TXN_AMT_RMB']),
                ]

            That is, we extend the function in question to a ``tuple`` whose first element refers to the function and second element 
            is a ``dict`` specifying extra parameters (other than ``left`` and ``right``) to pass to the function.

        :param str order_col: name of column for ordering
        :param fields: column names of resulting schema after computation
        :type fields: list of str

        :return: function to apply to partitions

        """

        def _assert_window(window):
            assert window['range'][1] is not None and window['range'][1] >= 0, 'Right value must be non-negative!'
            if window['range'][0] is not None:
                assert window['range'][0] >= window['range'][1], 'Left value must be no less than right value!'
            if window['row'][0] is not None and window['row'][1] is not None:
                assert window['row'][0] >= window['row'][1], 'Left value must be no less than right value!'

        target_col_set = list(set([col for _, cols in target_col for col, _ in cols]))

        for window in windows:
            _assert_window(window)

        # sort windows with repect to left and right range boundaries
        window_indices = list(range(len(windows)))
        window_indices_left_sorted = utils.sorted_handle_none(window_indices, key=lambda x:windows[x]['range'][0], reverse=True)
        window_indices_right_sorted = utils.sorted_handle_none(window_indices, key=lambda x:windows[x]['range'][1], reverse=True)
        # window_indices_bisorted[i][j] for i = 0, 1, ... are indices of windows sorted 
        # w.r.t. left range boundary if j = 0 or right range boundary otherwise
        window_indices_bisorted = list(zip(window_indices_left_sorted, window_indices_right_sorted))

        def _initialize(target_col_set):
            """
            Initialize and return relevant variables
            """

            target_col_history = {col:[] for col in target_col_set}
            row_history = []
            timeline = []
            timeline_indices = [[None, None] for _ in range(len(windows))]

            return target_col_history, row_history, timeline, timeline_indices

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

            # return None if timeline is empty
            if not timeline:
                return None

            val = timeline[-1] - range_boundary
            if is_left:
                # find the leftmost index of item in timeline greater than or equal to timeline[-1] - range_boundary
                return utils.find_ge(timeline, val, lo=prev_timeline_index)
            else:
                # find the rightmost index of item in timeline less than or equal to timeline[-1] - range_boundary
                return utils.find_le(timeline, val, lo=prev_timeline_index)

        def _window_aggregate_per_partition(iterator):
            """
            Window aggregate function
            """

            sentinel = None
            target_col_history, row_history, timeline, timeline_indices = _initialize(target_col_set)
            for row in iterator:
                item = row.asDict()
                if partition_col is not None and item[partition_col] != sentinel:
                    sentinel = item[partition_col]
                    target_col_history, row_history, timeline, timeline_indices = _initialize(target_col_set)

                # append new item
                timeline.append(item[order_col])
                row_history.append(item)
                for col in target_col_set:
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
                def _truncate(timeline, target_col_history, row_history, timeline_indices, delta):
                    """
                    Truncate `timeline` and `target_col_history` by `delta` and adjust `timeline_indices`
                    """

                    timeline = utils.truncate(timeline, delta)
                    for i in range(len(timeline_indices)):
                        for side in range(2):
                            if timeline_indices[i][side] is not None:
                                timeline_indices[i][side] -= delta
                    for col in target_col_set:
                        target_col_history[col] = utils.truncate(target_col_history[col], delta)
                    row_history = utils.truncate(row_history, delta)

                    return timeline, target_col_history, row_history, timeline_indices

                # truncate no discard
                smallest_left_boundary = utils.min_handle_none(timeline_indices, key=lambda x:x[0], reverse=False)[0] # find smallest index corresponding to left boundary or return None
                if smallest_left_boundary is not None and smallest_left_boundary >= NO_DISCARD_TRUNCATE_THRESHOLD:
                    timeline, target_col_history, row_history, timeline_indices = _truncate(timeline, target_col_history, row_history, timeline_indices, smallest_left_boundary)

                assert len(timeline) == len(target_col_history[target_col_set[0]]), 'Inconsistent length!'
                
                for window_idx, window in enumerate(windows):
                    for func_keys, cols in target_col:
                        for col,alias in cols:
                            target_col_history_filtered = filter_history(row_history, timeline_indices, window_idx, target_col_history, col)
                            '''
                            # handle case of ``None``
                            if timeline_indices[window_idx][0] is None or timeline_indices[window_idx][1] is None:
                                target_col_history_filtered = []
                            else:
                                target_col_history_filtered = target_col_history[col][timeline_indices[window_idx][0]:timeline_indices[window_idx][1]+1]
                                target_col_history_filtered = [v for v in target_col_history_filtered if v is not None]
                            '''

                            for func_key in func_keys:
                                if isinstance(func_key, tuple):
                                    extra_params = func_key[1]
                                    func_key = func_key[0]
                                else:
                                    extra_params = {}
                                
                                params = {'left':0, 'right':len(target_col_history_filtered)-1, 'query':item[col]}
                                utils.update_dict(params, extra_params, allow_common_keys=True)
                                if func_key in functions:
                                    item[utils.make_alias(col, functions[func_key].name(), 
                                        window['name'], partition_col, order_col, alias=alias)] = functions[func_key](
                                            target_col_history_filtered, 
                                            **params
                                            )

                res = [item[colname] for colname in fields]

                yield res

        return _window_aggregate_per_partition

    def map_partitions(self, t, windows=None, partition_col=None, target_col=None,
                        order_col=None, partition_num=200, new_cols=None, repartition_col=None):
        """
        This function performs window-aware aggregate to compute time-series features. 
        Aggregate functions specified by ``target_col`` are applied to the target columns also specified by ``target_col``,
        within windows specified by ``windows`` partitioned by ``partition_col``, and ordered by ``order_col``.

        Note that this function is not intended to be used directly. Use :py:meth:`aggregate` instead to perform window-aware aggregate, 
        which supports **multiple** partitions and hot-spot optimization.

        :param DataFrame t: input table    
        :param windows: list of ``dict``, each of which contains keys being ``name``, ``range``, and ``row``.
            The value of ``name`` indicates the window name.
            The value corresponding to ``range`` and ``row`` is supposed to be a binary ``tuple``, 
            which specifies temporal interval similar to ``range between`` and ``rows between`` in ``SQL``.

            The first element of both ``tuple`` is either non-negative or ``None``,
            whereas the second one is non-negative for ``range``, and ``None`` or non-negative for ``row``. 
            When both elements in the ``tuple`` are non-negative, the first element should be greater than the second one.
            When the first element in the ``tuple`` corresponding to ``range`` is ``None``, the range is unconstrained for left side. The same interpretation
            works for ``row``.

            The final window is the intersection of `range` and `row`, which extends the functionality provided in ``SQL`` in that
            we are able to specify both range and rows.

            One constraint applied by this function is that the time window must be backward. In other words, this function does not allow
            to utilize information in future.

            An example of ``windows`` is as follows:

            .. code-block:: python

                [{'name':'1min','range':(60,0),'row':(10,None)}],

            which indicates a window named ``1min`` with left boundary being the minimum of one minute and 10 rows, and right boundary being current row.

        :param partition_col: str, or list of str, or ``None``. When it is ``None``, the aggregate would be performed without
            grouping. Otherwise, it contains column name(s) for partitioning by.
        :param target_col: list of binary tuples, each of which contains list of function name(s) and list of target column(s), for example, 
            the ``target_col`` could be:

            .. code-block:: python

                [
                    (['max','avg'],['N_TXN_AMT_RMB']),
                    (['max'],['TARGET'])
                ]

            which indicates that we intend to perform max and average aggregate on ``N_TXN_AMT_RMB`` and only max on ``TARGET``.

            Allowed function names can be found in the API documentation of :py:mod:`autofe.legacy.py_functions`. For 
            example, suppose that one user seeks to utilize :py:class:`autofe.legacy.py_functions.MaxFunction`. Then, the user should 
            refer to the table in :py:mod:`autofe.legacy.py_functions`, which indicates that the function name is `max`.

            It is also flexible to specify the alias (i.e., column name) of a computed feature as follows:

            .. code-block:: python

                [
                    (['avg'],[('N_TXN_AMT_RMB', 'avg_amt')]),
                    (['max'],[('TARGET', 'max_target')])
                ]

            Note that customized alias only supports **one** aggregate function to **many** target columns. 
            In other words, it does not support many-to-many configuration.

            In addition, extra parameters for one function can be specified through the following syntax:

            .. code-block:: python

                [
                    ([('rto',{'prior':0.5})],['N_TXN_AMT_RMB']),
                ]

            That is, we extend the function in question to a ``tuple`` whose first element refers to the function and second element 
            is a ``dict`` specifying extra parameters (other than ``left`` and ``right``) to pass to the function.
        :param str order_col: name of column for ordering
        :param int partition_num: number of partitions, or number of parallelism
        :param new_cols: names of new columns
        :type new_cols: list of str
        :param repartition_col: ``None`` or list of str; if it is not ``None``, it is used as ``partition_col``. **Only for internal use.**

        :return: DataFrame
        """

        # update schema
        schema = copy.deepcopy(t.schema)
        for col in new_cols:
            schema.add(StructField(col,DoubleType(),True)) # double for gbdt
        fields = [x.name for x in schema.fields]
        if partition_col is None:
            res = t.repartition(partition_num)
        else:
            repartition_col = repartition_col if repartition_col is not None else [partition_col]
            res = t.repartition(partition_num, *repartition_col)
        order_cols = [col for col in [partition_col, order_col] if col is not None]
        if order_cols:
            res = res.sortWithinPartitions(order_cols)
        
        t = res.rdd\
                .mapPartitions(
                    WindowAggregator._window_aggregate_per_partition(windows, partition_col, target_col, order_col, 
                        fields, self.functions, self.NO_DISCARD_TRUNCATE_THRESHOLD, self._filter_history)
                    )\
                .toDF(schema)

        return t

    def process_hot_spot(self, t, windows, partition_col, order_col, num_subsets, hot_spot_vals):
        """
        Function to process hot spot without loss of information.

        **However, it is not recommended to use this function currently due to efficiency concern.**
        The optimization of this function is on development.
        """

        def _process_hot_spot_single_val(t, num_subsets, max_range, max_row):
            """
            :param max_range: `max_range` and `max_row` must not be None at the same time
            :param max_row: `max_range` and `max_row` must not be None at the same time
            """

            # compute number of rows and subset size
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
        if self.sql_context is None:
            t = t.withColumn(
                    row_number_col, 
                    F.row_number().over(W.Window.partitionBy(partition_col).orderBy(order_col)))
        else:
            self.sql_context.registerDataFrameAsTable(t, 't')
            t = self.sql_context.sql('select *, row_number() over (partition by {} order by {}) as {} from t'.format(partition_col, order_col, row_number_col))
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

    def aggregate(self, t, windows=None, partition_cols=None, target_col=None, order_col=None, condition=None,
                         partition_num=200, enable_support_skew=True, hot_spot_threshold=10000, num_subsets=1):
        """
        This function performs window-aware aggregate to compute time-series features. 
        Aggregate functions specified by ``target_col`` are applied to the target columns also specified by ``target_col``,
        within windows specified by ``windows`` partitioned by **each** column given by ``partition_cols``, and ordered by ``order_col``.

        In case of skewed data or *hot spot*, it performs optimization to improve computation efficiency.
        
        :param DataFrame t: input table    
        :param windows: list of ``dict``, each of which contains keys being ``name``, ``range``, and ``row``.
            The value of ``name`` indicates the window name.
            The value corresponding to ``range`` and ``row`` is supposed to be a binary ``tuple``, 
            which specifies temporal interval similar to ``range between`` and ``rows between`` in ``SQL``.

            The first element of both ``tuple`` is either non-negative or ``None``,
            whereas the second one is non-negative for ``range``, and ``None`` or non-negative for ``row``. 
            When both elements in the ``tuple`` are non-negative, the first element should be greater than the second one.
            When the first element in the ``tuple`` corresponding to ``range`` is ``None``, the range is unconstrained for left side. The same interpretation
            works for ``row``.

            The final window is the intersection of `range` and `row`, which extends the functionality provided in ``SQL`` in that
            we are able to specify both range and rows.

            One constraint applied by this function is that the time window must be backward. In other words, this function does not allow
            to utilize information in future.

            An example of ``windows`` is as follows:

            .. code-block:: python

                [{'name':'1min','range':(60,0),'row':(10,None)}],

            which indicates a window named ``1min`` with left boundary being the minimum of one minute and 10 rows, and right boundary being current row.

        :param partition_cols: list of str, or list of list, or ``None``. When it is ``None``, the aggregate would be performed without
            grouping. Otherwise, it contains column name(s) for partitioning by.
        :param target_col: list of binary tuples, each of which contains list of function name(s) and list of target column(s), for example, 
            the ``target_col`` could be:

            .. code-block:: python

                [
                    (['max','avg'],['N_TXN_AMT_RMB']),
                    (['max'],['TARGET'])
                ]

            which indicates that we intend to perform max and average aggregate on ``N_TXN_AMT_RMB`` and only max on ``TARGET``.

            Allowed function names can be found in the API documentation of :py:mod:`autofe.legacy.py_functions`. For 
            example, suppose that one user seeks to utilize :py:class:`autofe.legacy.py_functions.MaxFunction`. Then, the user should 
            refer to the table in :py:mod:`autofe.legacy.py_functions`, which indicates that the function name is `max`.

            It is also flexible to specify the alias (i.e., column name) of a computed feature as follows:

            .. code-block:: python

                [
                    (['avg'],[('N_TXN_AMT_RMB', 'avg_amt')]),
                    (['max'],[('TARGET', 'max_target')])
                ]

            Note that customized alias only supports **one** aggregate function to **many** target columns. 
            In other words, it does not support many-to-many configuration.

            In addition, extra parameters for one function can be specified through the following syntax:

            .. code-block:: python

                [
                    ([('rto',{'prior':0.5})],['N_TXN_AMT_RMB']),
                ]

            That is, we extend the function in question to a ``tuple`` whose first element refers to the function and second element 
            is a ``dict`` specifying extra parameters (other than ``left`` and ``right``) to pass to the function.
        :param str order_col: name of column for ordering
        :param condition: a function that returns bool based on one row of ``t``. For example, given ``t`` containing a column
            named ``trx_amt``, the following ``condition`` function returns ``True`` for any row where the transaction amount is 
            no greater than 100, and ``False`` otherwise:

            .. code-block:: python

                def condition(row):
                    return row['trx_amt'] <= 100

        :param int partition_num: number of partitions, or number of parallelism
        :param bool enable_support_skew: enable to support hot-spot optimization if ``True``; otherwise ``False``
        :param int hot_spot_threshold: the threshold imposed on the number of a specific value, above which the value is considered as hot spot
        :param int num_subsets: number of subsets into which skewed data is split. 
            If ``num_subsets`` is greater than 1, process hot spot without loss of information.
            otherwise, hot spot features are filled with ``None``
            **However, it is not recommended to set** ``num_subsets`` **greater than 1 currently due to efficiency concern.**
        :param HiveContext sql_context: context object of Spark

        :return: DataFrame
        """

        for arg in ['order_col', 'target_col','windows']:
            assert locals()[arg] is not None, '{} should not be None'.format(arg)

        partition_cols = [None] if partition_cols is None else list(partition_cols)

        utils.assert_target_col(target_col, self.functions)
        utils.normalize_target_col(target_col)

        self._filter_history = self._make_filter(condition)

        # normalize partition_col
        tmp_partition_cols = []
        if partition_cols:
            for i in range(len(partition_cols)):
                if isinstance(partition_cols[i], list):
                    old_col = partition_cols[i]
                    partition_cols[i] = utils.general_make_alias(*old_col)
                    if partition_cols[i] not in t.columns:
                        t = t.withColumn(partition_cols[i], F.concat(*old_col))
                        tmp_partition_cols.append(partition_cols[i])

        row_number_col = 'row_n'
        subset_id_col = 'subset_id'
        
        for partition_col in partition_cols:
            new_cols = []
            for window in windows:
                for func_keys, cols in target_col:
                    for col,alias in cols:
                        for func_key in func_keys:
                            func_key = func_key[0] if isinstance(func_key, tuple) else func_key
                            if func_key in self.functions:
                                new_cols.append(
                                    utils.make_alias(col, self.functions[func_key].name(), window['name'], partition_col, order_col, alias=alias)
                                    )
            
            if not enable_support_skew:
                t = self.map_partitions(t, windows=windows, partition_col=partition_col, target_col=target_col, 
                    order_col=order_col, partition_num=partition_num, new_cols=new_cols)
            else:
                # tackle hot spot
                groupby_aggregator = groupby_aggregate.GroupbyAggregator(self.sql_context)
                hot_spot_vals = utils.get_hot_spot_values(t, partition_col, hot_spot_threshold, groupby_aggregator=groupby_aggregator)
                t_non_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals) == False)
                t_non_hot_spot_res = self.map_partitions(t_non_hot_spot, windows=windows, partition_col=partition_col, 
                    target_col=target_col, order_col=order_col, partition_num=partition_num, new_cols=new_cols)
                if hot_spot_vals:
                    t_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals))
                    t_hot_spot_overlap = None
                    # either fillna or advanced processing
                    # if num_subsets > 1:
                    #     t_hot_spot_overlap = self.process_hot_spot(t_hot_spot, windows, partition_col, order_col, num_subsets, hot_spot_vals)
                    
                    if t_hot_spot_overlap is None:
                        t_hot_spot_res = utils.process_hot_spot_fillna(t_hot_spot, new_cols, None)
                    else:
                        t_hot_spot_res = self.map_partitions(
                            t_hot_spot_overlap, 
                            windows=windows, 
                            partition_col=partition_col, 
                            target_col=target_col, 
                            order_col=order_col, 
                            partition_num=partition_num, 
                            new_cols=new_cols, 
                            repartition_col=[partition_col, subset_id_col])
                        t_hot_spot_res = t_hot_spot_res.where(F.col(row_number_col) != -1).drop(subset_id_col).drop(row_number_col)
                    
                
                    t = t_hot_spot_res.unionAll(t_non_hot_spot_res)
                else:
                    t = t_non_hot_spot_res

        for col in tmp_partition_cols:
            t = t.drop(col)

        return t
        