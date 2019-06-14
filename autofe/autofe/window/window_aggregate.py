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
import itertools

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,ArrayType
from pyspark.sql import functions as F
from pyspark.sql import window as W

from .. import utils
from ..functions import sw_functions
from ..aggregate import groupby_aggregate

class WindowAggregator(object):
    """
    This class provides the functionality of window-aware aggregate similar to ``partition by`` offered in ``SQL``. 
    It optimizes time-series computation with upper bound of time complexity being O(N). 
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

        self.__functors = {}
        self.__colfun2alias = []
        for name,member in inspect.getmembers(sw_functions, inspect.isclass):
            mro = inspect.getmro(member)
            if mro[1] == sw_functions.SlidingWindowAggregateFunction:
                self.__functors[member.name()] = member

    @property
    def functors(self):
        """
        Return supported functors. 

        :return: a ``dict`` mapping function name to function class
        """

        return self.__functors

    def _make_filter(self, condition=None):
        def filter_history(row, val):
            if val is None:
                return False

            if condition is not None:
                return condition(row)
            else:
                return True
                
        return filter_history

    @staticmethod
    def _window_aggregate_per_partition(partition_col, colfun2alias, order_col, 
                                        fields, NO_DISCARD_TRUNCATE_THRESHOLD, filter_history):
        """
        This function returns a function computing time-series features from ``target_col`` by applying ``aggregate_functions`` 
        to a set of ``windows`` partitioned by ``partition_col``, ordered by ``order_col``.

        Note that this function is not intended to be used directly. Use :py:func:`window_aggregate` instead to perform window-aware aggregate.

        :param partition_col: str, or list of str, or ``None``. When it is ``None``, the aggregate would be performed without
            grouping. Otherwise, it contains column name(s) for partitioning by.
        :param dict colfun2alias: list of triple tuple of column, function, alias of new column
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

        target_col_set = list(set([col for _,colfun2alias_per_w in colfun2alias for col,_,_ in colfun2alias_per_w]))

        for window,_ in colfun2alias:
            _assert_window(window)

        def _initialize(target_col_set):
            """
            Initialize and return relevant variables

            :return:
                - cursor_indices: boundary of last window
            """
            acc_delta = 0

            keys = target_col_set + ['row', 'timeline']
            history = {col:[] for col in keys}

            cursor_indices = {}
            for window,_ in colfun2alias:
                cursor_indices[window['name']] = [0, 0]

            row_count = -1

            for _,colfun2alias_per_w in colfun2alias:
                for _,func,_ in colfun2alias_per_w:
                    func.reset()

            return history, cursor_indices, row_count, acc_delta

        def _check_within_window(cur_idx, cursor_idx, timeline, window, acc_delta=0):
            """
            Check whether the time corresponding to ``cursor_idx`` is within ``window`` of ``cur_idx``

            :return: return 1 if greater, -1 if smaller, 0 within
            """

            # check range
            smaller, within, greater = -1, 0, 1

            if timeline[cursor_idx - acc_delta] > (timeline[cur_idx - acc_delta] - window['range'][1]):
                return greater
            if window['range'][0] is not None and timeline[cursor_idx - acc_delta] < (timeline[cur_idx - acc_delta] - window['range'][0]):
                return smaller

            # check row
            if window['row'][1] is not None and cursor_idx > cur_idx - window['row'][1]:
                return greater
            if window['row'][0] is not None and cursor_idx < cur_idx - window['row'][0]:
                return smaller
            
            return within

        # truncate
        def _truncate(history, delta, acc_delta):
            """
            Truncate ``history`` by ``delta`` and adjust ``cursor_indices`` and ``row_count``
            """

            for k in history.keys():
                history[k] = utils.truncate(history[k], delta)

            acc_delta += delta

            return history, acc_delta

        def _window_aggregate_per_partition(iterator):
            """
            Window aggregate function
            """

            sentinel = None
            history, cursor_indices, row_count, acc_delta = _initialize(target_col_set)
            for row in iterator:
                item = row.asDict()
                if partition_col is not None and item[partition_col] != sentinel:
                    sentinel = item[partition_col]
                    history, cursor_indices, row_count, acc_delta = _initialize(target_col_set)

                # append new item
                row_count += 1
                history['timeline'].append(item[order_col])
                history['row'].append(item)
                for col in target_col_set:
                    history[col].append(item[col])

                for window, colfun2alias_per_w in colfun2alias:
                    # disjoint window
                    if _check_within_window(row_count, cursor_indices[window['name']][1], history['timeline'], window, acc_delta) == -1:
                        while cursor_indices[window['name']][1] <= row_count and _check_within_window(row_count, cursor_indices[window['name']][1], history['timeline'], window, acc_delta) == -1:
                            cursor_indices[window['name']][1] += 1
                        cursor_indices[window['name']][0] = cursor_indices[window['name']][1]
                        for col, func, alias in colfun2alias_per_w:
                            func.reset()

                    # now right cursor is either within window or right to the window
                    if _check_within_window(row_count, cursor_indices[window['name']][1], history['timeline'], window, acc_delta) == 0:
                        # slide right cursor
                        while cursor_indices[window['name']][1] <= row_count and _check_within_window(row_count, cursor_indices[window['name']][1], history['timeline'], window, acc_delta) == 0:
                            for col,func,alias in colfun2alias_per_w:
                                if filter_history(history['row'][cursor_indices[window['name']][1] - acc_delta], history[col][cursor_indices[window['name']][1]-acc_delta]):
                                    if func.name() not in ['min', 'max', 'last']:
                                        func.push(history[col][cursor_indices[window['name']][1]-acc_delta])
                                    else:
                                        func.push((history[col][cursor_indices[window['name']][1]-acc_delta], cursor_indices[window['name']][1]))
                                    
                            cursor_indices[window['name']][1] += 1
                        
                        # slide left cursor
                        while cursor_indices[window['name']][0] <= row_count and _check_within_window(row_count, cursor_indices[window['name']][0], history['timeline'], window, acc_delta) == -1:
                            for col,func,alias in colfun2alias_per_w:
                                if func.name() not in ['min', 'max', 'last'] and filter_history(history['row'][cursor_indices[window['name']][0]-acc_delta], history[col][cursor_indices[window['name']][0]-acc_delta]):
                                    func.pop(history[col][cursor_indices[window['name']][0]-acc_delta])
                            cursor_indices[window['name']][0] += 1

                    for col,func,alias in colfun2alias_per_w:
                        if func.name() not in ['min', 'max', 'last']:
                            item[alias] = func.get(query=item[col])
                        else:
                            item[alias] = func.get(query=lambda x: _check_within_window(row_count, x, history['timeline'], window, acc_delta)==0)

                smallest_cursor_index = min(cursor_indices.values(), key=lambda x: x[0])[0]
                if smallest_cursor_index - acc_delta >= NO_DISCARD_TRUNCATE_THRESHOLD:
                    history, acc_delta = _truncate(history, smallest_cursor_index - acc_delta, acc_delta)

                res = [item[alias] for alias in fields]

                yield res

        return _window_aggregate_per_partition

    def map_partitions(self, t, partition_col=None, target_col=None,
                        order_col=None, partition_num=200, repartition_col=None):
        """
        This function performs window-aware aggregate to compute time-series features. 
        Aggregate functions specified by ``target_col`` are applied to the target columns also specified by ``target_col``,
        within windows specified by ``windows`` partitioned by ``partition_col``, and ordered by ``order_col``.

        Note that this function is not intended to be used directly. Use :py:meth:`aggregate` instead to perform window-aware aggregate, 
        which supports **multiple** partitions and hot-spot optimization.

        :param DataFrame t: input table    
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

            Allowed function names can be found in the API documentation of :py:mod:`autofe.functions.sw_functions`. For 
            example, suppose that one user seeks to utilize :py:class:`autofe.functions.sw_functions.MaxFunction`. Then, the user should 
            refer to the table in :py:mod:`autofe.functions.sw_functions`, which indicates that the function name is `max`.

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
        :param colfun2alias: list of triple tuple of (column, function, alias)
        :type colfun2alias: list
        :param repartition_col: ``None`` or list of str; if it is not ``None``, it is used as ``partition_col``. **Only for internal use.**

        :return: DataFrame
        """

        # update schema
        schema = copy.deepcopy(t.schema)
        for _,colfun2alias_per_w in self.__colfun2alias:
            for _,_,col in colfun2alias_per_w:
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
                    WindowAggregator._window_aggregate_per_partition(partition_col, self.__colfun2alias, order_col, 
                        fields, self.NO_DISCARD_TRUNCATE_THRESHOLD, self._filter_history)
                    )\
                .toDF(schema)

        return t

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

            Allowed function names can be found in the API documentation of :py:mod:`autofe.functions.sw_functions`. For 
            example, suppose that one user seeks to utilize :py:class:`autofe.functions.sw_functions.MaxFunction`. Then, the user should 
            refer to the table in :py:mod:`autofe.functions.sw_functions`, which indicates that the function name is `max`.

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

        if enable_support_skew:
            groupby_aggregator = groupby_aggregate.GroupbyAggregator(self.sql_context)

        utils.assert_target_col(target_col, self.functors)
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
            self.__colfun2alias = []
            for window in windows:
                colfun2alias_per_w = []
                for func_keys, cols in target_col:
                    for col,alias in cols:
                        for func_key in func_keys:
                            if isinstance(func_key, tuple):
                                params = func_key[1]
                                func_key = func_key[0]
                            else:
                                params = {}
                            if func_key in self.functors:
                                colfun2alias_per_w.append(
                                    (col, 
                                    self.functors[func_key](**params),
                                    utils.make_alias(
                                    col, self.functors[func_key].name(), window['name'], partition_col, order_col, alias=alias
                                    ))
                                    )
                self.__colfun2alias.append((window, colfun2alias_per_w))

            if not enable_support_skew:
                t = self.map_partitions(t, partition_col=partition_col, target_col=target_col, 
                    order_col=order_col, partition_num=partition_num)
            else:
                # tackle hot spot
                hot_spot_vals = utils.get_hot_spot_values(t, partition_col, hot_spot_threshold, groupby_aggregator=groupby_aggregator)
                t_non_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals) == False)
                t_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals))
                t_non_hot_spot_res = self.map_partitions(t_non_hot_spot, partition_col=partition_col, 
                    target_col=target_col, order_col=order_col, partition_num=partition_num)
                if hot_spot_vals:
                    new_cols = [alias for _,colfun2alias_per_w in self.__colfun2alias for _,_,alias in colfun2alias_per_w]
                    t_hot_spot_res = utils.process_hot_spot_fillna(t_hot_spot, new_cols, None)
                    t = t_hot_spot_res.unionAll(t_non_hot_spot_res)
                else:
                    t = t_non_hot_spot_res

        for col in tmp_partition_cols:
            t = t.drop(col)

        return t
        