# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division
import copy
import functools
import inspect
from math import log,ceil,floor

from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,ArrayType

from .. import utils
from ..aggregate import groupby_aggregate
from ..functions import spark_functions
from ..CONSTANTS import NEGATIVE_INFINITY,POSITIVE_INFINITY

class PartitionbyAggregator(object):
    """
    This class provides the functionality of window-aware aggregate similar to ``partition by`` offered in ``SQL``.

    .. todo::
        - Support order by desc

    """

    def __init__(self, sql_context):
        """
        The constructor of :py:class:`PartitionbyAggregator`.

        :param sql_context: should be ``pyspark.sql.HiveContext`` object
        """
        
        self.sql_context = sql_context

        self.__functions = {}
        for name,member in inspect.getmembers(spark_functions, inspect.isclass):
            mro = inspect.getmro(member)
            if mro[1] in [spark_functions.SparkAggregateFunction, spark_functions.SqlAggregateFunction]:
                self.__functions[member.name()] = member()

    @property
    def functions(self):
        """
        Return supported functions. Note that due to the limitation of ``PySpark``, 
        :py:class:`autofe.functions.spark_functions.CountDistinctFunction` is not supported.
        Try :py:mod:`autofe.window.window_aggregate` and use :py:func:`autofe.functions.py_functions.countdistinct` instead.

        :return: a ``dict`` mapping function name to function object
        """

        return self.__functions

    def register_sql_func(self, update):
        """
        Register new sql functions if there is no name conflict. Otherwise, ``AssertionError`` would be raised.
        
        :param update: user-defined functions to register, which must follow the paradigm of :py:class:`autofe.functions.spark_functions.SqlAggregateFunction`
            For example, users can register a SQL function that counts the number of times that values of a column is smaller than 300:

            .. code-block:: python

                class MyFunction(SqlAggregateFunction):
                    @classmethod
                    def name(self):
                        return 'cnt_le_300'

                    def callable(self):
                        return 'count(if({target_col} <= 300, 1, null))'

            Note that ``target_col`` is a placeholder that will be replaced with the actual column name in question.

        :type update: sequence of ``SqlAggregateFunction``

        """

        func_dict = {func.name():func for func in update}

        utils.update_dict(self.functions, func_dict)


    def aggregate(self, t, windows=None, partition_cols=None, target_col=None, order_col=None, enable_support_skew=False, hot_spot_threshold=10000):
        """
        Perform window-aware grouped aggregate similar to ``partition by`` in ``SQL``. 

        Aggregate functions specified by ``target_col`` are applied to the target columns also specified by ``target_col``,
        with rows partitioned by ``partition_cols``. 

        Note that if ``partition_cols`` is given as a ``list``, it should be interpreted as performing
        aggregate respectively partitioned by each element in ``partition_cols``. For example, given ``partition_cols = ['id', 'mac']``, two aggregates
        would be performed: one on data grouped by ``id``, the other on data grouped by ``mac``. ``partition_cols`` can also contain elements that they themself
        are lists, such as ``partition_cols = ['id', ['mac', 'date']]``. In this case, one aggregate would be conducted on data grouped by ``id`` whereas
        the other on data grouped by both ``mac`` and ``date``.

        :param DataFrame t: input table
        :param windows: list of ``dict`` that has keys including ``name``, ``type``, and ``val``. The value of ``name`` indicates the window name. 
            The value of ``type`` is either ``range`` or ``row``, which specifies the unit of window, 
            similar to ``range between`` and ``rows between`` in ``SQL``. The value of ``val``, which is a ``tuple``, gives the left and right
            boundary of the window. When the value of a boundary is ``None``, it means unbounded.
            For example, the ``windows`` could be

            .. code-block:: python

                [{'name':'1h','type':'range','val':[-3600,0]}]

            which indicates that this is a window named `1h` with left boundary being 3600 seconds and right boundary being current row.


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

            Allowed function names can be found in the API documentation of :py:mod:`autofe.functions.spark_functions`. For 
            example, suppose that one user seeks to utilize :py:class:`autofe.functions.spark_functions.MaxFunction`. Then, the user should 
            refer to the table in :py:mod:`autofe.functions.spark_functions`, which indicates that the function name is `max`.

            It is also flexible to specify the alias (i.e., column name) of a computed feature as follows:

            .. code-block:: python

                [
                    (['avg'],[('N_TXN_AMT_RMB', 'avg_amt')]),
                    (['max'],[('TARGET', 'max_target')])
                ]

            Note that customized alias only supports **one** aggregate function to **many** target columns. 
            In other words, it does not support many-to-many configuration.
        :param order_col: list of str or ``None``. The name of column(s) used for ordering. 
            When more than two columns are provided, ordering is performed based on multiple columns as a whole. 
            However, multiple order columns are only valid when the type of ``windows`` is set to ``row``. 
            If ``None``, the aggregate would be performed without ordering data
        :param bool enable_support_skew: enable to support hot-spot optimization if ``True``; otherwise ``False``
        :param int hot_spot_threshold: the threshold imposed on the number of a specific value, above which the value is considered as hot spot

        :return: DataFrame
        """

        def _normalize_windows(windows):
            """
            :param windows: windows within which the aggregate is performed
            :type windows: sequence of dict
            """
            for window in windows:
               for side in range(2):
                    if window['val'][side] is None:
                        window['val'][side] = POSITIVE_INFINITY if side == 1 else NEGATIVE_INFINITY

        def _generate_bound(value):
            res = None
            if value == 0:
                res = 'current row'
            elif value == NEGATIVE_INFINITY:
                res = 'unbounded preceding'
            elif value == 'POSITIVE_INFINITY':
                res = 'unbounded following'
            elif value > 0:
                res = '{} following'.format(str(value))
            else:
                res = '{} preceding'.format(str(-value))

            return res

        def _add_spark_expr(exprs, func, target_col, window_spec, alias):
            exprs.append(func(F.col(target_col)).over(window_spec).alias(alias))

        def _add_sql_expr(exprs, func, target_col, window_spec, alias):
            """
            :param str window_spec: 
            """

            expr = func(target_col=target_col) + ' over ({window_spec}) as {alias}'
            exprs.append(expr.format(window_spec=window_spec, alias=alias))

        def _add_expr(exprs, func, target_col, window_spec, wname, partition_col, order_col, alias=None):
            order_col = [None] if order_col is None else order_col
            alias = alias if alias is not None else utils.general_make_alias(target_col, func.name(), wname, 'by', partition_col, *order_col)

            # track new columns associated to each partition_col
            if partition_col in exprs['new_cols']:
                exprs['new_cols'][partition_col].append(alias)
            else:
                exprs['new_cols'][partition_col] = [alias]
                exprs['spark'][partition_col] = []
                exprs['sql'][partition_col] = []

            if func.is_spark():
                _add_spark_expr(exprs['spark'][partition_col], func, target_col, window_spec['spark'], alias)
            else:
                _add_sql_expr(exprs['sql'][partition_col], func, target_col, window_spec['sql'], alias)


        for arg in ['target_col','windows']:
            assert locals()[arg] is not None, '{} should not be None'.format(arg)

        utils.assert_target_col(target_col, self.functions)
        utils.normalize_target_col(target_col)
        _normalize_windows(windows)
        partition_cols = partition_cols if partition_cols is None else list(partition_cols)

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
        
        exprs = {'spark':{}, 'sql':{}, 'new_cols':{}}
        partition_cols = [None] if partition_cols is None else list(partition_cols)
        for window in windows:
            for partition_col in partition_cols:
                window_spec = {'spark':None, 'sql':''}
                _partition_col = None
                if partition_col is None:
                    window_spec['spark'] = Window.partitionBy()
                    window_spec['sql'] = 'partition by 1'
                else:
                    window_spec['spark'] = Window.partitionBy(partition_col)
                    if type(partition_col) == list:
                        _partition_col = ','.join(partition_col)
                    else:
                        _partition_col = partition_col
                    window_spec['sql'] = 'partition by {}'.format(_partition_col)

                is_unbounded = True
                if window['val'][0] != NEGATIVE_INFINITY or window['val'][1] != POSITIVE_INFINITY:
                    is_unbounded = False

                if not is_unbounded and order_col is not None:
                    window_spec['spark'] = window_spec['spark'].orderBy(order_col)
                    window_spec['sql'] = window_spec['sql'] + ' order by {}'.format(','.join(order_col))

                    if window['type'] == 'range':
                        window_spec['spark'] = window_spec['spark'].rangeBetween(*window['val'])
                        window_spec['sql'] = window_spec['sql'] + ' range between {left} and {right}'.format(
                            left=_generate_bound(window['val'][0]),
                            right=_generate_bound(window['val'][1])
                            )
                    else:
                        window_spec['spark'] = window_spec['spark'].rowsBetween(*window['val'])
                        window_spec['sql'] = window_spec['sql'] + ' rows between {left} and {right}'.format(
                            left=_generate_bound(window['val'][0]),
                            right=_generate_bound(window['val'][1])
                            )

                for func_keys, cols in target_col:
                    for col,alias in cols:
                        for func_key in func_keys:
                            if func_key in self.functions:
                                _add_expr(exprs, self.functions[func_key], col, window_spec, window['name'], partition_col, order_col, alias=alias)
        
        def execute_exprs(t, spark_exprs=None, sql_exprs=None):
            """
            Execute expressions
            """
            
            if spark_exprs is not None:
                spark_exprs = t.columns + spark_exprs
                t = self.sql_context.createDataFrame(t.rdd, t.schema)
                t = t.select(*spark_exprs)
            if sql_exprs is not None:
                sql_exprs = t.columns + sql_exprs
                self.sql_context.registerDataFrameAsTable(t, 't')
                sql_clause = 'select ' + ','.join(sql_exprs) + ' from t'
                t = self.sql_context.sql(sql_clause)
            
            return t

        if not enable_support_skew:
            spark_exprs = []
            sql_exprs = []
            for spark_exprs_par in exprs['spark'].values():
                spark_exprs.extend(spark_exprs_par)
            for sql_exprs_par in exprs['sql'].values():
                sql_exprs.extend(sql_exprs_par)
            t = execute_exprs(t, spark_exprs, sql_exprs)
        else:
            groupby_aggregator = groupby_aggregate.GroupbyAggregator(self.sql_context)
            for partition_col in partition_cols:
                hot_spot_vals = utils.get_hot_spot_values(t, partition_col, hot_spot_threshold, groupby_aggregator=groupby_aggregator)
                t_non_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals) == False)
                t_non_hot_spot_res = execute_exprs(t_non_hot_spot, exprs['spark'][partition_col], exprs['sql'][partition_col])
                if hot_spot_vals:
                    t_hot_spot = t.where(F.col(partition_col).isin(hot_spot_vals))
                    t_hot_spot_res = utils.process_hot_spot_fillna(t_hot_spot, exprs['new_cols'][partition_col], None) 
                    t = t_hot_spot_res.unionAll(t_non_hot_spot_res)
                else:
                    t = t_non_hot_spot_res

        for col in tmp_partition_cols:
            t = t.drop(col)

        return t 