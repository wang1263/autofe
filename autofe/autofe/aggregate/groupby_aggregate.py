# coding: UTF-8

"""
.. todo:: 2019-01-28 14:25:29 WARN  Executor:66 - Managed memory leak detected; size = 262144 bytes, TID = 1241
"""
import copy
import inspect

from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import StringType,DoubleType,LongType,IntegerType
from pyspark.sql import functions as F

from .. import utils
from ..functions import spark_functions
from ..CONSTANTS import DELIMITER

# Global parameters
NUM_PARTITIONS = 200

class GroupbyAggregator(object):
    """
    This class provides functionality of aggregate through grouping similar to ``group by`` in ``SQL``
    """
    def __init__(self, sql_context):
        """
        The constructor of :py:class:`GroupbyAggregator`.

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
        Return supported functions.

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

            Note that ``target_col`` is a placeholder that will be automatically replaced with the actual column name in question.

        :type update: sequence of ``SqlAggregateFunction``

        """

        func_dict = {func.name():func for func in update}

        utils.update_dict(self.functions, func_dict)
        
    def _make_alias(self, fname, col, wname, suffix):
        """
        Make alias for computed column, which results in ``fname_col_wname_by_suffix``. 
        If any of these elements is empty or ``None``, this empty element would be ignored in alias.

        :param str fname: name of the function
        :param str col: name of the column
        :param str wname: window name
        :param str suffix: suffix added to the alias

        :return: alias of the computed column
        :rtype: str
        """

        alias = DELIMITER.join(
                [item for item in [fname, col, wname, 'by', suffix] if len(item) > 0]
            )

        return alias

    def _aggregate(self, t, groupby_col=None, target_col=None, wname=None, suffix=None):
        """
        Perform grouped aggregate similar to ``group by`` in ``SQL``.

        :param DataFrame t: input table
        :param groupby_col: str or list of str, column name(s) for grouping by
        :param target_col: list of binary tuples, each of which contains list of function name(s) and list of target column(s)
        :param str wname: window name
        :param str suffix: descriptive suffix, by default determined by 'groupby_col'

        :return: DataFrame
        """

        def _add_spark_expr(exprs, func, target_col, alias):
            exprs.append(func(target_col).alias(alias))

        def _add_sql_expr(exprs, func, target_col, alias):
            expr = func(target_col=target_col) + ' as {alias}'
            exprs.append(expr.format(alias=alias))

        def _add_expr(exprs, func, target_col, wname, suffix, alias=None):
            alias = alias if alias is not None else self._make_alias(func.name(), target_col, wname, suffix)
            if func.is_spark():
                _add_spark_expr(exprs['spark'], func, target_col, alias)
            else:
                _add_sql_expr(exprs['sql'], func, target_col, alias)

        functions = self.functions
        exprs = {'spark':[], 'sql':[]}
        for func_keys, cols in target_col:
            for col, alias in cols:
                for func_key in func_keys:
                    if func_key in functions:
                        _add_expr(exprs, functions[func_key], col, wname, suffix, alias=alias)
                    
        res_sql = None
        res_spark = None
        res = None
        if exprs['sql']:
            sql_clause = 'select {0}, ' + ','.join(exprs['sql']) + ' from t group by {0}'
            sql_clause = sql_clause.format(','.join(groupby_col))
            res_sql = self.sql_context.sql(sql_clause)
        if exprs['spark']:
            res_spark = t.groupBy(groupby_col).agg(*exprs['spark'])

        if res_sql is not None and res_spark is not None:
            res = res_spark.join(res_sql, groupby_col, 'inner')
        else:
            res = res_sql if res_sql is not None else res_spark

        return res

    def aggregate(self, t, groupby_col=None, target_col=None, wname=None, suffix=None, random_key=None, is_skewed=False, num_partitions_skew=10):
        """
        Perform grouped aggregate similar to ``group by`` in ``SQL``. 

        Aggregate functions specified by ``target_col`` are applied to the target columns also specified by ``target_col``,
        with rows grouped by ``groupby_col``. 

        Note that if ``groupby_col`` is given as a ``list``, it should be interpreted as performing
        aggregate grouped by ``groupby_col`` **as a whole** instead of separately. For example, 
        given ``groupby_cols = ['mac', 'id']``, the aggregate would be conducted on data grouped by both ``mac`` and ``id``.

        :param DataFrame t: input table
        :param groupby_col: str or list of str, column name(s) for grouping by
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
        :param str wname: **deprecated**, do not use it
        :param str suffix: descriptive suffix, by default determined by 'groupby_col'
        :param str random_key: name of column that could serve as random key for initial grouping by in case of skewed data; 
            it is generated if ``None``
        :param bool is_skewed: set to ``True`` if data is skewed; otherwise set to ``False``
        :param int num_partitions_skew: number of partitions for initial grouping by; it has no effect when ``random_key`` is given

        :return: DataFrame
        """

        def _assert_target_col(target_col, functions):
            """
            :param dict target_col: map from function list to columns
            :param list functions: candidate functions
            """
            for func_keys, cols in target_col:
                for func_key in func_keys:
                    if func_key not in functions:
                        raise ValueError('{} is not supported!'.format(func_key))
                    elif 'num' == func_key and len(func_keys) > 1:
                        raise ValueError('Duplicated numerical functions!')

        def _normalize_target_col(target_col):
            """
            Normalize `target_col` so that each target column is assigned an alias
            """
            for func_keys, cols in target_col:
                for i in range(len(cols)):
                    if isinstance(cols[i], str):
                        cols[i] = (cols[i], None)

        for arg in ['groupby_col','target_col']:
            assert locals()[arg] is not None, '{} should not be None'.format(arg)

        utils.assert_target_col(target_col, self.functions)
        utils.normalize_target_col(target_col)

        if suffix is None:
            if type(groupby_col) == list:
                suffix = DELIMITER.join(groupby_col)
            else:
                suffix = groupby_col

        if wname is None:
            wname = ''

        self.sql_context.registerDataFrameAsTable(t, 't')

        if not is_skewed:
            return self._aggregate(t, groupby_col, target_col, wname, suffix)
        else:
            res_nonsupport = None
            res_support = None

            target_col_support = []
            target_col_nonsupport = []

            for func_keys, cols in target_col:
                for func_key in func_keys:
                    if func_key in self.functions:
                        if self.functions[func_key].support_skewed():
                            target_col_support.append(([func_key], cols))
                        else:
                            target_col_nonsupport.append(([func_key], cols))

            res_nonsupport = self._aggregate(t, groupby_col, target_col_nonsupport, wname, suffix)

            if random_key is None:
                random_key = 'tmp_idx'
                t = t.withColumn(random_key, F.floor(F.rand()*num_partitions_skew))

            initialize_exprs = list(t.columns)
            init_target_col = []
            last_target_col = []
            final_exprs = []
            final_exprs.extend(groupby_col)
            for func_keys, cols in target_col_support:
                for col, alias in cols:
                    for func_key in func_keys:
                        if func_key in self.functions:
                            initialize_exprs.extend(self.functions[func_key].initialize(col))
                            init_target_col.extend(self.functions[func_key].init_agg(col))
                            last_target_col.extend(self.functions[func_key].last_agg(col))
                            expr = self.functions[func_key].finalize(col)
                            alias = alias if alias is not None else self._make_alias(func_key, col, wname, suffix)
                            final_exprs.append(expr.alias(alias))
            
            res_support = t.select(*initialize_exprs)
            res_support = self._aggregate(res_support, groupby_col+[random_key], target_col=init_target_col)
            res_support = self._aggregate(res_support, groupby_col, target_col=last_target_col)
            res_support = res_support.select(*final_exprs)

            if res_nonsupport is not None and res_support is not None:
                res = res_nonsupport.join(res_support, groupby_col, 'inner')
            else:
                res = res_nonsupport if res_nonsupport is not None else res_support

        return res
            