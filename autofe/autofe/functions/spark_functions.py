# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

"""
This module contains aggregate functions implemented in PySpark/SQL with support for tackling skewed data.

Each aggregate function has an associated pre-defined function name, returned by 
the function's ``name()`` method. When applying an aggregate function in this module, 
you should refer to the function's name. The mapping is as follows:

==============================	======
Function    					Name
==============================	======
MinFunction						min
MaxFunction						max
SumFunction						sum
AvgFunction						avg
StddevPopFunction				stddev
CountFunction					cnt
CountDistinctFunction			dcnt
ApproxCountDistinctFunction		adcnt
MedianFunction					med
==============================	======
"""

import six
import abc
from pyspark.sql import functions as F

from ..utils import gen_random_str
from ..CONSTANTS import DELIMITER

@six.add_metaclass(abc.ABCMeta)
class AggregateFunction(object):
	"""
	Base abstract class for aggregate functions.
	"""

	@classmethod
	@abc.abstractmethod
	def name(cls):
		"""
		Return the name reference of the function.

		:return: name of the function
		:rtype: str
		"""
		raise NotImplementedError()

	@abc.abstractmethod
	def is_spark(self):
		"""
		Return whether this function is Spark built-in function or not.

		:return: ``True`` if it's Spark built-in function; otherwise SQL function
		:rtype: bool
		"""
		raise NotImplementedError()

	@abc.abstractmethod
	def callable(self):
		"""
		The callable method that the class wraps:

		- For :py:class:`SparkAggregateFunction`, the callable is a built-in function in PySpark, such as ``F.min``
		- For :py:class:`SqlAggregateFunction`, the callable is a ``str`` 
		  which is a SQL clause following the format of ``'func({target_col}, arg0, arg1, ...)'``, 
		  such as ``'sum({target_col})'``, or ``'percentile_approx({target_col}, 0.5)'``.
		  Note that ``target_col`` is a placeholder that will be replaced with the actual column name in question.

		"""

		raise NotImplementedError()
	
	@abc.abstractmethod
	def __call__(self, *args, **kwargs):

		raise NotImplementedError()

	def support_skewed(self):
		"""
		Return whether the function supports skewed data.

		:return: Return ``True`` if the function supports skewed data optimization; otherwise return ``False``
		:rtype: bool
		"""
		if all([item in self.__class__.__dict__ for item in ['init_agg','last_agg','finalize']]):
			return True
		else:
			return False

class SparkAggregateFunction(AggregateFunction):
	"""
	Base class for Spark aggregate functions.
	"""
	
	def is_spark(self):
		return True

	@classmethod
	def prefix(cls):
		return DELIMITER.join([cls.__name__, 'sparkle'])

	def _make_alias(self, agg_func, code, col):
		"""
		Make alias for computed column.
		"""
		return DELIMITER.join([agg_func.prefix(), code, self.name(), col])

	def __call__(self, *args, **kwargs):
		return self.callable()(*args, **kwargs)

	def initialize(self, col):
		"""
		Conduct precomputation for required columns ``col`` to tackle skewed data.

		:param str col: column name
		:return: list of expressions to feed into ``DataFrame.select()``
		:rtype: list
		"""
		return []

	def init_agg(self, col):
		"""
		Carry out the initial aggregate to tackle skewed data.

		:param str col: column name
		:return: list of expressions following the same format as ``target_col`` accepted by :py:meth:`aggregate`
		"""
		raise NotImplementedError()

	def last_agg(self, col):
		"""
		Carry out the last aggregate to tackle skewed data.

		:param str col: column name
		:return: list of expressions following the same format as ``target_col`` accepted by :py:meth:`aggregate`
		"""
		raise NotImplementedError()

	def finalize(self, col):
		"""
		Carry out the final computation to tackle skewed data.

		:param str col: column name
		:return: list of expressions to feed into ``DataFrame.select()``
		"""
		raise NotImplementedError()
		
class SqlAggregateFunction(AggregateFunction):
	"""
	Base class for SQL aggregate functions.
	"""

	def is_spark(self):
		return False
	
	def __call__(self, *args, **kwargs):
		return self.callable().format(*args, **kwargs)
	
class MinFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the minimum of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'min'
	
	def callable(self):
		return F.min

	def init_agg(self, col):
		return [([self.name()],
				[(col, self._make_alias(self, 'init', col))])]
	
	def last_agg(self, col):
		return [([self.name()],
				[(self._make_alias(self, 'init', col), 
					self._make_alias(self, 'last', col))])]	

	def finalize(self, col):
		return F.col(self._make_alias(self, 'last', col))

class MaxFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the maximum of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'max'

	def callable(self):
		return F.max

	def init_agg(self, col):
		return [([self.name()],
				[(col, self._make_alias(self, 'init', col))])]
	
	def last_agg(self, col):
		return [([self.name()],
				[(self._make_alias(self, 'init', col), 
					self._make_alias(self, 'last', col))])]	

	def finalize(self, col):
		return F.col(self._make_alias(self, 'last', col))

class SumFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the sum of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'sum'

	def callable(self):
		return F.sum

	def init_agg(self, col):
		return [([self.name()],
				[(col, self._make_alias(self, 'init', col))])]
	
	def last_agg(self, col):
		return [([self.name()],
				[(self._make_alias(self, 'init', col), 
					self._make_alias(self, 'last', col))])]	

	def finalize(self, col):
		return F.col(self._make_alias(self, 'last', col))

class AvgFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the average of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'avg'

	def callable(self):
		return F.avg

	def init_agg(self, col):
		return [
				(
				[SumFunction.name()],
				[(col, self._make_alias(SumFunction, 'init', col))],
				),
				(
				[CountFunction.name()],
				[(col, 
					self._make_alias(CountFunction, 'init', col))]
				)
				]

	def last_agg(self, col):
		return [
				(
				[SumFunction.name()],
				[(self._make_alias(SumFunction, 'init', col), self._make_alias(SumFunction, 'last_sum', col))],
				),
				(
				[SumFunction.name()],
				[(self._make_alias(CountFunction, 'init', col), self._make_alias(SumFunction, 'last_cnt', col))]
				)
				]

	def finalize(self, col):
		return F.lit(1.0)*F.col(self._make_alias(SumFunction, 'last_sum', col))/F.col(self._make_alias(SumFunction, 'last_cnt', col))

class StddevPopFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the standard deviation (population) of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'stddev'

	def callable(self):
		return F.stddev_pop

	def initialize(self, col):
		res = F.col(col)*F.col(col)
		return [res.alias(self._make_alias(self, 'initialize_sum2', col))]

	def init_agg(self, col):
		col2 = self._make_alias(self, 'initialize_sum2', col)
		return [
				(
				[SumFunction.name()],
				[(col, self._make_alias(SumFunction, 'init', col))],
				),
				(
				[CountFunction.name()],
				[(col, 
					self._make_alias(CountFunction, 'init', col))]
				),
				(
				[SumFunction.name()],
				[(col2, self._make_alias(SumFunction, 'init', col2))],
				),
				(
				[CountFunction.name()],
				[(col2, 
					self._make_alias(CountFunction, 'init', col2))]
				),
				]
		
		return res

	def last_agg(self, col):
		col2 = self._make_alias(self, 'initialize_sum2', col)
		return [
				(
				[SumFunction.name()],
				[(self._make_alias(SumFunction, 'init', col), self._make_alias(SumFunction, 'last_sum', col))],
				),
				(
				[SumFunction.name()],
				[(self._make_alias(CountFunction, 'init', col), self._make_alias(SumFunction, 'last_cnt', col))]
				),
				(
				[SumFunction.name()],
				[(self._make_alias(SumFunction, 'init', col2), self._make_alias(SumFunction, 'last_sum', col2))],
				),
				(
				[SumFunction.name()],
				[(self._make_alias(CountFunction, 'init', col2), self._make_alias(SumFunction, 'last_cnt', col2))]
				)
				]

	def finalize(self, col):
		col2 = self._make_alias(self, 'initialize_sum2', col)
		avg_final = F.lit(1.0)*F.col(self._make_alias(SumFunction, 'last_sum', col))/F.col(self._make_alias(SumFunction, 'last_cnt', col))
		avg2_final = F.lit(1.0)*F.col(self._make_alias(SumFunction, 'last_sum', col2))/F.col(self._make_alias(SumFunction, 'last_cnt', col2))

		return F.sqrt(avg2_final - avg_final*avg_final)

class CountFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the count of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'cnt'

	def callable(self):
		return F.count

	def init_agg(self, col):
		return [([self.name()],
				[(col, self._make_alias(self, 'init', col))])]
	
	def last_agg(self, col):
		return [([SumFunction.name()],
				[(self._make_alias(self, 'init', col), 
					self._make_alias(SumFunction, 'last', col))])]	

	def finalize(self, col):
		return F.col(self._make_alias(SumFunction, 'last', col))

class CountDistinctFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the distinct count of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'dcnt'

	def callable(self):
		return F.countDistinct

class ApproxCountDistinctFunction(SparkAggregateFunction):
	"""
	Aggregate function: returns the approximate distinct count of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'adcnt'

	def callable(self):
		return F.approxCountDistinct

class MedianFunction(SqlAggregateFunction):
	"""
	Aggregate function: returns the median of all values in the expression.
	"""
	@classmethod
	def name(cls):
		return 'med'

	def callable(self):
		return 'percentile_approx({target_col}, 0.5)'