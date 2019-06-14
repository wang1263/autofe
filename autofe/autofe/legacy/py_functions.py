# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

"""
This module contains functions including aggregate functions, bucketize functions, as well as functions to fill NA.

Each aggregate function has an associated pre-defined function name, returned by 
the function's ``name()`` method. When applying an aggregate function in this module, 
you should refer to the function's name. The mapping is as follows:

==============================  ======
Function                        Name
==============================  ======
MinFunction                     min
MaxFunction                     max
SumFunction                     sum
PopVarianceFunction             popvar
SampVarianceFunction            smpvar
PopSDFunction                   popsd
SampSDFunction                  smpsd
AvgFunction                     avg
CountFunction                   cnt
ExistCountFunction              ecnt
CountDistinctFunction           dcnt
LastFunction                    last
ExistFunction                   exist
RatioFunction                   rto
EntropyFunction                 etrp
==============================  ======
"""

from __future__ import print_function,division
import six
import abc
from math import log,sqrt
import functools
from collections import deque

from .. import CONSTANTS

# helper function
def identity():
    """
    This function does nothing, just for purpose of testing
    """
    return lambda x:x

def _check_valid_boundary(left, right):
    """
    Function to check if left and right boundary is valid
    """
    if left is None or right is None or left>right:
        return False
    else:
        return True

# decorator to check boundary if valid
def check_boundary(val=CONSTANTS.NA):
    """
    This function can used as decorator and checks boundary whether it is valid.

    :param val: the returned value if the given boundary is not valid
    :return: decorator that checks boundary
    """
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
    This function can used as decorator and casts result to ``rtype`` if it is not ``None``.

    :param rtype: type of return
    :type rtype: built-in type of Python
    :return: decorator that casts result to ``rtype``
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
def transform(transformer=identity()):
    """
    This function can be used as decorator and transforms returned values based on ``transformer``.

    :param transformer: function that accepts a single value and conducts transformation, such as discretization:

        .. code-block:: python

            def best_discretize_ever(x):
                return round(-log(1.0002/(0.0001+x)-1), 2)

    :type transformer: function
    :return: decorator that transforms returned values
    """
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
            res = _transformer(res) if res != CONSTANTS.NA else res
            return res
        return wrapper
    return decorator

# functions to discretize
def discrete_0_INF(N = 2):
    """
    Return a function that discretizes a positive value so that it is rounded to ``N`` digits after the decimal point.
    The implementation is as follows:

    .. code-block:: python

        lambda x: round(log(x+1), N)

    :param int N: number of digits rounded after the decimal points
    :return: discretization function
    """

    return lambda x: round(log(x+1), N)

def discrete_0_1(N = 2):
    """
    Return a function that discretizes a value ranging between 0 and 1 so that it is rounded to ``N`` digits after the decimal point.
    The implementation is as follows:

    .. code-block:: python

        lambda x: round(-log(1.0002/(0.0001+x)-1), N)

    :param int N: number of digits rounded after the decimal points
    :return: discretization function
    """

    return lambda x: round(-log(1.0002/(0.0001+x)-1), N)

def discrete_NINF_INF(N = 2):
    """
    Return a function that discretizes any value so that it is rounded to ``N`` digits after the decimal point.
    The implementation is as follows:

    .. code-block:: python

        lambda x:round(log(x+1), N) if (x>=0) else -round(log(-x+1), N)

    :param int N: number of digits rounded after the decimal points
    :return: discretization function
    """
    
    return lambda x:round(log(x+1), N) if (x>=0) else -round(log(-x+1), N)

# functions to fill na
def fillna(x):
    """
    Function to fill NA defined by ``CONSTANTS.NULL`` and ``CONSTANTS.NA``.

    :param x: input value
    :return: the returned value depends on the value of ``x``:

        - if ``x`` equals ``None`` or empty string, returns ``CONSTANTS.NULL``;
        - if ``x`` is an empty list, returns ``CONSTANTS.NA``;
        - otherwise returns ``x``.

    """
    if x is None or x == '':
        return CONSTANTS.NULL
    elif x == []:
        return CONSTANTS.NA
    else:
        return x

#aggregate function
@cast_to()
@transform()
@check_boundary()
def fsum(a, query=None, left=None, right=None):
    """
    Compute sum of sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    return float(sum(a[left:right+1]))

@cast_to()
@transform()
@check_boundary()
def fpvar(a, query=None, left=None, right=None):
    """
    Compute population variance of sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    s = sum(a[left:right+1])
    s2 = sum(item*item for item in a[left:right+1])
    n = right+1-left
    avg = 1.0*s/n

    if n <= 0:
        return None
    res = 1.0*s2/n - avg*avg

    return res

@cast_to()
@transform()
@check_boundary()
def fsvar(a, query=None, left=None, right=None):
    """
    Compute sample variance of sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    s = sum(a[left:right+1])
    s2 = sum(item*item for item in a[left:right+1])
    n = right+1-left
    avg = 1.0*s/n

    if n <= 1:
        return None
    res = 1.0/(n-1)*(s2 - avg*avg*n)

    return res

@cast_to()
@transform()
@check_boundary()
def fpsd(a, query=None, left=None, right=None):
    """
    Compute population standard deviation of sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    s = sum(a[left:right+1])
    s2 = sum(item*item for item in a[left:right+1])
    n = right+1-left
    avg = 1.0*s/n

    if n <= 0:
        return None
    res = 1.0*s2/n - avg*avg

    return sqrt(res)

@cast_to()
@transform()
@check_boundary()
def fssd(a, query=None, left=None, right=None):
    """
    Compute sample standard deviation of sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    s = sum(a[left:right+1])
    s2 = sum(item*item for item in a[left:right+1])
    n = right+1-left
    avg = 1.0*s/n

    if n <= 1:
        return None
    res = 1.0/(n-1)*(s2 - avg*avg*n)

    return sqrt(res)

@cast_to()
@transform()
@check_boundary()
def fmax(a, query=None, left=None, right=None):
    """
    Find maximum value in the sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """
    
    return float(max(a[left:right+1]))

@cast_to()
@transform()
@check_boundary()
def fmin(a, query=None, left=None, right=None):
    """
    Find minimum value in the sub-sequence ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """
    
    return float(min(a[left:right+1]))

@cast_to()
@check_boundary()
def ifexist(a, query, left=None, right=None):
    """
    Check if the value ``query`` exists in the sub-sequence ``a[left:right+1]``. If ``query`` is ``None``, 
    return ``None``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: element in question
    """

    if query is None:
        return None
    try:
        _ = a.index(query, left, right+1)
        return 1
    except ValueError:
        return 0

@cast_to()
@transform()
@check_boundary()
def count(a, query=None, left=None, right=None):
    """
    Count the number of elements in the sub-sequence ``a[left:right+1]``

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    return right+1-left

@cast_to()
@transform()
@check_boundary()
def existcount(a, query, left=None, right=None):
    """
    Count the number of times ``query`` appearing in the sub-sequence ``a[left:right+1]``

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: element in question
    """

    return a[left:right+1].count(query)

@cast_to()
@transform()
@check_boundary()
def countdistinct(a, query=None, left=None, right=None):
    """
    Count the number of distinct elements in sub-sequence of ``a[left:right+1]``

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    return len(set(a[left:right+1]))

@cast_to()
@check_boundary()
def last(a, query=None, left=None, right=None):

    """
    Return the last element in the sub-sequence of ``a[left:right+1]``

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    return a[right]

@cast_to()
@transform()
@check_boundary()
def ratio(a, query, left=None, right=None, prior=None):
    """
    Compute the (weighted) ratio between the number of times of ``query`` appearing and the length of sub-sequence of ``a[left:right+1]``.

    - If ``prior``, namely the prior probability that ``query`` appears in ``a[left:right+1]``, is given, the ratio is computed with regularization as follows:

        .. math::

            (1 - \\frac{1}{l+1}) \\cdot r + \\frac{1}{l+1} \\cdot r,

        where :math:`l` is the length of the sub-sequence and :math:`r` is the raw ratio, namely, the (raw) number of times that ``query`` appears.
        The regularization is helpful when there are only a few samples, that is, when the length of ``a[left:right+1]`` is small. In this case,
        the raw ratio lacks sufficient statistical significance and this computation favors the prior probability. On the other hand, when there are
        sufficient samples, the weight of prior probability decreases and that of raw ratio increases.
    - otherwise, returns the raw ratio.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: element in question
    :param prior: prior probability of ``query``
    """

    sub_seq = a[left:right+1]
    res = 1.0*sub_seq.count(query)/len(sub_seq)
    if prior is not None:
        reg_coef = 1 - 1/(len(sub_seq)+1) # the more data, the more weight we put on the ratio
        res = reg_coef*res + (1 - reg_coef)*prior
    return res

@cast_to()
@transform()
@check_boundary()
def entropy(a, query=None, left=None, right=None):
    """
    Compute the entropy of sub-sequence of ``a[left:right+1]``

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    count = {}
    for i in range(left, right+1):
        count[a[i]] = count.get(a[i], 0) + 1
        
    total = sum(count.values())
    probs = [1.0*v/total for v in count.values()]
    res = sum(prob*log(prob) for prob in probs)

    return abs(res)

@cast_to()
@transform()
@check_boundary()
def avg(a, query=None, left=None, right=None):
    """
    Compute the average of sub-sequence of ``a[left:right+1]``.

    :param a: iterable
    :param left: ``int`` or ``None``
    :param right: ``int`` or ``None``
    :param query: preserved
    """

    l = a[left:right+1]
    res = 1.0*sum(l)/len(l) if l else None
    return res

@six.add_metaclass(abc.ABCMeta)
class AggregateFunction(object):
    """
    Base abstract class for Python aggregate functions.
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
    def callable(self):
        """
        Returns the callable method that the class wraps. 
        The wrapped method must follow the signature of ``some_callable(a, query=None, left=None, right=None)``. 
        It indicates that we apply the ``some_callable`` method to the sublist ``a[left:right+1]`` with the element in question 
        whose value is equal to ``query``.

        For example, we might return the method ``existcount(a, query, left=None, right=None)`` which 
        counts the element equal to ``query`` in the sublist ``a[left:right+1]``.
        """

        raise NotImplementedError()
    
    def __call__(self, *args, **kwargs):

        return self.callable()(*args, **kwargs)

class MinFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fmin`.
    """

    @classmethod
    def name(cls):
        return 'min'

    def callable(self):
        return fmin

class MaxFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fmax`.
    """

    @classmethod
    def name(cls):
        return 'max'

    def callable(self):
        return fmax

class SumFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fsum`.
    """

    @classmethod
    def name(cls):
        return 'sum'

    def callable(self):
        return fsum

class PopVarianceFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fpvar`.
    """

    @classmethod
    def name(cls):
        return 'popvar'

    def callable(self):
        return fpvar

class SampVarianceFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fsvar`.
    """

    @classmethod
    def name(cls):
        return 'smpvar'

    def callable(self):
        return fsvar

class PopSDFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fpsd`.
    """

    @classmethod
    def name(cls):
        return 'popsd'

    def callable(self):
        return fpsd

class SampSDFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.fssd`.
    """

    @classmethod
    def name(cls):
        return 'smpsd'

    def callable(self):
        return fssd

class AvgFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.avg`.
    """

    @classmethod
    def name(cls):
        return 'avg'

    def callable(self):
        return avg

class CountFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.count`.
    """

    @classmethod
    def name(cls):
        return 'cnt'

    def callable(self):
        return count

class ExistCountFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.existcount`.
    """

    @classmethod
    def name(cls):
        return 'ecnt'

    def callable(self):
        return existcount

class CountDistinctFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.countdistinct`.
    """

    @classmethod
    def name(cls):
        return 'dcnt'

    def callable(self):
        return countdistinct

class LastFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.last`.
    """

    @classmethod
    def name(cls):
        return 'last'

    def callable(self):
        return last

class ExistFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.ifexist`.
    """

    @classmethod
    def name(cls):
        return 'exist'

    def callable(self):
        return ifexist

class RatioFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.ratio`.
    """

    @classmethod
    def name(cls):
        return 'rto'

    def callable(self):
        return ratio

class EntropyFunction(AggregateFunction):
    """
    See :py:class:`autofe.legacy.py_functions.entropy`.
    """

    @classmethod
    def name(cls):
        return 'etrp'

    def callable(self):
        return entropy