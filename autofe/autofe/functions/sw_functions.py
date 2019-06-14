# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

"""
This module contains aggregate functions optimized in case of sliding window computation. 

Each aggregate function has an associated pre-defined function name, returned by 
the function's ``name()`` method. When applying an aggregate function in this module, 
you should refer to the function's name. The mapping is as follows:

=======================================     ======
Function                                    Name
=======================================     ======
SlidingWindowMinFunction                    min
SlidingWindowMaxFunction                    max
SlidingWindowSumFunction                    sum
SlidingWindowSquaredSumFunction             sqsum
SlidingWindowPopVarianceFunction            popvar
SlidingWindowSampVarianceFunction           smpvar
SlidingWindowPopSDFunction                  popsd
SlidingWindowSampSDFunction                 smpsd
SlidingWindowAvgFunction                    avg
SlidingWindowCountFunction                  cnt
SlidingWindowExistCountFunction             ecnt
SlidingWindowCountDistinctFunction          dcnt
SlidingWindowLastFunction                   last
SlidingWindowExistFunction                  exist
SlidingWindowRatioFunction                  rto
SlidingWindowEntropyFunction                etrp
=======================================     ======

The above functions are guaranteed to have O(N) time complexity with N number of data.
"""

from __future__ import print_function,division
from math import log,sqrt
from collections import deque

class SlidingWindowAggregateFunction(object):
    """
    Base abstract class for Python aggregate functions with sliding window optimization.
    """

    @classmethod
    def name(cls):
        """
        Return the name reference of the function.

        :return: name of the function
        :rtype: str
        """
        raise NotImplementedError()

    def reset(self):
        """
        Reset to initial states
        """

        self.__init__()

    def push(self, val):
        """
        Push to computing window
        """

        raise NotImplementedError()        

    def pop(self, val):
        """
        Pop out of computing window
        """

        raise NotImplementedError()

    def get(self, query):
        """
        Get result
        """

        raise NotImplementedError()        

class SlidingWindowSumFunction(SlidingWindowAggregateFunction):
    """
    Compute sum of a sliding window
    """
    def __init__(self):
        self.__val = None

    @classmethod
    def name(self):
        return 'sum'

    def push(self, val):
        if self.__val is None:
            self.__val = val
        else:
            self.__val += val
        
    def pop(self, val):
        assert self.__val is not None, 'Must first push!'
        self.__val = self.__val - val

    def get(self, query=None):
        self.__val = None if self.__val is None else float(self.__val)
        return self.__val

class SlidingWindowCountFunction(SlidingWindowAggregateFunction):
    """
    Compute count of a sliding window
    """
    def __init__(self):
        self.__val = 0

    @classmethod
    def name(self):
        return 'cnt'

    def push(self, val):
        self.__val += 1
        
    def pop(self, val):
        self.__val -= 1

    def get(self, query=None):
        return float(self.__val)

class SlidingWindowAvgFunction(SlidingWindowAggregateFunction):
    """
    Compute average of a sliding window
    """
    def __init__(self):
        self.__cnt = SlidingWindowCountFunction()
        self.__sum = SlidingWindowSumFunction()

    @classmethod
    def name(self):
        return 'avg'

    def push(self, val):
        self.__cnt.push(val)
        self.__sum.push(val)

    def pop(self, val):
        self.__cnt.pop(val)
        self.__sum.pop(val)

    def get(self, query=None):
        res = None if self.__cnt.get() == 0 else float(self.__sum.get())/self.__cnt.get()
        return res

class SlidingWindowLastFunction(SlidingWindowAggregateFunction):
    """
    Compute last value of a sliding window
    """
    def __init__(self):
        self.__val = None

    @classmethod
    def name(self):
        return 'last'

    def push(self, val):
        """
        :param val: tuple of column value and cursor index
        """
        self.__val = val # (v, idx)
        
    def pop(self, val):
        pass

    def get(self, query=None):
        # query is condition
        if self.__val is not None and query(self.__val[1]):
            try:
                return float(self.__val[0])
            except:
                return self.__val[0]
        else:
            return None

class SlidingWindowExistCountFunction(SlidingWindowAggregateFunction):
    """
    Compute the count of a value in a sliding window
    """
    def __init__(self):
        self.__counter = {}

    @classmethod
    def name(self):
        return 'ecnt'

    def push(self, val):
        self.__counter[val] = self.__counter.get(val, 0) + 1.
        
    def pop(self, val):
        self.__counter[val] -= 1

    def get(self, query):
        res = self.__counter.get(query, 0)
            
        return float(res)

class SlidingWindowExistFunction(SlidingWindowAggregateFunction):
    """
    Compute existence of a value in a sliding window. If a value exists in the sliding window, returns ``1``; otherwise, returns ``0``.
    """
    def __init__(self):
        self.__ecnt = SlidingWindowExistCountFunction()

    @classmethod
    def name(self):
        return 'exist'

    def push(self, val):
        self.__ecnt.push(val)

    def pop(self, val):
        self.__ecnt.pop(val)

    def get(self, query):
        cnt = self.__ecnt.get(query)
        res = 0 if cnt <= 0 else 1

        return float(res)

class SlidingWindowSquaredSumFunction(SlidingWindowAggregateFunction):
    """
    Compute squared sum of a sliding window
    """
    def __init__(self):
        self.__val = None

    @classmethod
    def name(self):
        return 'sqsum'

    def push(self, val):
        sq_val = val*val
        if self.__val is None:
            self.__val = sq_val
        else:
            self.__val += sq_val
        
    def pop(self, val):
        assert self.__val is not None, 'Must first push!'
        self.__val = self.__val - val*val

    def get(self, query=None):
        self.__val = None if self.__val is None else float(self.__val)
        return self.__val

class SlidingWindowPopVarianceFunction(SlidingWindowAggregateFunction):
    """
    Compute population variance of a sliding window
    """
    def __init__(self):
        self.__sqsum = SlidingWindowSquaredSumFunction()
        self.__avg = SlidingWindowAvgFunction()
        self.__cnt = SlidingWindowCountFunction()

    @classmethod
    def name(self):
        return 'popvar'

    def push(self, val):
        self.__sqsum.push(val)
        self.__avg.push(val)
        self.__cnt.push(val)
        
    def pop(self, val):
        self.__sqsum.pop(val)
        self.__avg.pop(val)
        self.__cnt.pop(val)

    def get(self, query=None):
        avg = self.__avg.get()
        if self.__cnt.get() == 0:
            return None
        res = 1.0*self.__sqsum.get()/self.__cnt.get() - avg*avg
        return res

class SlidingWindowSampVarianceFunction(SlidingWindowAggregateFunction):
    """
    Compute sample variance of a sliding window
    """
    def __init__(self):
        self.__sqsum = SlidingWindowSquaredSumFunction()
        self.__avg = SlidingWindowAvgFunction()
        self.__cnt = SlidingWindowCountFunction()

    @classmethod
    def name(self):
        return 'smpvar'

    def push(self, val):
        self.__sqsum.push(val)
        self.__avg.push(val)
        self.__cnt.push(val)
        
    def pop(self, val):
        self.__sqsum.pop(val)
        self.__avg.pop(val)
        self.__cnt.pop(val)

    def get(self, query=None):
        avg = self.__avg.get()
        if self.__cnt.get() <= 1:
            return None
        res = 1.0/(self.__cnt.get() - 1)*(self.__sqsum.get() - avg*avg*self.__cnt.get())
        return res

class SlidingWindowPopSDFunction(SlidingWindowAggregateFunction):
    """
    Compute population standard deviation of a sliding window
    """
    def __init__(self):
        self.__pvar = SlidingWindowPopVarianceFunction()

    @classmethod
    def name(self):
        return 'popsd'

    def push(self, val):
        self.__pvar.push(val)
        
    def pop(self, val):
        self.__pvar.pop(val)

    def get(self, query=None):
        pvar = self.__pvar.get()
        if pvar is None:
            return None
        res = sqrt(pvar)
        return res

class SlidingWindowSampSDFunction(SlidingWindowAggregateFunction):
    """
    Compute sample standard deviation of a sliding window
    """
    def __init__(self):
        self.__svar = SlidingWindowSampVarianceFunction()

    @classmethod
    def name(self):
        return 'smpsd'

    def push(self, val):
        self.__svar.push(val)
        
    def pop(self, val):
        self.__svar.pop(val)

    def get(self, query=None):
        __svar = self.__svar.get()
        if __svar is None:
            return None
        res = sqrt(__svar)
        return res

class SlidingWindowCountDistinctFunction(SlidingWindowAggregateFunction):
    """
    Compute distinct count of a sliding window
    """
    def __init__(self):
        self.__counter = {}

    @classmethod
    def name(self):
        return 'dcnt'

    def push(self, val):
        self.__counter[val] = self.__counter.get(val, 0) + 1.
        
    def pop(self, val):
        self.__counter[val] -= 1
        if self.__counter[val] <= 0:
            del self.__counter[val]

    def get(self, query=None):
        res = len(set(self.__counter.keys()))
            
        return float(res)

class SlidingWindowRatioFunction(SlidingWindowAggregateFunction):
    """
    Compute ratio of a value in a sliding window
    """
    def __init__(self, prior=None, query=None):
        self.prior = prior
        self.query = query
        self.__ecnt = SlidingWindowExistCountFunction()
        self.__cnt = SlidingWindowCountFunction()

    def reset(self):
        prior = self.prior
        query = self.query
        self.__init__(prior, query)

    @classmethod
    def name(self):
        return 'rto'

    def push(self, val):
        self.__ecnt.push(val)
        self.__cnt.push(val)

    def pop(self, val):
        self.__ecnt.pop(val)
        self.__cnt.pop(val)

    def get(self, query):
        if self.__cnt.get() == 0:
            return None

        query = query if self.query is None else self.query
        res = self.__ecnt.get(query)/self.__cnt.get()

        if self.prior is not None:
            reg_coef = 1 - 1/(self.__cnt.get() + 1) # the more data, the more weight we put on the ratio
            res = reg_coef*res + (1 - reg_coef)*self.prior
            
        return float(res)

class SlidingWindowEntropyFunction(SlidingWindowAggregateFunction):
    """
    Compute entropy of a sliding window
    """
    def __init__(self):
        self.__counter = {}

    @classmethod
    def name(self):
        return 'etrp'

    def push(self, val):
        self.__counter[val] = self.__counter.get(val, 0) + 1

    def pop(self, val):
        self.__counter[val] -= 1
        if self.__counter[val] <= 0:
            del self.__counter[val]        

    def get(self, query=None):
        if not self.__counter:
            return None

        total = sum(self.__counter.values())

        probs = [1.0*v/total for v in self.__counter.values()]
        res = sum(prob*log(prob) for prob in probs)

        return float(abs(res))

class SlidingWindowMinFunction(SlidingWindowAggregateFunction):
    """
    Compute minimum of a sliding window
    """
    def __init__(self):
        self.__deque = deque()

    @classmethod
    def name(self):
        return 'min'

    def push(self, val):
        """
        :param val: tuple of column value and cursor index
        """
        while self.__deque and self.__deque[-1][0] >= val[0]:
            self.__deque.pop()    
        self.__deque.append(val)

    def pop(self, condition):
        while self.__deque and not condition(self.__deque[0][1]):
            self.__deque.popleft()
        

    def get(self, query=None):
        """
        :param query: condition function
        """
        self.pop(query)
        if self.__deque:
            return float(self.__deque[0][0])
        else:
            return None

class SlidingWindowMaxFunction(SlidingWindowAggregateFunction):
    """
    Compute maximum of a sliding window
    """
    def __init__(self):
        self.__deque = deque()

    @classmethod
    def name(self):
        return 'max'

    def push(self, val):
        """
        :param val: tuple of column value and cursor index
        """
        while self.__deque and self.__deque[-1][0] <= val[0]:
            self.__deque.pop()    
        self.__deque.append(val)

    def pop(self, condition):
        while self.__deque and not condition(self.__deque[0][1]):
            self.__deque.popleft()
        

    def get(self, query=None):
        """
        :param query: condition function
        """
        self.pop(query)

        if self.__deque:
            return float(self.__deque[0][0])
        else:
            return None