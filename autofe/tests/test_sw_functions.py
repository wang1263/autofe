# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

from __future__ import print_function,division,absolute_import
import os
import sys
import random
import inspect
import functools
import unittest

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), ..)))
from ..autofe import utils, CONSTANTS
from ..autofe.functions import sw_functions
from ..autofe.legacy import py_functions

class FunctionTestCase(unittest.TestCase):
    def setUp(self):
        seed = 5
        k = 1000
        n = 10000
        random.seed(seed)
        self.list = random.sample(range(n), k)
        _py_functions = {}
        for name,member in inspect.getmembers(py_functions, inspect.isclass):
            mro = inspect.getmro(member)
            if mro[1] == py_functions.AggregateFunction:
                _py_functions[member.name()] = member()
        _sw_functions = {}
        for name,member in inspect.getmembers(sw_functions, inspect.isclass):
            mro = inspect.getmro(member)
            if mro[1] == sw_functions.SlidingWindowAggregateFunction:
                _sw_functions[member.name()] = member()
        self.__functions = {}
        for k in _py_functions.keys():
            self.__functions[k] = (_py_functions[k], _sw_functions[k])

    def test_empty_window(self):
        return_zero = ['cnt', 'ecnt', 'exist', 'dcnt']
        idx_func = ['min', 'max', 'last']
        for fname, (_,func) in self.__functions.items():
            if fname in idx_func:
                query = lambda x: x is not None
                res = func.get(query=query)
            else:
                res = func.get(query=0)
            ref = 0 if fname in return_zero else None
            self.assertEqual(res, ref)

        return_zero = ['cnt', 'ecnt', 'exist', 'dcnt', 'last', 'max', 'min', 'sum']
        for fname, (_,func) in self.__functions.items():
            if fname in idx_func:
                func.push((0, 0))
                func.pop(lambda x: x is not None)
            else:
                func.push(0)
                func.pop(0)
            
            if fname in idx_func:
                query = lambda x: x is not None
                res = func.get(query=query)
            else:
                res = func.get(query=0)
            ref = 0 if fname in return_zero else None
            self.assertEqual(res, ref)

    def test_sw_functions(self):
        wsize = 25
        idx_func = ['min', 'max', 'last']
        for i in range(wsize):
            for fname, (py_func, sw_func) in self.__functions.items():
                ref = py_func(self.list, query=self.list[i], left=0, right=i)
                if fname in idx_func:
                    sw_func.push((self.list[i], i))
                    res = sw_func.get(lambda x: x is not None)
                else:
                    sw_func.push(self.list[i])
                    res = sw_func.get(self.list[i])
                self.assertEqual(res, ref)

        for i in range(wsize, len(self.list)):
            for fname, (py_func, sw_func) in self.__functions.items():
                ref = py_func(self.list, query=self.list[i], left=i-wsize+1, right=i)
                if fname in idx_func:
                    sw_func.push((self.list[i], i))
                    res = sw_func.get(lambda x: i-x < wsize)
                else:
                    sw_func.push(self.list[i])
                    sw_func.pop(self.list[i-wsize])
                    res = sw_func.get(self.list[i])
                self.assertEqual(res, ref)
                
        
if __name__ == '__main__':
    unittest.main()
