# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

"""
This module generate pyspark operator code
"""
import yaml
import sys
import os
sys.path.append('.')
from template_utils import *

def table_join(conf):
    return None

def fe(conf):
    return None

def generate_code():
    conf = yaml.load(open('/Users/zxd/solution/autofe/conf/tb.yaml','r'))
    base = '/Users/zxd/solution/autofe/output/'

# fe
    for dag in conf:
        fe_inputs=conf[dag]['inputs']
        fe_ops = []
        fe_ops_args = {}
        tables = []
        fe_table = None
        for op in conf[dag]['ops']:
            fe_ops.append( conf[dag]['ops'][op]['name'] )
            fe_ops_args[op] = conf[dag]['ops'][op]
            tables.append(fe_ops_args[op]['args']['table'])
            wide_table = conf[dag]['ops'][op]['output']

        template_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/templates'
        render_ops_template(template_dir, 'fe.jinja2', fe_inputs, tables, fe_ops, fe_ops_args, wide_table, base+dag+'.py')


if __name__ == '__main__':
    generate_code()