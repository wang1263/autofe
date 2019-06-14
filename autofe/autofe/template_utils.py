# coding: UTF-8
########################################################################
#
# Copyright (c) 2018 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

import os
import sys
import functools
import unittest
import jinja2
from jinja2 import Environment, FileSystemLoader

def render_ops_template(template_dir, template_file, fe_inputs, tables, ops, ops_args, wide_table, out_file):
    env = Environment( loader=FileSystemLoader(template_dir) )
    template = env.get_template( template_file )
    content = template.render( fe_inputs = fe_inputs, tables = tables, op_list=ops, op_args=ops_args, wide_table=wide_table)
    output = open(out_file, 'w')
    output.write(content)