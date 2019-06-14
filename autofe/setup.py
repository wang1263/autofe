# coding: UTF-8
########################################################################
#
# Copyright (c) 2019 4Paradigm.com, Inc. All Rights Reserved
#
########################################################################

"""
pip install -e .
"""

import setuptools
from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

params = {
    'name':'autofe',
    'version':'0.1',
    'description':'A general framework for feature engineering',
    'long_description':readme(),
    'long_description_content_type':"text/markdown",
    'classifiers':[
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Operating System :: MacOS'
    ],
    'keywords':'feature engineering',
    'url':'http://git.4paradigm.com/yaojunlin/autofe',
    'author':'Sparkle',
    'author_email':'yaojunlin@4paradigm.com',
    # license:'MIT',
    'packages':setuptools.find_packages(),
    'install_requires':[
      'pyspark',
    ],
    'include_package_data':True,
    'zip_safe':False
}

setup(**params)