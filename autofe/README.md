# AutoFE - A general framework for feature engineering

AutoFE is a framework based on Python and PySpark that provides the following features:

* Efficient feature computation that is not only tailored to risk control but generalizable to other scenes
* Standardised modeling that relieves the burden of development and allows more time to explore feature engineering

This README aims to provide a general introduction to AutoFE. For more details, users could refer to [the API documentation](https://nexus.4pd.io/repository/raw-hosted/yaojunlin/autofe-api-documentation/master/public/index.html) (please use VPN to visit).

## Table of Contents

* [More About AutoFE](#more-about-autofe)
* [Installation](#installation)
* [Getting Started](#getting-started)
* [Communication](#communication)
* [Releases and Contributing](#releases-and-contributing)
* [The Team](#the-team)

## More About AutoFE

AutoFE currently contains the following components:

| Component             | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| **autofe.arithmetic** | Library supports arithmetic                                  |
| **autofe.functions**  | Module provides a wide range of functions for Python and PySpark |
| **autofe.window**     | Window-based feature extractor that allows efficient and flexible time-series feature computation |
| **autofe.aggregate**  | Compute grouped aggregate similar to `SQL`'s `group by`      |
| **autofe.woe**        | High-level tool that computes woe features with regularization |

## Installation

### Locally

Follow the steps below to install AutoFE locally.

1. It is recommended to create a virtual environment using [virtualenv](https://virtualenv.pypa.io/en/latest/) so that problems of dependencies and versions could be better addressed.

2. Clone the repository from the remote

   ```bash
   git clone https://github.com/wang1263/autofe.git
   ```

3. Change to the AutoFE directory

   ```bash
   cd autofe
   ```

4. Install dependencies required by AutoFE if they do not exist locally

   ```bash
   pip install -r requirements.txt
   ```

5. Install AutoFE

   ```bash
   python setup.py install
   ```

6. Testing

   Try importing AutoFE package in your local Python environment:

   ```python
   >>> import autofe
   ```

   If you see the following error appear,

   ```python
   Traceback (most recent call last):
     File "<stdin>", line 1, in <module>
   ModuleNotFoundError: No module named 'autofe'
   ```

   it is likely that the installation is not correct. 

   If no such error appears, a further test could be made to see if `PySpark` works with AutoFE as expected. Run the following script:

   ```bash
   cd .. # go to the parent directory of AutoFE
   python -m autofe.tests.test_groupby_aggregate # run test
   ```

   After a few seconds, if you see the following output,

   ```bash
   ----------------------------------------------------------------------
   Ran 4 tests in 24.723s
   
   OK
   ```

   Congratulations! You have successfully installed and configured AutoFE.

The installation duration depends on network condition, computer configuration, as well as whether dependencies are fulfilled or not. If the dependencies have been already fulfilled, the installation should take only several minutes.

### On Prophet platform

Compared to local installation, the Prophet platform does not currently support a direct installation of third-party packages. To install and use AutoFE on Prophet platform, you can follow the steps below.

1. Go to the directory of AutoFE

2. Zip the sub-directory named also autofe (only include .py files)

   ```bash
   zip -r autofe.zip autofe -i "*.py"
   ```

3. Connect via VPN to **m7-model-dev01**, and upload autofe.zip

   ```bash
   scp autofe.zip username@m7-model-dev01:/home/username/dest_dir
   ```

   `username` and `dest_dir` should be replaced with the actual username and target directory. For example,

   ```bash
   scp autofe.zip yaojunlin@m7-model-dev01:/home/yaojunlin/
   ```

   Type the password following the prompt and finish uploading.

   ```bash
   yaojunlin@m7-model-dev01's password:
   autofe.zip                                             100%   25KB  69.9KB/s   00:00
   ```

4. Login to the server (**m7-model-dev01**) and put autofe.zip to HDFS

   ```bash
   hadoop fs -put autofe.zip hdfs://m7-model-hdp01:8020/user/yaojunlin/
   ```

5. On Prophet platform, edit the PySpark operator as follows

   ```python
   # coding: UTF-8
   # input script according to definition of "run" interface
   from trailer import logger
   from pyspark import SparkContext
   from pyspark.sql import SQLContext,HiveContext
   from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
    
   # create HiveContext object
   sc = SparkContext._active_spark_context
   sqlContext = HiveContext(sc)
        
   # import Module files
   py_path = 'hdfs://m7-model-hdp01:8020/user/yaojunlin/autofe.zip' # py_path替换为实际路径
   sc.addPyFile(py_path)
    
   # import autofe and enjoy it!
   import autofe
   from autofe.window import window_aggregate
   from autofe.functions import py_functions
   from autofe.aggregate import groupby_aggregate
   ```

   Voilà!

## Getting Started

The following pointers to get you started:

* [Tutorial](https://wiki.4paradigm.com/pages/viewpage.action?pageId=51908038)
* [The API documentation](https://nexus.4pd.io/repository/raw-hosted/yaojunlin/autofe-api-documentation/master/public/index.html) (please use VPN to visit)

## Communication

Report bugs, contribute new features, discuss implementations, etc. on [wiki](https://wiki.4paradigm.com/pages/viewpage.action?pageId=51907905).

## Releases and Contributing

We welcome all contributions. If you are planning to contribute back bug-fixes, or new features, utility functions or extensions to the core, please submit to the wiki page.
