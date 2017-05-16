#!/bin/bash

# Clone, install, and run
git clone https://github.com/aguimaraesduarte/FirefoxCrashGraphs.git

# We use jupyter by default, but here we want to use python
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS
spark-submit main.py --master yarn
