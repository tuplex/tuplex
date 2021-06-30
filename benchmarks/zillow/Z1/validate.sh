#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2020
# validate that output is the same across all frameworks!

INPUT_FILE=/data/zillow/Z1_preprocessed/zillow_Z1_10G.csv

PYTHON=python3.6
PYPY=/opt/pypy3/bin/pypy3

# pure python runs with/without pypy
${PYTHON} runpython.py --mode tuple --output-path python3_tuple --path $INPUT_FILE
${PYTHON} runpython.py --mode dict --output-path python3_dict --path $INPUT_FILE
${PYPY} runpython.py --mode tuple --output-path pypy3_tuple --path $INPUT_FILE
${PYPY} runpython.py --mode dict --output-path pypy3_dict --path $INPUT_FILE

# Pyspark runs with/without pypy
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
spark-submit runpyspark.py --mode rdd_tuple --output-path pyspark_python3_tuple --path $INPUT_FILE
spark-submit runpyspark.py --mode rdd_dict --output-path pyspark_python3_dict --path $INPUT_FILE
spark-submit runpyspark.py --mode sql --output-path pyspark_python3_df --path $INPUT_FILE
export PYSPARK_PYTHON=${PYPY}
export PYSPARK_DRIVER_PYTHON=${PYPY}
spark-submit runpyspark.py --mode rdd_tuple --output-path pyspark_pypy3_tuple --path $INPUT_FILE
spark-submit runpyspark.py --mode rdd_dict --output-path pyspark_pypy3_dict --path $INPUT_FILE
spark-submit runpyspark.py --mode sql --output-path pyspark_pypy3_df --path $INPUT_FILE

# Tuplex (need to fix folder...)
${PYTHON} runtuplex.py --output-path tuplex_output --path $INPUT_FILE

# Dask + Pandas
${PYTHON} rundask.py --mode dask --output-path dask_output --path $INPUT_FILE
${PYTHON} rundask.py --mode pandas --output-path pandas_output --path $INPUT_FILE

# run validation script
${PYTHON} validate_all.py