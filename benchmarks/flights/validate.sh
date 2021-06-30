#!/usr/bin/env bash
# (c) 2017-2020 L.Spiegelberg
# runs different experiments

INPUT_FILE=/data/flights/flights_on_time_performance_2019_01.csv


# run tunplex
python3.6 runtuplex.py --path ${INPUT_FILE}

# run pyspark
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6
spark-submit runpyspark.py --path ${INPUT_FILE}

# run dask
python3.6 rundask.py --path ${INPUT_FILE}


# run (super slow) validation script
python3.6 validate_all.py
