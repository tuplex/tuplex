#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2021
# runs zillow Z2 benchmark for docker rewritten

# for docker image
export PATH=/opt/pypy3/bin/:$PATH

INPUT_FILE=/data/zillow/Z2_preprocessed/zillow_Z2_10G.csv
RESDIR=/results/zillow/Z2
OUTPUT_DIR=/results/output/zillow/Z2
NUM_RUNS=11
TIMEOUT=1800
PYTHON=python3.6
PYPY=/opt/pypy3/bin/pypy3
mkdir -p ${RESDIR}

echo "benchmarking tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex no-nvo"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex_nonvo-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --no-nvo --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex (single threaded)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex_st-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --single-threaded --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex (single threaded, no-nvo)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex_st-nonvo-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --no-nvo --single-threaded --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex (single threaded w. preload)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex_st-preload-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --single-threaded --preload --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/python3_tuple-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpython.py --mode tuple --output-path ${OUTPUT_DIR}/python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/python3_dict-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpython.py --mode dict --output-path ${OUTPUT_DIR}/python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_tuple-run-$r.txt"
  timeout $TIMEOUT ${PYPY} runpython.py --mode tuple --output-path ${OUTPUT_DIR}/pypy3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_dict-run-$r.txt"
  timeout $TIMEOUT ${PYPY} runpython.py --mode dict --output-path ${OUTPUT_DIR}/pypy3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Pyspark runs with/without pypy
echo "benchmarking pyspark"
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_tuple-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_tuple --output-path ${OUTPUT_DIR}/pyspark_python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_dict-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_dict --output-path ${OUTPUT_DIR}/pyspark_python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_df-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode sql --output-path ${OUTPUT_DIR}/pyspark_python3_df --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark with pypy"
export PYSPARK_PYTHON=${PYPY}
export PYSPARK_DRIVER_PYTHON=${PYPY}
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_tuple-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_tuple --output-path ${OUTPUT_DIR}/pyspark_pypy3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_dict-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_dict --output-path ${OUTPUT_DIR}/pyspark_pypy3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_df-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode sql --output-path ${OUTPUT_DIR}/pyspark_pypy3_df --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Dask + Pandas
echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask_python3-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} rundask.py --mode dask --output-path ${OUTPUT_DIR}/dask_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pandas"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pandas_python3-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} rundask.py --mode pandas --output-path ${OUTPUT_DIR}/pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask_pypy3-run-$r.txt"
  timeout $TIMEOUT ${PYPY} rundask.py --mode dask --output-path ${OUTPUT_DIR}/dask_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pandas"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pandas_pypy3-run-$r.txt"
  timeout $TIMEOUT ${PYPY} rundask.py --mode pandas --output-path ${OUTPUT_DIR}/pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python with cython"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cython3_tuple-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpython.py --compiler cython --mode tuple --output-path ${OUTPUT_DIR}/python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cython3_dict-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpython.py --compiler cython --mode dict --output-path ${OUTPUT_DIR}/python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python with nuitka"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/nuitka3_tuple-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpython.py --compiler nuitka --mode tuple --output-path ${OUTPUT_DIR}/python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/nuitka3_dict-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpython.py --compiler nuitka --mode dict --output-path ${OUTPUT_DIR}/python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# run baselines too
bash run_compiled_baselines.sh
