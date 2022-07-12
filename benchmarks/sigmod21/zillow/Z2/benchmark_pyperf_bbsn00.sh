#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2020
# validate that output is the same across all frameworks!

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ./benchmark_pyperf_bbsn00.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

# bbsn00
export PATH=/opt/pypy3.6/bin/:$PATH

INPUT_FILE=data/zillow_clean@10G.csv
#INPUT_FILE=data/zillow_clean.csv
RESDIR=benchmark_results/pyperf
OUTPUT_DIR=benchmark_output/pyperf
NUM_RUNS=11
TIMEOUT='8h'
mkdir -p ${RESDIR}
mkdir -p ${OUTPUT_DIR}

echo "benchmarking tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex (single threaded)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-st-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --output-path ${OUTPUT_DIR}/tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/python3_tuple-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runpython.py --mode tuple --output-path ${OUTPUT_DIR}/python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/python3_dict-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runpython.py --mode dict --output-path ${OUTPUT_DIR}/python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_tuple-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} pypy3 runpython.py --mode tuple --output-path ${OUTPUT_DIR}/pypy3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_dict-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} pypy3 runpython.py --mode dict --output-path ${OUTPUT_DIR}/pypy3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# pypy3 loop version
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_tuple_wloops-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} pypy3 runpython_wloops.py --mode tuple --output-path ${OUTPUT_DIR}/pypy3_tuple_wloops --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_dict_wloops-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} pypy3 runpython_wloops.py --mode dict --output-path ${OUTPUT_DIR}/pypy3_dict_wloops --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Pyspark runs with/without pypy
echo "benchmarking pyspark"
export PYSPARK_PYTHON=python3.7
export PYSPARK_DRIVER_PYTHON=python3.7
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_tuple-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_tuple --output-path ${OUTPUT_DIR}/pyspark_python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_dict-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_dict --output-path ${OUTPUT_DIR}/pyspark_python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_df-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode sql --output-path ${OUTPUT_DIR}/pyspark_python3_df --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark with pypy"

export PYSPARK_PYTHON=pypy3
export PYSPARK_DRIVER_PYTHON=pypy3
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_tuple-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_tuple --output-path ${OUTPUT_DIR}/pyspark_pypy3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_dict-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_dict --output-path ${OUTPUT_DIR}/pyspark_pypy3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_df-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode sql --output-path ${OUTPUT_DIR}/pyspark_pypy3_df --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Dask + Pandas
echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask_python3-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 rundask.py --mode dask --output-path ${OUTPUT_DIR}/dask_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pandas"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pandas_python3-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 rundask.py --mode pandas --output-path ${OUTPUT_DIR}/pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask_pypy3-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} pypy3 rundask.py --mode dask --output-path ${OUTPUT_DIR}/dask_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pandas"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pandas_pypy3-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} pypy3 rundask.py --mode pandas --output-path ${OUTPUT_DIR}/pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Ray/Modin?
# requires pip3 install modin[ray]
# export MODIN_ENGINE=ray  # Modin will use Ray
#export MODIN_ENGINE=dask  # Modin will use Dask


echo "benchmarking python with cython"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cython3_tuple-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runpython.py --compiler cython --mode tuple --output-path ${OUTPUT_DIR}/python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cython3_dict-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runpython.py --compiler cython --mode dict --output-path ${OUTPUT_DIR}/python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python with nuitka"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/nuitka3_tuple-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runpython.py --compiler nuitka --mode tuple --output-path ${OUTPUT_DIR}/python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/nuitka3_dict-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runpython.py --compiler nuitka --mode dict --output-path ${OUTPUT_DIR}/python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
