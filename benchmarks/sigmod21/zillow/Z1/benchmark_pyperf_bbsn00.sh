#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2020
# validate that output is the same across all frameworks!


# bbsn00
export PATH=/opt/pypy3.6/bin/:$PATH

INPUT_FILE=/disk/data/zillow/large10GB.csv
RESDIR=results_pyperf
NUM_RUNS=5
TIMEOUT=900
mkdir -p ${RESDIR}

echo "benchmarking tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex (single threaded)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex_st-run-$r.txt"
  timeout $TIMEOUT python3 runtuplex_st.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/python3_tuple-run-$r.txt"
  timeout $TIMEOUT python3 runpython.py --mode tuple --output-path python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/python3_dict-run-$r.txt"
  timeout $TIMEOUT python3 runpython.py --mode dict --output-path python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_tuple-run-$r.txt"
  timeout $TIMEOUT pypy3 runpython.py --mode tuple --output-path pypy3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_dict-run-$r.txt"
  timeout $TIMEOUT pypy3 runpython.py --mode dict --output-path pypy3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# pypy3 loop version
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_tuple_wloops-run-$r.txt"
  timeout $TIMEOUT pypy3 runpython_wloops.py --mode tuple --output-path pypy3_tuple_wloops --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pypy3_dict_wloops-run-$r.txt"
  timeout $TIMEOUT pypy3 runpython_wloops.py --mode dict --output-path pypy3_dict_wloops --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Pyspark runs with/without pypy
echo "benchmarking pyspark"
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_tuple-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_tuple --output-path pyspark_python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_dict-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_dict --output-path pyspark_python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_python3_df-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode sql --output-path pyspark_python3_df --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark with pypy"

export PYSPARK_PYTHON=pypy3
export PYSPARK_DRIVER_PYTHON=pypy3
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_tuple-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_tuple --output-path pyspark_pypy3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_dict-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode rdd_dict --output-path pyspark_pypy3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark_pypy3_df-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --mode sql --output-path pyspark_pypy3_df --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Dask + Pandas
echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask_python3-run-$r.txt"
  timeout $TIMEOUT python3 rundask.py --mode dask --output-path dask_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pandas"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pandas_python3-run-$r.txt"
  timeout $TIMEOUT python3 rundask.py --mode pandas --output-path pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask_pypy3-run-$r.txt"
  timeout $TIMEOUT pypy3 rundask.py --mode dask --output-path dask_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking pandas"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pandas_pypy3-run-$r.txt"
  timeout $TIMEOUT pypy3 rundask.py --mode pandas --output-path pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# Ray/Modin?
# requires pip3 install modin[ray]
# export MODIN_ENGINE=ray  # Modin will use Ray
#export MODIN_ENGINE=dask  # Modin will use Dask


echo "benchmarking python with cython"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cython3_tuple-run-$r.txt"
  timeout $TIMEOUT python3 runpython.py --compiler cython --mode tuple --output-path python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cython3_dict-run-$r.txt"
  timeout $TIMEOUT python3 runpython.py --compiler cython --mode dict --output-path python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking python with nuitka"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/nuitka3_tuple-run-$r.txt"
  timeout $TIMEOUT python3 runpython.py --compiler nuitka --mode tuple --output-path python3_tuple --path $INPUT_FILE >$LOG 2>$LOG.stderr
done
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/nuitka3_dict-run-$r.txt"
  timeout $TIMEOUT python3 runpython.py --compiler nuitka --mode dict --output-path python3_dict --path $INPUT_FILE >$LOG 2>$LOG.stderr
done