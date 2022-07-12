#!/usr/bin/env bash

# use 5 runs and a timeout after 60min
NUM_RUNS=1
TIMEOUT=3600

#DATA_PATH='/disk/data/weblogs_clean'
DATA_PATH='/disk/data/weblogs_clean/2000.01.01.txt'
IP_PATH='/disk/data/ip_blacklist.csv'

RESDIR=results_logs

mkdir -p ${RESDIR}

echo "running tuplex (regex mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-regex-run-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type regex >$LOG 2>$LOG.stderr
done


# spark
echo "benchmarking pyspark (fast spark regex)"
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-spark-regex-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type 'spark_regex' >$LOG 2>$LOG.stderr
done

