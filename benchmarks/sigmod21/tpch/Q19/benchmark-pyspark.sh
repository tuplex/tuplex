#!/usr/bin/env bash
# this script benchmarks only pyspark

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ./benchmark.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

# use 10 runs (first one ignore, it's warmup)
NUM_RUNS=11
TIMEOUT=14400

BASE_RESDIR=results_tpcq19

mkdir -p ${BASE_RESDIR}
mkdir -p ${BASE_RESDIR}/sf1
mkdir -p ${BASE_RESDIR}/sf10


export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
echo "benchmarking pyspark (SF-1)"
LINEITEM_DATA_PATH=/hot/data/tpch/sf-1/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch/sf-1/part.tbl
RESDIR=${BASE_RESDIR}/sf1
echo "Pyspark nosql (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode nosql >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode sql >$LOG 2>$LOG.stderr
done

echo "Pyspark nosql (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode nosql --cache >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode sql --cache >$LOG 2>$LOG.stderr
done


echo "benchmarking pyspark (SF-10)"
LINEITEM_DATA_PATH=/hot/data/tpch/sf-10/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch/sf-10/part.tbl
RESDIR=${BASE_RESDIR}/sf10
echo "Pyspark RDD (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode nosql >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode sql >$LOG 2>$LOG.stderr
done

echo "Pyspark RDD (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode nosql --cache >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode sql --cache >$LOG 2>$LOG.stderr
done
