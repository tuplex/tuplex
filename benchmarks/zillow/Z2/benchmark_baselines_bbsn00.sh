#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2021
# run benchmark of all baselines (C++, Scala, PySparksQL-Scala)...

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark_baselines_bbsn00.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ././benchmark_baselines_bbsn00.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

# bbsn00
export PATH=/opt/pypy3.6/bin/:$PATH

INPUT_FILE=data/zillow_clean@10G.csv
#INPUT_FILE=data/zillow_clean.csv # small file for testing purposes
RESDIR=benchmark_results/baselines
OUTPUT_DIR=benchmark_output/baselines
NUM_RUNS=11
TIMEOUT=900
mkdir -p ${RESDIR}
mkdir -p ${OUTPUT_DIR}

# (1) C++ baselines
echo "benchmarking C++"
cd baseline
./build-cpp.sh
cd ..
cp baseline/tester cc_baseline
mkdir -p $OUTPUT_DIR/cc_no_preload_output
mkdir -p $OUTPUT_DIR/cc_preload_output
echo "running C++ baseline without preload"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cc-no-preload-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./cc_baseline --path $INPUT_FILE --output_path $OUTPUT_DIR/cc_no_preload_output >$LOG 2>$LOG.stderr
done
echo "running C++ baseline with preload"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cc-with-preload-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./cc_baseline --path $INPUT_FILE --output_path $OUTPUT_DIR/cc_no_preload_output --preload >$LOG 2>$LOG.stderr
done
rm cc_baseline
# (2) Scala baselines
echo "Running pure Scala baseline"
cd scala
./build.sh
cd ..
mkdir -p $OUTPUT_DIR/scala
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/scala-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} java -Xmx256g -jar scala/target/scala-2.11/ZillowScala-assembly-0.1.jar $INPUT_FILE $OUTPUT_DIR/scala/zillow_out.csv >$LOG 2>$LOG.stderr
done

echo "Running SparkSQL Scala baseline"
cd sparksql-scala
./build.sh
cd ..
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/sparksql-scala-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master 'local[16]' --driver-memory 256g --class tuplex.exp.spark.ZillowClean sparksql-scala/target/scala-2.11/zillowscala_2.11-0.1.jar $INPUT_FILE $OUTPUT_DIR/sparksql-scala >$LOG 2>$LOG.stderr
done
