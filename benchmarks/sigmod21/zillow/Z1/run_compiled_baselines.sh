#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2021
# runs zillow Z1 benchmark for docker rewritten

# for docker image
export PATH=/opt/pypy3/bin/:$PATH

INPUT_FILE=/data/zillow/Z1_preprocessed/zillow_Z1_10G.csv
RESDIR=/results/zillow/Z1
OUTPUT_DIR=/results/output/zillow/Z1
NUM_RUNS=11
TIMEOUT=1800
PYTHON=python3.6
PYPY=/opt/pypy3/bin/pypy3
mkdir -p ${RESDIR}

# (1) C++
cd baseline || echo "cd baseline failed"
# compile cpp
echo "compiling cpp"
for ((r = 1; r <= NUM_RUNS; r++)); do
COMPILE_LOG="${RESDIR}/cpp_baseline-compile-run-$r.txt"
(time ./build-cpp.sh) &>$COMPILE_LOG
done

# preload vs. no preload
echo "benchmarking cpp (without preload)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cpp_no_preload-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./tester --path $INPUT_FILE --output_path no_preload_output >$LOG 2>$LOG.stderr
done

# preload data first
echo "benchmarking cpp (with preload)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cpp_preload-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./tester --path $INPUT_FILE --output_path preload_output --preload  >$LOG 2>$LOG.stderr
done
cd .. || echo "cd .. failed"

# (2) Scala
cd scala || echo "cd scala failed"
if [ -d target ]; then rm -rf target; fi
sbt package
for ((r = 1; r <= NUM_RUNS; r++)); do
  COMPILE_LOG="${RESDIR}/scala_baseline-compile-run-$r.txt"
  rm -rf target
  (time sbt package) &>$COMPILE_LOG
done
sbt assembly
cd .. || echo "cd .. failed"

# run
mkdir -p $OUTPUT_DIR/scala
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/scala-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} java -Xmx100g -jar scala/target/scala-2.11/ZillowScala-assembly-0.1.jar $INPUT_FILE $OUTPUT_DIR/scala/zillow_out.csv >$LOG 2>$LOG.stderr
done

# (3) Spark w. Scala
echo "Running SparkSQL Scala baseline"
cd sparksql-scala || echo "cd sparksql-scala failed"
if [ -d target ]; then rm -rf target; fi
sbt package
for ((r = 1; r <= NUM_RUNS; r++)); do
  COMPILE_LOG="${RESDIR}/sparksql-scala_baseline-compile-run-$r.txt"
  rm -rf target
  (time sbt package) &>$COMPILE_LOG
done
cd .. || echo "cd .. failed"

for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/sparksql-scala-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master 'local[16]' --driver-memory 100g --class tuplex.exp.spark.ZillowClean sparksql-scala/target/scala-2.11/zillowscala_2.11-0.1.jar $INPUT_FILE $OUTPUT_DIR/sparksql-scala >$LOG 2>$LOG.stderr
done

