#!/usr/bin/env bash

# use 5 runs and a timeout after 60min
NUM_RUNS=1
TIMEOUT=3600

DATA_PATH='/disk/data/weblogs_clean'
PCRE2_LIB_DIR='/opt/lib'
PCRE2_INCLUDE_DIR='/opt/include'

# make results directory
RESDIR=results_logs
mkdir -p ${RESDIR}

# make 1G file
echo "making test file"
TEST_FILE=logs_test_file
head -c 1G $DATA_PATH >$TEST_FILE

# compile cpp
echo "compiling cpp"
export LIBRARY_PATH=$PCRE2_LIB_DIR:$LIBRARY_PATH
export LD_LIBRARY_PATH=$PCRE2_LIB_DIR:$LD_LIBRARY_PATH
export C_INCLUDE_PATH=$PCRE2_INCLUDE_DIR:$C_INCLUDE_PATH
export CPLUS_INCLUDE_PATH=$PCRE2_INCLUDE_DIR:$CPLUS_INCLUDE_PATH
g++ -O3 -march=native -o tester cpp_regex.cpp -lpcre2-8

# compile java
echo "compiling java"
javac JavaRegexTest.java

# cpp
echo "running cpp"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cpp-run-$r.txt"
  timeout $TIMEOUT ./tester $TEST_FILE >$LOG 2>$LOG.stderr
done

# java
echo "benchmarking java"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/java-run-$r.txt"
  timeout $TIMEOUT java -Xms2g JavaRegexTest $TEST_FILE >$LOG 2>$LOG.stderr
done

