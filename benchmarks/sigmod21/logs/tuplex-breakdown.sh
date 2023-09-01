#!/usr/bin/env bash

# use 3 runs and a timeout after 60min
NUM_RUNS=3
TIMEOUT=3600

DATA_PATH='/disk/data/weblogs_clean'
IP_PATH='/disk/data/ip_blacklist.csv'
# we're using 16 threads, pin to 2 sockets (because each socket has 8 cores so we can utilize 16 cores). Some frameworks actually use more threads
HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
RESDIR=results_logs
PIPELINE_TYPE='strip'
mkdir -p ${RESDIR}

# bar 1/6: noopt
echo "running tuplex without any optimizations"
python3 create_conf.py | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-noopt.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-noopt-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type ${PIPELINE_TYPE} >$LOG 2>$LOG.stderr
done

# bar 2/6: noopt + llvm
echo "running tuplex without any optimizations + llvm"
python3 create_conf.py --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-noopt+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-noopt+llvm-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type ${PIPELINE_TYPE} >$LOG 2>$LOG.stderr
done

# bar 3/6: logical
echo "running tuplex with logical optimizations"
python3 create_conf.py --opt-filter --opt-pushdown | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-logical.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-logical-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type ${PIPELINE_TYPE} >$LOG 2>$LOG.stderr
done

# bar 4/6: logical + llvm
echo "running tuplex with logical optimizations + llvm"
python3 create_conf.py --opt-filter --opt-pushdown --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-logical+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-logical+llvm-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type ${PIPELINE_TYPE} >$LOG 2>$LOG.stderr
done

# bar 5/6: logical + null
echo "running tuplex with null-value optimization"
python3 create_conf.py --opt-filter --opt-pushdown --opt-null | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-null.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-null-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type ${PIPELINE_TYPE} >$LOG 2>$LOG.stderr
done

# bar 6/6: logical + null + llvm
echo "running tuplex with null-value optimization & LLVM"
python3 create_conf.py --opt-filter --opt-pushdown --opt-null --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-null+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-null+llvm-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type ${PIPELINE_TYPE} >$LOG 2>$LOG.stderr
done