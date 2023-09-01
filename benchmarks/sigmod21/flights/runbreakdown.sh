#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2021
# use 11 runs and a timeout after 60min
NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=3600

LG_INPUT_PATH='/data/flights'
SM_INPUT_PATH='/data/flights_small'
RESDIR=/results/flights/flights_breakdown
OUTPUT_DIR=/results/output/flights_output_breakdown
PYTHON=python3.6
DATA_PATH=$SM_INPUT_PATH
mkdir -p ${RESDIR}

# use small file to avoid swapping

CONF="--executor-memory 60G --executor-count 3"

# in this experiment optimizations are successively enabled and the effect of stage-fusion
# simulated via the .cache() statement.

# no stage fusion
# bar 1/6: noopt
echo "running tuplex without any optimizations"
python3.6 create_conf.py ${CONF} | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-nosf-noopt.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-nosf+noopt-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --simulate-spark --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 2/6: llvm
echo "running tuplex without any optimizations + llvm"
python3.6 create_conf.py ${CONF} --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-nosf-noopt+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-nosf+noopt+llvm-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --simulate-spark --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 3/6: logical
echo "running tuplex with logical optimizations"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-nosf-logical.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-nosf+logical-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --simulate-spark --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 4/6: logical + llvm
echo "running tuplex with logical optimizations + llvm"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-nosf-logical+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-nosf+logical+llvm-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --simulate-spark --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 5/6: logical + null
echo "running tuplex with null-value optimization"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown --opt-null | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-nosf-null.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-nosf+null-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --simulate-spark --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 6/6: logical + null + llvm
echo "running tuplex with null-value optimization & LLVM"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown --opt-null --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-nosf-null+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-nosf+null+llvm-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --simulate-spark --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done




# now the configs WITH stage fusion
# bar 1/6: noopt
echo "running tuplex without any optimizations"
python3.6 create_conf.py ${CONF} | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-noopt.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-noopt-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 2/6: llvm
echo "running tuplex without any optimizations + llvm"
python3.6 create_conf.py ${CONF} --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-noopt+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-noopt+llvm-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 3/6: logical
echo "running tuplex with logical optimizations"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-logical.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-logical-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 4/6: logical + llvm
echo "running tuplex with logical optimizations + llvm"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-logical+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-logical+llvm-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 5/6: logical + null
echo "running tuplex with null-value optimization"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown --opt-null | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-null.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-null-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done

# bar 6/6: logical + null + llvm
echo "running tuplex with null-value optimization & LLVM"
python3.6 create_conf.py ${CONF} --opt-filter --opt-pushdown --opt-null --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-null+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-null+llvm-run-$r.txt"
  rm -rf $OUTPUT_DIR
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done


# cleanup output
rm -rf $OUTPUT_DIR
