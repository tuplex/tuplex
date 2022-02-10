#!/usr/bin/env bash
# (c) 2021 L.Spiegelberg
# runs Tuplex TPCH Q19 benchmark on AWS setup

# use 11 runs and a timeout after 60min
NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=3600

# preprocessed lineitem path (i.e. integers + only needed columns)
L_INPUT_PATH='/data/tpch/q19_preprocessed/lineitem-sf-10.tbl'
P_INPUT_PATH='/data/tpch/q19_preprocessed/part-sf-10.tbl'
RESDIR=/results/tpch/q19
OUTPUT_DIR=/results/output/tpch/q19
PYTHON=python3.6
mkdir -p ${RESDIR}
mkdir -p ${OUTPUT_DIR}


# --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH


# Single threaded
echo "Single-threaded"
# types hardcoded in tuplex
${PYTHON} create_conf.py --executor-count 0 --driver-memory 100G --opt-filter --opt-llvm --opt-parser | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_st.json"

echo "Tuplex (E2E)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-st-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex (Cached)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-st-cached-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py ---lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --preprocessed --cache --weld >$LOG 2>$LOG.stderr
done

echo "Weld"
echo "building weld"
export WELD_HOME=/opt/weld/
cd weld && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j 16 && cd ../.. || echo "failed to compile weld"
echo "running benchmark"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  ./weld/build/weldq19 --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --preprocessed 2>$LOG.stderr | tee $LOG
done


# Multithreaded
echo "Multi-threaded"
echo "Hyper"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/hyper-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runhyper.py --weld-mode --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --preprocessed >$LOG 2>$LOG.stderr
done

${PYTHON} create_conf.py --opt-filter --opt-llvm --opt-parser --executor-memory 6G --driver-memory 10G | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_mt.json"

echo "Tuplex (E2E)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py --weld --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --preprocessed >$LOG 2>$LOG.stderr
done

echo "Tuplex (Cached)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py --weld --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --preprocessed --cache >$LOG 2>$LOG.stderr
done

export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "Pyspark RDD (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --mode rdd --preprocessed >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --mode sql --preprocessed >$LOG 2>$LOG.stderr
done

echo "Pyspark RDD (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-cached-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --mode rdd --cache --preprocessed >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-cached-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $L_INPUT_PATH --part_path $P_INPUT_PATH --mode sql --cache --preprocessed >$LOG 2>$LOG.stderr
done
