# bbsn00
export PATH=/opt/pypy3.6/bin/:$PATH

INPUT_FILE=/disk/data/zillow/large10GB.csv
RESDIR=results_zillow
NUM_RUNS=3
TIMEOUT=900
HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
mkdir -p ${RESDIR}

# bar 1/6: noopt
echo "running tuplex without any optimizations"
python3 create_conf.py | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-noopt.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-noopt-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# bar 2/6: noopt + llvm
echo "running tuplex without any optimizations + llvm"
python3 create_conf.py --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-noopt+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-noopt+llvm-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# bar 3/6: logical
echo "running tuplex with logical optimizations"
python3 create_conf.py --opt-filter --opt-pushdown | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-logical.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-logical-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# bar 4/6: logical + llvm
echo "running tuplex with logical optimizations + llvm"
python3 create_conf.py --opt-filter --opt-pushdown --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-logical+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-logical+llvm-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# bar 5/6: logical + null
echo "running tuplex with null-value optimization"
python3 create_conf.py --opt-filter --opt-pushdown --opt-null | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-null.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-null-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# bar 6/6: logical + null + llvm
echo "running tuplex with null-value optimization & LLVM"
python3 create_conf.py --opt-filter --opt-pushdown --opt-null --opt-llvm | tee tuplex_config.json
cp tuplex_config.json $RESDIR/tuplex_config-null+llvm.json
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-null+llvm-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done