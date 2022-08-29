NUM_RUNS=3
TIMEOUT=14400

RESDIR="benchmark_results"
DATA_PATH="/hot/data/flights_all/"
PLAIN_OUTPUT="/hot/scratch/$USER/output/incremental"
INCREMENTAL_OUTPUT="/hot/scratch/$USER/output/plain"

rm -rf $RESDIR
rm -rf $INCREMENTAL_OUTPUT
rm -rf $PLAIN_OUTPUT

mkdir -p $RESDIR

echo "running out-of-order experiments"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "trial ($r/$NUM_RUNS)"

  echo "running plain"
  LOG="${RESDIR}/out-of-order-plain-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --data-path $DATA_PATH --output-path $PLAIN_OUTPUT >$LOG 2>$LOG.stderr

  echo "running incremental"
  LOG="${RESDIR}/out-of-order-inc-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --inc-res --data-path $DATA_PATH --output-path $INCREMENTAL_OUTPUT >$LOG 2>$LOG.stderr
done

echo "running in-order experiments"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "trial ($r/$NUM_RUNS)"

  echo "running plain"
  LOG="${RESDIR}/in-order-plain-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --in-order --data-path $DATA_PATH --output-path $PLAIN_OUTPUT >$LOG 2>$LOG.stderr

  echo "running incremental"
  LOG="${RESDIR}/in-order-inc-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --in-order --inc-res --data-path $DATA_PATH --output-path $INCREMENTAL_OUTPUT >$LOG 2>$LOG.stderr
done
