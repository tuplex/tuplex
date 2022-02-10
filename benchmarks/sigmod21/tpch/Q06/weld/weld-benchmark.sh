

# run this within the docker container
# started via docker run -ti -v /hot/data:/data --cpuset-cpus="0-15" --cpus=16  weld-experiments
# docker run -ti -v /hot/data:/data -v $PWD:/experiments weld-experiments
# i.e. this should pin docker execution to the first two sockets and also mounts the data dir

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


export WELD_NUM_THREADS=16

# to compile need. apt-get install libncurses5-dev libncursesw5-dev

NUM_RUNS=11
BASE_RESDIR=results
RESDIR=$BASE_RESDIR/sf1
mkdir -p $RESDIR

echo "running Weld preprocessed (SF-1)"
DATA_PATH=/data/tpch_preprocessed/sf-1/lineitem.tbl
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  ${HWLOC} ./weldq6 --path $DATA_PATH --preprocessed 2>$LOG.stderr | tee $LOG
done

RESDIR=$BASE_RESDIR/sf10
mkdir -p $RESDIR

echo "running Weld preprocessed (SF-10)"
DATA_PATH=/data/tpch_preprocessed/sf-10/lineitem.tbl
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  ${HWLOC} ./weldq6 --path $DATA_PATH --preprocessed 2>$LOG.stderr | tee $LOG
done