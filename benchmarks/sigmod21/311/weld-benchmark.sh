

# run this within the docker container
# started via docker run -ti -v /hot/data:/data --cpuset-cpus="0-15" --cpus=16  weld-experiments
# docker run -ti -v /hot/data:/data weld-experiments
# i.e. this should pin docker execution to the first two sockets and also mounts the data dir


# to compile need. apt-get install libncurses5-dev libncursesw5-dev

NUM_RUNS=20
RESDIR=results
mkdir -p $RESDIR
DATA_PATH=/data/weld/311-service-requests-sf\=2000.csv

export WELD_NUM_THREADS=16
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  python2 rungrizzly.py --path $DATA_PATH 2>$LOG.stderr | tee $LOG
done
