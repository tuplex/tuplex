#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs the central hyperspecialization vs. no hyperspecialization experiment

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=7 #4 #10
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=results_hyper/internal-fmt
mkdir -p ${RESDIR}
PYTHON=python3.9
echo "benchmarking nohyper (hot)"
[ -d job ] && rm -rf job
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-nohyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-hyper --internal-fmt >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-nohyper-run-$r.json"
done

# mv job folder
mkdir -p $RESDIR/flights-nohyper
mv job/*.json $RESDIR/flights-nohyper
rm -rf job


# hyper-specialized
echo "benchmarking hyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-hyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --internal-fmt >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-hyper-run-$r.json"
done


# mv job folder
mkdir -p $RESDIR/flights-hyper
mv job/*.json $RESDIR/flights-hyper
rm -rf job

echo "no constant folding now..."

echo "benchmarking nohyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-nohyper-nocf-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-cf --no-hyper --internal-fmt >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-nohyper-nocf-run-$r.json"
done


# mv job folder
mkdir -p $RESDIR/flights-nohyper-nocf
mv job/*.json $RESDIR/flights-nohyper-nocf
rm -rf job


# hyper-specialized
echo "benchmarking hyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-hyper-nocf-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-cf --internal-fmt >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-hyper-nocf-run-$r.json"
done


# mv job folder
mkdir -p $RESDIR/flights-hyper-nocf
mv job/*.json $RESDIR/flights-hyper-nocf
rm -rf job

#-------------------------------------------------------------------------------------------------
# set 2: badparse format optimization
RESDIR=results_hyper/badparse-fmt
mkdir -p ${RESDIR}
PYTHON=python3.9
echo "benchmarking nohyper (hot)"
[ -d job ] && rm -rf job
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-nohyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-hyper >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-nohyper-run-$r.json"
done

# mv job folder
mkdir -p $RESDIR/flights-nohyper
mv job/*.json $RESDIR/flights-nohyper
rm -rf job


# hyper-specialized
echo "benchmarking hyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-hyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-hyper-run-$r.json"
done


# mv job folder
mkdir -p $RESDIR/flights-hyper
mv job/*.json $RESDIR/flights-hyper
rm -rf job

echo "no constant folding now..."

echo "benchmarking nohyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-nohyper-nocf-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-cf --no-hyper >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-nohyper-nocf-run-$r.json"
done


# mv job folder
mkdir -p $RESDIR/flights-nohyper-nocf
mv job/*.json $RESDIR/flights-nohyper-nocf
rm -rf job


# hyper-specialized
echo "benchmarking hyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-hyper-nocf-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-cf >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-hyper-nocf-run-$r.json"
done


# mv job folder
mkdir -p $RESDIR/flights-hyper-nocf
mv job/*.json $RESDIR/flights-hyper-nocf
rm -rf job

echo "done!"

