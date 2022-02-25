#!/usr/bin/env bash
# version of the C++ experiment where sse instructions and vectorize are disabled for the queries
# configure variables here

# use only year 2013 to save time
ROOT_PATH="/data/flights*2013*.csv"
RES_ROOT_DIR=results-agg-experiment-nosse
NUM_RUNS="${NUM_RUNS:-11}"
SO_SPECIALIZED=agg_weather_specialized-nosse.so
SO_GENERAL=agg_weather_general-nosse.so

#export PATH=/opt/llvm@6/bin:$PATH
echo "Building shared object..."
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -fno-vectorize -mno-sse -DNDEBUG -o $SO_GENERAL src/agg_query/agg_general.cc
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -fno-vectorize -mno-sse -DNDEBUG -o $SO_SPECIALIZED src/agg_query/agg_specialized.cc

echo "FINAL EXE"
# use here vectorized instructions to make things fast...
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl

echo "Starting benchmark..."
BENCHDIR=$RES_ROOT_DIR/logs
RESULT_DIR=$RES_ROOT_DIR/output
mkdir -p $BENCHDIR
mkdir -p ${RESULT_DIR}

# perform optimized/sampled query (assuming ideal sampling!)
# shellcheck disable=SC2045
for file in $(ls $ROOT_PATH); do
  name=$(basename file)
  year=$(echo $file | egrep -o "[0-9]{4}")
  month=$(echo $file | egrep -o "_[0-9]{2}\." | tr -d "_.")
  yearmonth="${year}${month}"
  if (( yearmonth < 200306 )); then
    # special case
    echo "$name is special case"

    # run per file the query
    for ((r=1; r <= NUM_RUNS; r++)); do
      LOG=${BENCHDIR}/flights-specialized-run-$r-date-${yearmonth}.txt
      OUTPUT=$RESULT_DIR/out-specialized-${yearmonth}.csv
      ./runner -i ${file} -o $OUTPUT -d ./agg_weather_specialized.so 2>&1 | tee $LOG
    done

    for ((r=1; r <= NUM_RUNS; r++)); do
      LOG=${BENCHDIR}/flights-general-run-$r-date-${yearmonth}.txt
      OUTPUT=$RESULT_DIR/out-general-${yearmonth}.csv
      ./runner -i ${file} -o $OUTPUT -d ./agg_weather_general.so 2>&1 | tee $LOG
    done
  else
    # general case
    echo "$name is general case"
     for ((r=1; r <= NUM_RUNS; r++)); do
        LOG=${BENCHDIR}/flights-general-run-$r-date-${yearmonth}.txt
        OUTPUT=$RESULT_DIR/out-general-${yearmonth}.csv
        ./runner -i ${file} -o $OUTPUT -d ./agg_weather_general.so 2>&1 | tee $LOG

        # mimick specilaized by copying, no difference.
        cp $LOG ${BENCHDIR}/flights-specialized-run-$r-date-${yearmonth}.txt
        cp -r $OUTPUT $RESULT_DIR/out-specialized-${yearmonth}.csv
    done
  fi
done

echo "Done."