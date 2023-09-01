#!/usr/bin/env bash
# this benchmark uses C++ versions to check whether aggregates may benefit from specialized code execution...

# build everything first



#export PATH=/opt/llvm@6/bin:$PATH
echo "Building shared object"
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o agg_weather_general.so src/agg_query/agg_general.cc
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o agg_weather_specialized.so src/agg_query/agg_specialized.cc

echo "FINAL EXE"
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl

#ROOT_PATH="/hot/data/flights_all/flights*.csv"
ROOT_PATH="/data/flights*.csv"

NUM_RUNS="${NUM_RUNS:-11}"

BENCHDIR=results-agg-experiment/logs
RESULT_DIR=results-agg-experiment/output
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
