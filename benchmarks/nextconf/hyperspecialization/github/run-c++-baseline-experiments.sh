#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# collects script invocations required to produce graphs for flight experiments

# Writing C++ baselines (or equivalent JSON_EXTRACT in SQL) is cumbersome, multiple iterations necessary till pipeline works with files having different schemas.
# Easiest way was to load everything into a Python object, and then simply decode as is.
# However, mapping is not trivial: What parts of the object and which parts won't?
# As solution, speculate and use different techniques.

set -e pipefail

cd c++_baseline && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$(nproc) && cd ../..

PROG=./c++_baseline/build/cc_github
INPUT_PATTERN='/hot/data/github_daily/*.json'
${PROG} --help

echo ">>> Running C++ baseline (best)"
${PROG} -m "best" --input-pattern "${INPUT_PATTERN}" --output-path "./local-exp/c++-baseline/github/best/output" --result-path "./local-exp/c++-baseline/github/best_results.csv"

echo ">>> Running C++ baseline (condensed C-struct)"
${PROG} -m "cstruct" --input-pattern "${INPUT_PATTERN}" --output-path "./local-exp/c++-baseline/github/cstruct/output" --result-path "./local-exp/c++-baseline/github/cstruct_results.csv"

echo ">>> Running C++ baseline (cJSON)"
${PROG} -m "cjson" --input-pattern "${INPUT_PATTERN}" --output-path "./local-exp/c++-baseline/github/cjson/output" --result-path "./local-exp/c++-baseline/github/cjson_results.csv"

## Validating results
echo ">>> Validating python baseline vs. C++ (best)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/c++-baseline/github/best/output"

echo ">>> Validating python baseline vs. C++ (C-struct)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/c++-baseline/github/cstruct/output"

echo ">>> Validating python baseline vs. C++ (cJSON)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/c++-baseline/github/cjson/output"

echo "validation succeeded!"


## run a couple runs here
#NUM_RUNS=3
#
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  echo "-- RUN ${r}/${NUM_RUNS}"
#
#done
