#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# collects script invocations required to produce graphs for flight experiments

# cmake -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles" -DBUILD_WITH_CEREAL=ON -DSKIP_AWS_TESTS=OFF -DBUILD_WITH_ORC=OFF -DBUILD_WITH_AWS=ON -DPython3_EXECUTABLE=/home/leonhards/.pyenv/shims/python3.9 -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=/opt/llvm-9 ..
# cmake -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles" -DBUILD_WITH_CEREAL=ON -DSKIP_AWS_TESTS=OFF -DBUILD_WITH_ORC=OFF -DBUILD_WITH_AWS=ON -DPython3_EXECUTABLE=/home/leonhards/.pyenv/shims/python3.9 -DAWS_S3_TEST_BUCKET='tuplex-test' -DLLVM_ROOT_DIR=/opt/llvm-16.0.6 ..

# helper function
run_benchmarks() {
  # Run python baseline experiment
  python3 runtuplex-new.py --mode python --input-pattern "/hot/data/github_daily/*.json" --output-path "./local-exp/python-baseline/github/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/python-baseline/github/results.ndjson"

  # --> has memory issue in interpreter path
  # Run Tuplex without hyperspecialization (original Tuplex)
  python3 runtuplex-new.py --mode tuplex --no-hyper --no-promo --input-pattern "/hot/data/github_daily/*.json" --output-path "./local-exp/tuplex/github/nohyper/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/tuplex/github/nohyper/results.ndjson"

  # --> to be run
  # Run Tuplex with hyperspecialization enabled (Viton), but disable filter-promotion and constant-folding
  python3 runtuplex-new.py --mode tuplex --no-promo --input-pattern "/hot/data/github_daily/*.json" --output-path "./local-exp/tuplex/github/hyper-noopt/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/tuplex/github/hyper-noopt/results.ndjson"

  # --> to be run
  # Run Tuplex with hyperspecializaiton enabled (Viton), and enable filter-promotion and constant-folding
  python3 runtuplex-new.py --mode tuplex --input-pattern "/hot/data/github_daily/*.json" --output-path "./local-exp/tuplex/github/hyper/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/tuplex/github/hyper/results.ndjson"
}

# run all benchmarks once
echo ">>> Running all configs once in order to validate results."
run_benchmarks

# Validating results
echo ">>> Validating python baseline vs. tuplex (no hyper, no promo)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex/github/nohyper/output"

echo ">>> Validating python baseline vs. tuplex (hyper enabled, no promo)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex/github/hyper-noopt/output"

echo ">>> Validating python baseline vs. tuplex (hyper enabled, promo enabled)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex/github/hyper/output"

echo "validation succeeded!"


# run a couple runs
NUM_RUNS=3

for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "-- RUN ${r}/${NUM_RUNS}"
  run_benchmarks
done
