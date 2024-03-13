#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# collects script invocations required to produce graphs for flight experiments


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


# Validating results
echo ">>> Validating python baseline vs. tuplex (no hyper, no promo)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex/github/nohyper/output"

echo ">>> Validating python baseline vs. tuplex (hyper enabled, no promo)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex/github/hyper-noopt/output"

echo ">>> Validating python baseline vs. tuplex (hyper enabled, promo enabled)"
python3 validate.py "./local-exp/python-baseline/github/output" "./local-exp/tuplex/github/hyper/output"

echo "validation succeeded!"