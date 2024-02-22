#!/usr/bin/env bash
# (c) 2024 L.Spiegelberg
# collects script invocations required to produce graphs for flight experiments


# Run python baseline experiment
#python3 runtuplex-new.py --mode python --input-pattern "/hot/data/flights_all/*.csv" --output-path "./local-exp/python-baseline/flights/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/python-baseline/flights/results.ndjson"

# --> has memory issue in interpreter path
# Run Tuplex without hyperspecialization (original Tuplex)
python3 runtuplex-new.py --mode tuplex --no-hyper --no-promo --no-cf --input-pattern "/hot/data/flights_all/*.csv" --output-path "./local-exp/tuplex/flights/nohyper/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/tuplex/flights/nohyper/results.ndjson"

# --> to be run
# Run Tuplex with hyperspecialization enabled (Viton), but disable filter-promotion and constant-folding
python3 runtuplex-new.py --mode tuplex --no-promo --no-cf --input-pattern "/hot/data/flights_all/*.csv" --output-path "./local-exp/tuplex/flights/hyper-noopt/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/tuplex/flights/hyper-noopt/results.ndjson"

# --> to be run
# Run Tuplex with hyperspecializaiton enabled (Viton), and enable filter-promotion and constant-folding
python3 runtuplex-new.py --mode tuplex --input-pattern "/hot/data/flights_all/*.csv" --output-path "./local-exp/tuplex/flights/hyper/output" --scratch-dir "./local-exp/scratch" --result-path "./local-exp/tuplex/flights/hyper/results.ndjson"
