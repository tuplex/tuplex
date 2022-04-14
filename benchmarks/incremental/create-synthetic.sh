#!/usr/bin/env bash

set -x

python3 synthesize-data.py --dataset-size $1 --output-path /hot/scratch/bgivertz/synthetic/synth0.csv --exceptions 0

for ((i = 1; i <= 9; i++)) do
  python3 synthesize-data.py --dataset-size $1 --output-path /hot/scratch/bgivertz/synthetic/synth$i.csv --exceptions 0.$i
done

python3 synthesize-data.py --dataset-size $1 --output-path /hot/scratch/bgivertz/synthetic/synth10.csv --exceptions 1
