#!/usr/bin/env bash
#(c) L.Spiegelberg

echo "creating 100MB test file"
python3 sample_zillow.py data/large100MB.csv  100000000
echo "creating 1GB test file"
python3 sample_zillow.py data/large1GB.csv  1000000000
echo "creating 10GB test file"
python3 sample_zillow.py data/large10GB.csv 10000000000
