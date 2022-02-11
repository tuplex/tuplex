#!/usr/bin/env bash
# (c) L.Spiegelberg 2021
# validate that the dirty zillow pipeline produces same number of output tuples in plain and incremental mode
[ -d plain_output ] && rm -rf plain_output
[ -d incremental_output ] && rm -rf incremental_output

echo "running in order without incremental resolution:"
python3 runtuplex.py --path data/zillow_dirty.csv --output-path plain_output --resolve-in-order
echo "running in order with incremental resolution:"
python3 runtuplex.py --path data/zillow_dirty.csv --incremental-resolution --output-path incremental_output --resolve-in-order

python3 compare_folders.py plain_output incremental_output

rm -rf plain_output
rm -rf incremental_output

echo "running no order without incremental resolution:"
python3 runtuplex.py --path data/zillow_dirty.csv --output-path plain_output
echo "running no order with incremental resolution:"
python3 runtuplex.py --path data/zillow_dirty.csv --incremental-resolution --output-path incremental_output

python3 compare_folders.py plain_output incremental_output

rm -rf plain_output
rm -rf incremental_output