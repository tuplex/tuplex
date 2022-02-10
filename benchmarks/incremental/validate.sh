#!/usr/bin/env bash
# (c) L.Spiegelberg 2021
# validate that the dirty zillow pipeline produces same number of outpout tuples in custom and tuplex mode
[ -d plain_output ] && rm -rf plain_output
[ -d incremental_output ] && rm -rf incremental_output

echo "running without incremental resolution:"
python3 runtuplex.py --path data/zillow_dirty.csv --output-path plain_output --resolve-in-order
echo "running with incremental resolution:"
python3 runtuplex.py --path data/zillow_dirty.csv --incremental-resolution --output-path incremental_output --resolve-in-order

echo "custom logic line count: $(wc -l plain_output/part0.csv)"
echo "tuplex logic line count: $(wc -l incremental_output/part0.csv)"

# diff only works on single file
echo "diff result (no output = fine): "
diff plain_output/part0.csv incremental_output/part0.csv

python3 compare_folders.py plain_output incremental_output

rm -rf plain_output
rm -rf incremental_output
