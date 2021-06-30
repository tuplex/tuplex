#!/usr/bin/env bash
# (c) L.Spiegelberg 2021
# validate that the dirty zillow pipeline produces same number of outpout tuples in custom and tuplex mode
[ -d custom_output ] && rm -rf custom_output
[ -d custom_output ] && rm -rf resolve_output

echo "running custom logic:"
python3 runtuplex.py --path data/zillow_dirty.csv -m custom --output-path custom_output --resolve-in-order
echo "running tuplex logic:"
python3 runtuplex.py --path data/zillow_dirty.csv -m resolve --output-path resolve_output --resolve-in-order

echo "custom logic line count: $(wc -l custom_output/part0.csv)"
echo "tuplex logic line count: $(wc -l resolve_output/part0.csv)"

# diff only works on single file
echo "diff result (no output = fine): "
diff custom_output/part0.csv resolve_output/part0.csv

python3 compare_folders.py custom_output resolve_output

rm -rf custom_output
rm -rf resolve_output
