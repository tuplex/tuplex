#!/usr/bin/env bash

/usr/bin/time --verbose python3.6 runtuplex.py --path /hot/data/311/311_preprocessed.csv

echo "Input file stats:"
du -sh /hot/data/311/311_preprocessed.csv

# cached, b.c. wc is so slow
echo "lines: 197,628,001 /hot/data/311/311_preprocessed.csv"
#echo "#lines: $(wc -l /hot/data/311/311_preprocessed.csv)" 
