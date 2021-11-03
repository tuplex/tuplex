## Orc Benchmarks
The following four scripts are used to benchmark Orc reading and writing against
Tuplex's CSV implementation:
1. `runcsv-read-single.py`
2. `runcsv-write.py`
3. `runorc-read-single.py`
4. `runorc-write.py`

## Datasets
In order to benchmark Orc against CSV, first import a CSV and Orc file
that contain the exact same dataset and place them into the `data` folder.
One method for doing so would be first copying or generating a CSV file
from another benchmark, for example `zillow/Z1` and then using an [`orc-tool`](https://orc.apache.org/docs/cpp-tools.html) to generate an Orc version as well.

## Writing
The following command can be run to benchmark Orc writng:
```
python3 runorc-write.py --path data/<dataset>.csv --output-path tuplex_output/
```

The following command can be run to benchmark CSV writing:
```
python3 runcsv-write.py --path data/<dataset>.csv --output-path tuplex_output/
```

## Reading
The following command can be run to benchmark Orc reading:
```
python3 runorc-read-single.py --path data/<dataset>.orc
```

The following command can be run to benchmark CSV reading:
```
python3 runcsv-read-single.py --path data/<dataset>.csv
```