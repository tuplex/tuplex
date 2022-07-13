
## Incremental Exception Resolution Experiment

In this experiment, we run a sequence of six pipelines over the dirty zillow dataset.
- The first pipeline contains no ignore or resolver operations
- The final pipeline contains 5 unique ignore and resolver operations
- Each of the pipelines in between incrementally adds on an additional resolver until all are present for the final pipeline

We compare the following conditions, for a total of 8 experimental trials
- Plain vs Incremental Resolution
- Single vs Multi-threaded
- Merge in order vs Merge without order

In order to get 10GB of input data, replicate dirty zillow data 1460x (or use 1500x for simplicity).

### Setup
To replicate the original data, create the 10G files with the following settings:
```
python3 replicate-data.py -s 1460 -o data/zillow_dirty@10G.csv
```
Note that both files have the same number of rows, but the synthetic version is slightly larger.

### Running the benchmark
Use
`nohup perflock ./benchmark.sh -hwloc &`
