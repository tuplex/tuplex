
## Dirty Zillow data experiment

In this experiment, we compare the following quantities
- Run on dirty zillow data (no ignores, no resolvers) => i.e. overhead of tracking exceptions
- Run on dirty zillow data (with ignores, with resolvers) => overhead + resolution
- Run on dirty zillow data (with custom cleaning logic, i.e. do all the custom cleaning cases like studio apartments etc.)


In order to get 10GB of input data, replicate dirty zillow data 1460x (or use 1500x for simplicity).

### Setup
To replicate the original data, create the 10G files with the following settings:
```
python3 replicate-data.py -s 1460 -o data/zillow_dirty@10G.csv
python3 preprocess-data.py -s 1460 -m synth -o data/zillow_dirty_synthetic@10G.csv
```
Note that both files have the same number of rows, but the synthetic version is slightly larger.

### Running the benchmark
Use
`nohup perflock ./benchmark.sh -hwloc &`
