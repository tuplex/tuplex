This is a microbenchmark to compare the speeds of a single field regex extraction in Tuplex and PySpark. We created this microbenchmark because PySpark does not have a method to extract multiple match groups at once from a regex match, so we wanted to directly compare the speeds of the regex operation, without being affected by the fact that Tuplex extracts multiple fields with one call, while PySpark uses multiple `regexp_extract` call.
`benchmark.sh` is the benchmark script
`compare.py` is the validation script
`runpyspark.py`, `runtuplex.py` are the individual job scripts in PySpark and Tuplex
`tuplex_config.json` is the experiment configuration for Tuplex

On bbsn00, we ran over 74gb and got that (on average) Tuplex took 119.59s and PySpark took 356.93s for a 2.98x speedup.