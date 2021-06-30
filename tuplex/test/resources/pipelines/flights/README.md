## Flights data
This directory contains several small sample files
to test with tuplex

- `flights_on_time_performance_2019_01.10ksample.csv` 9999 rows + header which will lead to normal case/exceptional case split
- `flights_on_time_performance_2019_01.sample.csv` super small sample which should work per default
- `flights_on_time_performance_2019_01.sample.csv` this will produce sampling errors, so in order to process the pipeline the interpreter is required...
- `GlobalAirportDatabase.txt` and `L_CARRIER_HISTORY.csv` are the lookup tables used in the query.