## Zillow (clean) experiment
This is the experiment where the target is to preprocess scraped zillow data. The pipeline has no joins nor aggregates, but makes heavy use of string processing. Holds query Z2 -> condos for sale.

### Setup
To create the experimental data file, run `./preprocess-data.py` first. This will create the `10G` file.

### Benchmarks

### Validation
To be sure that all frameworks are installed and the pipelines yield the same results, you can run `./validate.sh`.


### Notes:
In the clean data, if bathroom extraction is pushed down (i.e. becomes first row), exceptions actually occur. Yet, all rows are filtered out in the current pipeline mode.