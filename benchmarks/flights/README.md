# Running flight benchmark experiment


Dask:

pip3 install "dask[complete]"

python3 rundask.py --path data/2019_1_full.csv


Spark:

export PYSPARK_PYTHON=python3 && spark-submit --driver-memory 16g runpyspark.py --path data/2019_1_full.csv
