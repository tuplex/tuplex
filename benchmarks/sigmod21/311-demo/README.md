To invoke on bbsn00:


Tuplex (should be 1.5s - 1.8s):

Spark (should be 70s):

spark-submit --master "local[64]" --driver-memory 256g runpyspark.py --path /hot/data/311/311_preprocessed.csv --sql-mode

(the other version is way too slow)


sudo apt-get install asciinema


