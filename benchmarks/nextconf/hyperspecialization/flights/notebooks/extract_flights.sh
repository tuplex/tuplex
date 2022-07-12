
for file in `ls /disk/data/flights_gzip/*csv.gz`; do
    target=/hot/data/flights_all/$(basename $file .gz)
    gunzip -c $file > $target
    echo $target
done
