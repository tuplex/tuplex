full sample of flight data, produced via
for file in `ls /hot/data/flights_all/*.csv`; do echo `basename $file`.sample; head -n 1001 $file > `basename $file`.sample; tail -n 1000 $file >> `basename $file`.sample; done
