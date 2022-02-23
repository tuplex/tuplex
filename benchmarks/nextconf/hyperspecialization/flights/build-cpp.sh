#echo "SHARED OBJECT BASIC"
#cd process_row
#g++ -shared -fPIC -O3 -o process_row_orig.so process_row_orig.cpp
#g++ -shared -fPIC -O3 -o process_row_constant.so process_row_constant.cpp
#g++ -shared -fPIC -O3 -o process_row_narrow.so process_row_narrow.cpp
#cd ..


export PATH=/opt/llvm@6/bin:$PATH

echo "Building shared object"
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o agg_weather_general.so src/agg_query/agg_general.cc
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o agg_weather_specialized.so src/agg_query/agg_specialized.cc

echo "FINAL EXE"
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl


ROOT_PATH="/hot/data/flights_all/flights*.csv"


# shellcheck disable=SC2045
for file in $(ls $ROOT_PATH); do
  name=$(basename file)
  year=$(echo $file | egrep -o "[0-9]{4}")
  month=$(echo $file | egrep -o "_[0-9]{2}\." | tr -d "_.")
  yearmonth="${year}${month}"
  if (( yearmonth < 200306 )); then
    # special case
    echo "$name is special case"
  else
    # general case
    echo "$name is general case"
  fi

done
# invocation then via e.g. ./runner -i /disk/data/flights/flights_on_time_performance_2009_12.csv -o test.csv shared_obj
