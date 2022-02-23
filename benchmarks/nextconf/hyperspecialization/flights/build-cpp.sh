#echo "SHARED OBJECT BASIC"
#cd process_row
#g++ -shared -fPIC -O3 -o process_row_orig.so process_row_orig.cpp
#g++ -shared -fPIC -O3 -o process_row_constant.so process_row_constant.cpp
#g++ -shared -fPIC -O3 -o process_row_narrow.so process_row_narrow.cpp
#cd ..


export PATH=/opt/llvm@6/bin:$PATH

echo "Building shared objects"
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o process_row_orig.so src/process_row/process_row_orig.cc

echo "FINAL EXE"
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl
# invocation then via e.g. ./runner -i /disk/data/flights/flights_on_time_performance_2009_12.csv -o test.csv shared_obj
