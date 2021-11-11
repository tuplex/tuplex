echo "building shared libraries"
g++ -shared -fPIC -O3 -o cpp_shared/count_um_list.so cpp_shared/count_um_list.cc
g++ -shared -fPIC -O3 -o cpp_shared/count_m_list.so cpp_shared/count_m_list.cc

echo "building benchmark runner"
g++ -g -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o count_unique_bench count_unique_bench.cc -ldl
