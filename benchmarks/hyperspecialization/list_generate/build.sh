echo "building main binaries"
g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o stdumap_int_int count_unique_bench.cc -DCOUNT_UM_INT -I/usr/include/python3.8 -lpython3.8 -ldl
g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o stdmap_int_int count_unique_bench.cc -DCOUNT_M_INT -I/usr/include/python3.8 -lpython3.8 -ldl
g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o stdmap_string_int count_unique_bench.cc -DCOUNT_M_STRING -I/usr/include/python3.8 -lpython3.8 -ldl
g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o stdumap_string_int count_unique_bench.cc -DCOUNT_UM_STRING -I/usr/include/python3.8 -lpython3.8 -ldl
g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o fixed_range_nopydict count_unique_bench.cc -DFIXED_RANGE -I/usr/include/python3.8 -lpython3.8 -ldl
g++ -g -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o tuplex_int_nopydict count_unique_bench.cc -DTUPLEX_INT -I/usr/include/python3.8 -lpython3.8 -ldl

echo "building shared libs"

cd tuplex-hashmap
./build.sh
cd ..

cd specialized-hashmap
./build.sh
cd ..

cd cpp_shared
./build.sh
cd ..

echo "done"
