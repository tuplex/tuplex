echo "SHARED OBJECT BASIC"
cd process_row
g++ -shared -fPIC -O3 -o process_row_orig.so process_row_orig.cpp
g++ -shared -fPIC -O3 -o process_row_constant.so process_row_constant.cpp
g++ -shared -fPIC -O3 -o process_row_narrow.so process_row_narrow.cpp
cd ..

echo "FINAL EXE"
g++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o basic basic.cpp -ldl
