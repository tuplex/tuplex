#!/usr/bin/env bash
# this benchmark uses C++ versions to check whether aggregates may benefit from specialized code execution...

# build everything first


export PATH=/opt/llvm@6/bin:$PATH

echo "Building shared objects"
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o process_row_orig.so src/process_row/process_row_orig.cc

echo "FINAL EXE"
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl