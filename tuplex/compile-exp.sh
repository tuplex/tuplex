#!/usr/bin/env bash

rm -rf build
echo "compiling everything without cereal"
mkdir build && cd build 
cmake -DBUILD_WITH_ORC=ON -DBUILD_WITH_AWS=ON -DCMAKE_BUILD_TYPE=Debug -DPYTHON3_VERSION=3.6 -DLLVM_ROOT_DIR=/usr/lib/llvm-9 ..
/usr/bin/time -v make -j128
cd ..
echo "done"
rm -rf build
echo "compiling with cereal"
mkdir build && cd build
cmake -DBUILD_WITH_ORC=ON -DBUILD_WITH_AWS=ON -DCMAKE_BUILD_TYPE=Debug -DPYTHON3_VERSION=3.6 -DLLVM_ROOT_DIR=/usr/lib/llvm-9 -DBUILD_WITH_CEREAL=ON ..
/usr/bin/time -v make -j128
cd ..
echo "done"

