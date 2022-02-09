#!/usr/bin/env bash
# builds Tuplex within experimental container

export CC=gcc-10
export CXX=g++-10


CPU_COUNT=$(nproc)
echo "Building using $CPU_COUNT"

TUPLEX_DIR=/code

cd $TUPLEX_DIR && cd tuplex && mkdir -p build && \
cd build && \
cmake -DBUILD_WITH_AWS=OFF -DBUILD_NATIVE=ON -DPYTHON3_VERSION=3.6 -DLLVM_ROOT_DIR=/usr/lib/llvm-9 -DCMAKE_BUILD_TYPE=Release .. && \
make -j${CPU_COUNT} tuplex && \
cd dist/python/ && \
python3.6 setup.py install