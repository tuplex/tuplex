#!/usr/bin/env bash
# builds Tuplex within experimental container

# specify here which compiler to use
export CC=gcc
export CXX=g++

CPU_COUNT=$(nproc)
echo "Building using $CPU_COUNT"

TUPLEX_DIR=/code
AWS_S3_TEST_BUCKET=tuplex-test

cd $TUPLEX_DIR && cd tuplex && mkdir -p build && \
cd build && rm -rf -- * && \
cmake -DBUILD_WITH_AWS=ON -DBUILD_NATIVE=ON -DPYTHON3_VERSION=3.9 -DSKIP_AWS_TESTS=OFF -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET=${AWS_S3_TEST_BUCKET} -DLLVM_ROOT_DIR=/opt/llvm-9.0 -DCMAKE_BUILD_TYPE=Release .. && \
make -j${CPU_COUNT} tuplex && \
cd dist/python/ && \
python3.6 setup.py install