#!/usr/bin/env bash
# builds Tuplex within experimental container

# specify here which compiler to use
export CC=gcc
export CXX=g++

set -e
set -o pipefail

CPU_COUNT=$(nproc)
echo "Building using $CPU_COUNT cores"
BUILD_WITH_CEREAL="${BUILD_WITH_CEREAL:-OFF}"
if [ $BUILD_WITH_CEREAL == 'ON' ]; then
  echo "Building with cereal support"
else
  BUILD_WITH_CEREAL=OFF
fi
TUPLEX_DIR=/code
AWS_S3_TEST_BUCKET=tuplex-test

# make sure cloudpickle 2.x is not installed
python3.9 -m pip uninstall cloudpickle -y
python3.9 -m pip install 'cloudpickle<2.0.0'

cd $TUPLEX_DIR && cd tuplex && mkdir -p build && \
cd build && rm -rf -- * && \
cmake -DBUILD_WITH_CEREAL=${BUILD_WITH_CEREAL} -DBUILD_WITH_AWS=ON -DBUILD_NATIVE=ON -DPYTHON3_VERSION=3.9 -DSKIP_AWS_TESTS=OFF -DBUILD_WITH_ORC=ON -DAWS_S3_TEST_BUCKET=${AWS_S3_TEST_BUCKET} -DLLVM_ROOT_DIR=/opt/llvm-9.0 -DCMAKE_BUILD_TYPE=Release .. && \
make -j${CPU_COUNT} tuplex && \
cd dist/python/ && \
tar cvzf tuplex.tar.gz * && \
python3.9 setup.py install
