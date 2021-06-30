#!/usr/bin/env bash
# script to install AWS C++ SDK and runtime

apt-get install -y libcurl4-openssl-dev libssl-dev uuid-dev zlib1g-dev libpulse-dev

#install AWS C++ sdk
git clone https://github.com/aws/aws-sdk-cpp.git &&
cd aws-sdk-cpp && mkdir build && pushd build &&
cmake -DCMAKE_BUILD_TYPE=Release .. &&
make -j16 &&
make install &&
popd

# C++ AWS runtime
git clone https://github.com/awslabs/aws-lambda-cpp.git &&
pushd aws-lambda-cpp &&
git fetch && git fetch --tags &&
git checkout v0.2.2 &&
mkdir build &&
cd build &&
cmake .. -DCMAKE_BUILD_TYPE=Release &&
make -j 8 && make install && popd
