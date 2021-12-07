#!/usr/bin/env bash
# script to install AWS C++ SDK and runtime

apt-get install -y libcurl4-openssl-dev libssl-dev uuid-dev zlib1g-dev libpulse-dev

# AWS SDK
# tag 1.9.142?
# => note in 1.9.134/135 there has been a renaming of cJSON symbols -> this requires linking/renaming. cf. https://github.com/aws/aws-sdk-cpp/commit/2848c4571c94b03bc558378440f091f2017ef7d3
# note for centos7 there's an issue with SSL. Either use aws sdk with -DBUILD_DEPS=ON/-DUSE_OPENSSL=OFF. or force -DUSE_OPENSSL=ON.
cd /tmp &&
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git &&
  cd aws-sdk-cpp && git checkout tags/1.9.133 && mkdir build && pushd build &&
  cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=/opt .. &&
  make -j32 &&
  make install &&
  popd &&
  cd - || echo "AWS SDK failed"

# C++ AWS runtime
git clone https://github.com/awslabs/aws-lambda-cpp.git &&
pushd aws-lambda-cpp &&
git fetch && git fetch --tags &&
git checkout v0.2.2 &&
mkdir build &&
cd build &&
cmake .. -DCMAKE_BUILD_TYPE=Release &&
make -j 8 && make install && popd
