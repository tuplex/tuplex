#!/usr/bin/env bash

echo "installing AWS SDK from source"
CPU_CORES=$(sysctl -n hw.physicalcpu)

cd /tmp &&
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git &&
  cd aws-sdk-cpp && git checkout tags/1.9.328 && mkdir build && pushd build &&
  cmake -DCMAKE_OSX_DEPLOYMENT_TARGET=10.13 -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" .. &&
  make -j${CPU_CORES} &&
  make install &&
  popd &&
  cd - || echo "AWS SDK failed"
