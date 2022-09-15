#!/usr/bin/env bash

echo "installing AWS SDK from source"
CPU_CORES=$(sysctl -n hw.physicalcpu)

# if macOS is 10.x -> use this as minimum
MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=10.13"

MACOS_VERSION=$(sw_vers -productVersion)
echo "processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }
Used as such:

if [ $(version $VAR) -ge $(version "11.0.0") ]; then
    echo "Newer MacOS detected, all good."
    MINIMUM_TARGET=""
else
    # keep as is
    echo "defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

cd /tmp &&
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git &&
  cd aws-sdk-cpp && git checkout tags/1.9.328 && mkdir build && pushd build &&
  cmake ${MINIMUM_TARGET} -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" .. &&
  make -j${CPU_CORES} &&
  make install &&
  popd &&
  cd - || echo "AWS SDK failed"
