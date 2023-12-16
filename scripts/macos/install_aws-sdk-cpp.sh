#!/usr/bin/env bash

# check if dir exists (i.e. restored from cache, then skip)
if [ -d "/usr/local/include/aws" ]; then
  echo ">> Skip aws-sdk-cpp compile from source, already exists."
  exit 0
fi

echo ">> installing AWS SDK from source"
CPU_CORES=$(sysctl -n hw.physicalcpu)

# if macOS is 10.x -> use this as minimum
MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=10.13"

MACOS_VERSION=$(sw_vers -productVersion)
echo "-- processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

MACOS_VERSION_MAJOR=$(echo "$MACOS_VERSION" | awk -F \. {'print $1'})
if (( $MACOS_VERSION_MAJOR >= 11 )); then
    echo "-- Newer MacOS detected (>=11.0), using more recent base target."
    echo "-- Using minimum target ${MACOS_VERSION_MAJOR}.0"
    MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOS_VERSION_MAJOR}.0"
else
    # keep as is
    echo "defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

cd /tmp &&
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git &&
  cd aws-sdk-cpp && git checkout tags/1.11.164 && mkdir build && pushd build &&
  cmake ${MINIMUM_TARGET} -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=17 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" .. &&
  make -j${CPU_CORES} &&
  make install &&
  popd &&
  cd - || echo ">> error: AWS SDK failed"

