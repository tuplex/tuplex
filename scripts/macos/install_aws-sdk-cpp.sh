#!/usr/bin/env bash

AWSSDK_CPP_VERSION=1.11.164

echo ">> installing AWS SDK ${AWSSDK_CPP_VERSION} from source"
CPU_COUNT=$(sysctl -n hw.physicalcpu)
echo "-- building with ${CPU_COUNT} cores"

# if macOS is 10.x -> use this as minimum
MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=10.13"

MACOS_VERSION=$(sw_vers -productVersion)
echo "-- processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

MACOS_VERSION_MAJOR=${MACOS_VERSION%.*}
if [ $MACOS_VERSION_MAJOR -ge 11 ]; then
    echo "-- Newer MacOS detected (>=11.0), using more recent base target."
    MACOS_VERSION_MAJOR=${MACOS_VERSION%.*}
    echo "-- Using minimum target ${MACOS_VERSION_MAJOR}.0"
    MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOS_VERSION_MAJOR}.0"
else
    # keep as is
    echo "defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

cd /tmp \
  && git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git \
  && cd aws-sdk-cpp && git checkout tags/${AWSSDK_CPP_VERSION} && sed -i 's/int ret = Z_NULL;/int ret = static_cast<int>(Z_NULL);/g' src/aws-cpp-sdk-core/source/client/RequestCompression.cpp && mkdir build && cd build \
  && cmake ${MINIMUM_TARGET} -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=17 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=${PREFIX} .. \
  && make -j ${CPU_COUNT} \
  && make install &&
  popd &&
  cd - || echo ">> error: AWS SDK failed"

