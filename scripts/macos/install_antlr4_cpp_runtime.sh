#!/usr/bin/env bash
# This is a script to install the antlr4 runtime.

set -euxo pipefail

# Specify here target location.
PREFIX=${PREFIX:-/usr/local}

# If antlr4 exists already, skip.
[ -d "antlr4" ] && exit 0

if [ -d "${PREFIX}/include/antlr4-runtime" ]; then
  echo "skip antlr4 runtime install, directory already exists"
  exit 0
fi

# use arm64 or x86_64.
ARCH=${ARCH:-x86_64}

echo ">>> Building for architecture: ${ARCH}"

# if macOS is 10.x -> use this as minimum
MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=10.13"

MACOS_VERSION=$(sw_vers -productVersion)
echo "-- processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

MACOS_VERSION_MAJOR=`echo $MACOS_VERSION | cut -d . -f1`

if [ "$MACOS_VERSION_MAJOR" -ge 11 ]; then
    echo "-- Newer MacOS detected (>=11.0), using more recent base target."
    echo "-- Using minimum target ${MACOS_VERSION_MAJOR}.0"
    MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOS_VERSION_MAJOR}.0"
else
    # keep as is
    echo "defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

# Ensure $PREFIX/{lib,include} exist.
mkdir -p $PREFIX/include
mkdir -p $PREFIX/lib

# with sed, modify deploy to add osx_deployment_target
git clone https://github.com/antlr/antlr4.git \
&& cd antlr4 && cd runtime &&  git fetch --all --tags \
&& git checkout tags/4.13.1 -b 4.13.1 && cd Cpp/ \
&& sed -i '' "s/cmake ./cmake . ${MINIMUM_TARGET}/g" deploy-macos.sh \
&& sed -i '' "s/CMAKE_OSX_ARCHITECTURES=\"arm64; x86_64\"/CMAKE_OSX_ARCHITECTURES=\"${ARCH}\"/g" deploy-macos.sh \
&& cat deploy-macos.sh \
&& ./deploy-macos.sh \
&& unzip -l antlr4-cpp-runtime-macos.zip && unzip antlr4-cpp-runtime-macos.zip \
&& cd lib && cp -R * $PREFIX/lib/ && cd .. \
&& mv antlr4-runtime $PREFIX/include/ \
&& echo "ANTLR4 Cpp runtime installed to $PREFIX."

# execute copy command (fix for delocate wheel)
ls -l $PREFIX/include
ls -l $PREFIX/lib

cp $PREFIX/lib/libantlr4-runtime.dylib /Users/runner/work/tuplex/tuplex/libantlr4-runtime.dylib || echo "cp failed."

exit 0
