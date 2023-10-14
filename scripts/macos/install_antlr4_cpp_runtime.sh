#!/usr/bin/env bash
# this is a script to install the antlr4 runtime

# specify here target location
PREFIX=/usr/local

# if antlr4 exists already, skip
[ -d "antlr4" ] && exit 0

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

# with sed, modify deploy to add osx_deployment_target
git clone https://github.com/antlr/antlr4.git \
&& cd antlr4 && cd runtime &&  git fetch --all --tags \
&& git checkout tags/4.13.1 -b 4.13.1 && cd Cpp/ \
&& sed -i '' "s/cmake ./cmake . ${MINIMUM_TARGET}/g" deploy-macos.sh \
&& cat deploy-macos.sh \
&& ./deploy-macos.sh \
&& unzip -l antlr4-cpp-runtime-macos.zip && unzip antlr4-cpp-runtime-macos.zip \
&& cd lib && cp -R * $PREFIX/lib/ && cd .. \
&& mv antlr4-runtime $PREFIX/include/ \
&& echo "ANTLR4 Cpp runtime installed to $PREFIX"

# execute copy command (fix for delocate wheel)
ls -l /usr/local/include
ls -l /usr/local/lib

cp lib/libantlr4-runtime.dylib /Users/runner/work/tuplex/tuplex/libantlr4-runtime.dylib

exit 0
