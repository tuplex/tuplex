#!/usr/bin/env bash

CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

DEST_PATH=$1
CPU_CORES=$(sysctl -n hw.physicalcpu)

# PYTHON_VERSION="$(basename -- $PYTHON_EXECUTABLE)"
# PY_INCLUDE_DIR=`$PYTHON_EXE -c "import sysconfig; print(sysconfig.get_path('include'))"`
#
# export C_INCLUDE_PATH=$PY_INCLUDE_DIR:$C_INCLUDE_PATH
# export CPLUS_INCLUDE_PATH=$PY_INCLUDE_DIR:$CPLUS_INCLUDE_PATH


# build incl. boost python
cd /tmp || exit
wget https://boostorg.jfrog.io/artifactory/main/release/1.75.0/source/boost_1_75_0.tar.gz
tar xf boost_1_75_0.tar.gz
cd /tmp/boost_1_75_0 || exit

# cf. https://stackoverflow.com/questions/28830653/build-boost-with-multiple-python-versions

# i.e.
# tools/build/src/user-config.jam
# using python : 2.7 : /opt/python/cp27-cp27mu/bin/python : /opt/python/cp27-cp27mu/include/python2.7 : /opt/python/cp27-cp27mu/lib ;
# using python : 3.5 : /opt/python/cp35-cp35m/bin/python : /opt/python/cp35-cp35m/include/python3.5m : /opt/python/cp35-cp35m/lib ;
# using python : 3.6 : /opt/python/cp36-cp36m/bin/python : /opt/python/cp36-cp36m/include/python3.6m : /opt/python/cp36-cp36m/lib ;
# using python : 3.7 : /opt/python/cp37-cp37m/bin/python : /opt/python/cp37-cp37m/include/python3.7m : /opt/python/cp37-cp37m/lib ;
# python=2.7,3.5,3.6,3.7

# copy the file to adjust
touch tools/build/src/user-config.jam
cp $CWD/user-config.jam tools/build/src/user-config.jam
./bootstrap.sh --prefix=${DEST_PATH} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time"
./b2 python="3.6,3.7,3.8,3.9" cxxflags="-fPIC" link=static -j "$CPU_CORES"
./b2 python="3.6,3.7,3.8,3.9" cxxflags="-fPIC" link=static install
