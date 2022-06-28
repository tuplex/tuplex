#!/usr/bin/env bash
# (c) 2021 Tuplex team

# exact python versions AWS uses:
# Python 3.9 runtime --> Python 3.9.8
# Python 3.8 runtime --> Python 3.8.11
PYTHON3_VERSION=3.9.8
PYTHON3_MAJMIN=${PYTHON3_VERSION%.*}


# this script creates a deployable AWS Lambda zip package within the benchmarkII docker image

TUPLEX_DIR=/code
LOCAL_BUILD_FOLDER=$TUPLEX_DIR/tuplex/build-lambda
SRC_FOLDER=tuplex

# convert to absolute paths
get_abs_filename() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

CPU_COUNT=$(nproc)
echo "Building using $CPU_COUNT cores"
BUILD_WITH_CEREAL="${BUILD_WITH_CEREAL:-OFF}"
if [ $BUILD_WITH_CEREAL == 'ON' ]; then
  echo "Building with cereal support"
else
  BUILD_WITH_CEREAL=OFF
fi

LOCAL_BUILD_FOLDER=$(get_abs_filename $LOCAL_BUILD_FOLDER)
SRC_FOLDER=$(get_abs_filename $SRC_FOLDER)
echo "Tuplex source: $SRC_FOLDER"
echo "Building lambda in: $LOCAL_BUILD_FOLDER"

mkdir -p $LOCAL_BUILD_FOLDER

# only release works, b.c. of size restriction
BUILD_TYPE=Release

export LD_LIBRARY_PATH=/opt/lambda-python/lib:\$LD_LIBRARY_PATH && \
/opt/lambda-python/bin/python${PYTHON3_MAJMIN} -m pip install cloudpickle numpy && \
cd $LOCAL_BUILD_FOLDER && \\
cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_WITH_CEREAL=${BUILD_WITH_CEREAL} -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON -DPYTHON3_EXECUTABLE=/opt/lambda-python/bin/python${PYTHON3_MAJMIN} -DBOOST_ROOT=/opt/boost/python${PYTHON3_MAJMIN}/ -GNinja /code/tuplex && \\
cmake --build . --target tplxlam && \
python${PYTHON3_MAJMIN} /code/tuplex/python/zip_cc_runtime.py --input $LOCAL_BUILD_FOLDER/dist/bin/tplxlam --runtime $LOCAL_BUILD_FOLDER/dist/bin/tuplex_runtime.so --python /opt/lambda-python/bin/python${PYTHON3_MAJMIN} --output $LOCAL_BUILD_FOLDER/tplxlam.zip

echo "done."
