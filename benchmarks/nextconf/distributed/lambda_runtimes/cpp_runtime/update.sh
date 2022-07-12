#!/bin/bash
NAME="test-python"
BUILD_DIR=../../../build-py38-haswell
BUILD_LOC=experiments/LambdaPythonVsCustom/cpp_runtime
aws lambda update-function-code --function-name ${NAME} --zip-file "fileb://${BUILD_DIR}/${BUILD_LOC}/${NAME}.zip"
