#!/bin/bash
NAME="test-ctypes-python"
BUILD_DIR=../../../../build-py38-haswell/dist/lib
OBJ="libctypes-tuplex.so"
TUPLEX_OBJ="tuplex_runtime.so"
cd ${NAME}
rm "${NAME}.zip"

#cp "${BUILD_DIR}/libctypes-tuplex.so" .
mkdir -p lib
cp "${BUILD_DIR}/${OBJ}" lib/
cp "${BUILD_DIR}/${TUPLEX_OBJ}" lib/

# try to hack elf file
#cd lib
#patchelf --remove-rpath "${OBJ}"
#patchelf --force-rpath --set-rpath ./ "${OBJ}"
#cd ..

zip -r "${NAME}.zip" ./
aws lambda update-function-code --function-name ${NAME} --zip-file "fileb://${NAME}.zip"
