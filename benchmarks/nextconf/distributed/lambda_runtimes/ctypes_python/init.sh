#!/bin/bash
PYTHON_RESOURCES_DIR=../python38_resources
PACKAGE_DIR=test-ctypes-python
LIB_DIR=${PACKAGE_DIR}/lib
cp -r ${PYTHON_RESOURCES_DIR}/usr_lib/* ${PACKAGE_DIR}/
cp /opt/lib/libantlr4-runtime.so.4.8 ${LIB_DIR}/
cp /usr/lib64/libmagic.so.1 ${LIB_DIR}/
cp /usr/lib64/libcurl.so.4 ${LIB_DIR}/
cp /usr/lib64/libnghttp2.so.14 ${LIB_DIR}/
cp /usr/lib64/libidn2.so.0 ${LIB_DIR}/
cp /usr/lib64/libssh2.so.1 ${LIB_DIR}/
cp /usr/lib64/libldap-2.4.so.2 ${LIB_DIR}/
cp /usr/lib64/liblber-2.4.so.2 ${LIB_DIR}/
cp /usr/lib64/libuuid.so.1 ${LIB_DIR}/
cp /usr/lib64/libunistring.so.0 ${LIB_DIR}/
cp /usr/lib64/libsasl2.so.3 ${LIB_DIR}/
cp /usr/lib64/libssl3.so ${LIB_DIR}/
cp /usr/lib64/libsmime3.so ${LIB_DIR}/
cp /usr/lib64/libnss3.so ${LIB_DIR}/
