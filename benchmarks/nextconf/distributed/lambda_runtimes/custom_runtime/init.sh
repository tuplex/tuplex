#!/bin/bash
PYTHON_RESOURCES_DIR=../python38_resources
PACKAGE_DIR=test-bash-python
cp -r ${PYTHON_RESOURCES_DIR}/bin ${PACKAGE_DIR}
cp -r ${PYTHON_RESOURCES_DIR}/lib ${PACKAGE_DIR}
cp -r ${PYTHON_RESOURCES_DIR}/usr_lib/* ${PACKAGE_DIR}/lib/python3.8/site-packages/
cp -r ${PYTHON_RESOURCES_DIR}/lib64 ${PACKAGE_DIR}
