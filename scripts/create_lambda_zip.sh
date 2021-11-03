#!/usr/bin/env bash
# (c) 2021 Tuplex team

# this script creates a deployable AWS Lambda zip package using docker

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Executing buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd .. # go to root of repo

# start code here...

mkdir build-lambda
cd build-lambda


# end code here...
popd > /dev/null