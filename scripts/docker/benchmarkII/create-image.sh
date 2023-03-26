#!/usr/bin/env bash
# (c) 2021 Tuplex contributors
# builds benchmark II image

# locate this file in order to execute docker within the script dir
pushd . > '/dev/null';
SCRIPT_PATH="${BASH_SOURCE[0]:-$0}";

while [ -h "$SCRIPT_PATH" ];
do
    cd "$( dirname -- "$SCRIPT_PATH"; )";
    SCRIPT_PATH="$( readlink -f -- "$SCRIPT_PATH"; )";
done

cd "$( dirname -- "$SCRIPT_PATH"; )" > '/dev/null';
SCRIPT_PATH="$( pwd; )";
popd  > '/dev/null';

set -e
set -o pipefail

while :; do
    case $1 in
        -u|--upload) UPLOAD="SET"
        ;;
        *) break
    esac
    shift
done

pushd $SCRIPT_PATH > /dev/null

# build benchmark docker image
docker build -t tuplex/benchmarkii .

# is upload set?
if [[ "${UPLOAD}" == 'SET' ]]; then
  docker login
  docker push tuplex/benchmarkii
fi

popd > /dev/null