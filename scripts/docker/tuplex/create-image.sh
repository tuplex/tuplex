#!/usr/bin/env bash
# (c) 2022 Tuplex contributors
# builds notebook image

while :; do
    case $1 in
        -u|--upload) UPLOAD="SET"
        ;;
        *) break
    esac
    shift
done

# copy data from examples folder and include it in docker image
cp ../../../examples/*.ipynb .
cp ../../../examples/*.py .
cp -R ../../../examples/sample_data .


# build benchmark docker image
# copy from scripts to current dir because docker doesn't understand files
# outside the build context
docker build -t tuplex/tuplex:0.3.6dev -f Dockerfile . || exit 1

# is upload set?
if [[ "${UPLOAD}" == 'SET' ]]; then
  docker login
  docker push tuplex/tuplex:0.3.6dev
fi
