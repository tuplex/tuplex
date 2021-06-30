#!/usr/bin/env bash
# (c) 2021 Tuplex contributors
# builds notebook image

while :; do
    case $1 in
        -u|--upload) UPLOAD="SET"
        ;;
        *) break
    esac
    shift
done

# build benchmark docker image
# copy from scripts to current dir because docker doesn't understand files
# outside the build context
docker build -t tuplex/tuplex . || exit 1

# is upload set?
if [[ "${UPLOAD}" == 'SET' ]]; then
  docker login
  docker push tuplex/tuplex
fi
