#!/usr/bin/env bash
# (c) 2021 Tuplex contributors
# builds benchmark image

while :; do
    case $1 in
        -u|--upload) UPLOAD="SET"
        ;;
        *) break
    esac
    shift
done

# build benchmark docker image
docker build -t tuplex/benchmark . || exit 1

# is upload set?
if [[ "${UPLOAD}" == 'SET' ]]; then
  docker login
  docker push tuplex/benchmark
fi
