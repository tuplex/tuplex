#!/usr/bin/env bash
# (c) 2017-2023 Tuplex contributors
# build CI images for different Python versions

while :; do
    case $1 in
        -u|--upload) UPLOAD="SET"
        ;;
        *) break
    esac
    shift
done

PYTHON_VERSIONS=(3.11.6 3.10.13 3.9.18 3.8.18)
PYTHON_VERSIONS=(3.10.13)

for python_version in "${PYTHON_VERSIONS[@]}"; do
  echo ">>> Building image for Python ${python_version}"
  py_majmin=${python_version%.*}
  TAG="tuplex/ci:${py_majmin}"
  echo "-- docker image tag: $TAG"

  # build tuplex/ci:3.x image
  docker build --build-arg="PYTHON_VERSION=${python_version}" --squash -t $TAG . || exit 1

  # is upload set?
  if [[ "${UPLOAD}" == 'SET' ]]; then
    docker login
    docker push $TAG
  fi
done