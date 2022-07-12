#!/usr/bin/env bash
# (c) L.Spiegelberg 2021
# this script creates a docker image helpful for developing the Lambda container on a non-Linux platform like MacOS


# copy Ubuntu 20.04 dependency files in...
cp ../../ubuntu2004/install_reqs.sh .

# build docker container
docker build -t tuplex/lambda .
