#!/usr/bin/env bash

# this is the benchmark file to run from within the docker container

# test for 1GB, 3GB, 5GB, 10GB
MEMORY_SIZES=(1000 3000 5000 10000)

# how often to repeat each setup
NRUNS=5

LAMBDA_NAME=pywren_runner

for MEMORY_SIZE in "${MEMORY_SIZES[@]}"; do

  echo "configuring Lambda to use ${MEMORY_SIZE}mb"
  aws lambda update-function-configuration --function-name ${LAMBDA_NAME} --memory-size ${MEMORY_SIZE}
  aws lambda wait function-updated --function-name ${LAMBDA_NAME}
  echo "Lambda ${LAMBDA_NAME} updated"

  
done

#aws lambda update-function-configuration --function-name pywren_runner --memory-size 1536
