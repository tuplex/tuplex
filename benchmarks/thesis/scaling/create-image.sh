#!/usr/bin/env bash

DOCKER_IMAGE_NAME=pyscaling


# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

echo ">>> creating docker image ${DOCKER_IMAGE_NAME}"

echo ">>> fetching data file (100MB) from S3"
mkdir -p .cache
if [ ! -f .cache/large100MB.csv ]; then
  echo "-- file not found, download"
  aws s3 cp s3://tuplex-public/data/zillow/large100MB.csv .cache/large100MB.csv
else
    echo "-- file cached, skip download step. Remove .cache dir to force redownload"
fi

if [ ! -f .cache/large100MB.csv ]; then
  echo "failed to download file, abort"
  exit 1
fi

echo ">>> fetching data file (100MB) from S3"
mkdir -p .cache
if [ ! -f .cache/large1GB.csv ]; then
  echo "-- file not found, download"
  aws s3 cp s3://tuplex-public/data/zillow/large1GB.csv .cache/large1GB.csv
else
    echo "-- file cached, skip download step. Remove .cache dir to force redownload"
fi

if [ ! -f .cache/large1GB.csv ]; then
  echo "failed to download file, abort"
  exit 1
fi

echo ">>> building image"
docker build -t ${DOCKER_IMAGE_NAME} .

echo "DONE."