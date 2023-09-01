#!/usr/bin/env bash
# invoke this here to build docker image, run etc.

# exit when any command fails
set -e

# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting

./create-image.sh

mkdir -p ./results

docker run -v $PWD/results:/results -e NUM_RUNS=5 -e MAX_PARALLELISM=2 pyscaling bash /code/benchmark.sh