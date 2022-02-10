#!/usr/bin/env bash
# need to run benchmark via docker because of python versions etc.

# current dir, make benchmark results dir
mkdir -p results/
# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Running LAMBDA in AWS Region: ${AWS_DEFAULT_REGION}"

./configure.py && \
docker build -t pywren . && \
docker run -it -v $CWD/results:/results -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} pywren bash /work/benchmark.sh