#!/usr/bin/env bash

# this script helps to plot local data via the script running the versions within the docker container
# do not use paths with spaces...
INPUT_PATH="$1"
OUTPUT_PATH="$2"
me=`basename "$0"`

if [ -z "${INPUT_PATH}" ]; then
 echo "please specify input folder, usage: $me <input-folder> [output-folder]"
 exit 1
fi

if [ -z "${OUTPUT_PATH}" ]; then
  OUTPUT_PATH=$PWD/plots
fi

# convert to real paths...!
# if real path is not available on MacOS, install via brew install coreutils

function realpath() {
  # use python
  python3 -c "import os; print(os.path.abspath('$1'))"
}

INPUT_PATH="$(realpath ${INPUT_PATH})"
OUTPUT_PATH="$(realpath $OUTPUT_PATH)"

echo "-- Plotting using input data from $INPUT_PATH, storing results in $OUTPUT_PATH"

echo "-- Docker command: docker run -v \"$INPUT_PATH\":/work/results -v \"$OUTPUT_PATH\":/work/plots sigmod21/plot python3 /scripts/tuplex.py plot all --input-path /work/results --output-path /work/plots"
#docker run -v "$INPUT_PATH":/work/results -v "$OUTPUT_PATH":/work/plots sigmod21/plot python3 /scripts/tuplex.py plot all --input-path /work/results --output-path /work/plots

docker run -v "$INPUT_PATH":/work/results -v "$OUTPUT_PATH":/work/plots sigmod21/plot python3 /scripts/tuplex.py plot figure6 --input-path /work/results --output-path /work/plots


echo "Done."
echo " -- please find all plots in $OUTPUT_PATH"
