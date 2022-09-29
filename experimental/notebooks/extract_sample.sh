#!/usr/bin/env bash

# unix is faster

mkdir -p .cache
N='100'
for file in `ls ~/Downloads/github_daily_sample/*.json.gz`; do
    echo $file
    NAME=$(basename $file)
    echo .cache/${NAME}
    DATA=$(head -n ${N} < <(cat $file | gunzip))
    #DATA=$(cat $file | gunzip | head -n 100)
    # > .cache/${NAME}
done
#!
