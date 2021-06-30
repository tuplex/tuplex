#!/usr/bin/env bash

for ((i = 1; i <= 12; i++)); do

  fmt=$(printf %02d $i)

  mkdir -p data-${fmt}
  for ((j = 1; j <= i; j++)); do
    subfmt=$(printf %02d $j)
    cp data/flights_on_time_performance_2019_${subfmt}.csv data-${fmt}/
  done
  echo "created folder for ${fmt}"
done
echo "done."
