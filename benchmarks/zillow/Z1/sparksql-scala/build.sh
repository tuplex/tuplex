#!/usr/bin/env bash
# (c) L.Spiegelberg
# creates jar file to submit to spark (non-fat jar)

# because sbt first pulls in stuff, ignore the first build when timing
if [ -d target ]; then rm -rf target; fi

sbt package

rm -rf target

(time sbt package)

# to execute the spark version, use
#spark-submit --class tuplex.exp.spark.ZillowClean target/scala-2.11/zillowscala_2.11-0.1.jar input_path output_path