#!/usr/bin/env bash
# (c) L.Spiegelberg
# creates jar file to submit to spark (non-fat jar)

# because sbt first pulls in stuff, ignore the first build when timing
if [ -d target ]; then rm -rf target; fi

sbt package

rm -rf target

(time sbt package)

sbt assembly

# to execute the pure scala version, use
#java -jar scala/target/scala-2.11/ZillowScala-assembly-0.1.jar input output
