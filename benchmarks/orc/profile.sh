#!/usr/bin/env bash

perf record -F 99 -a -g -- $1
perf script > out.perf
/home/bgivertz/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
/home/bgivertz/FlameGraph/flamegraph.pl out.folded > $2

