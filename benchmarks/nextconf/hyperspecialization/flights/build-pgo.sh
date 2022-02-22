#!/usr/bin/env bash

# put in here the details
INPUT_PATH=/disk/data/flights/flights_on_time_performance_2009_12.csv
PROF_NAME=$(basename $INPUT_PATH)".profraw"

echo "Storing profile in $PROF_NAME"
mkdir -p profiles


export PATH=/opt/llvm@6/bin:$PATH

echo "Building shared object"

# build object using prof instructions
clang++ -shared -fPIC -g -fprofile-instr-generate -fcoverage-mapping -o process_row_orig_profile.so src/process_row/process_row_orig.cc

#clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o process_row_orig.so src/process_row/process_row_orig.cc

echo "FINAL EXE"
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl


# performing profiling run
echo "performing profiling run"
LLVM_PROFILE_FILE="${PROF_NAME}" ./runner -i ${INPUT_PATH} -o test.csv -d process_row_orig_profile.so

echo "merge to output"
llvm-profdata merge -output=code.profdata "${PROF_NAME}"

# can merge multiple runs together via https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization llvm-profdata merge,, yet use here a single run

# recompiling using profile
echo "recompiling using profile"
clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -fprofile-instr-use=code.profdata -o process_row_orig.so src/process_row/process_row_orig.cc

echo "running again using pgo optimized shared object"
./runner -i ${INPUT_PATH} -o test.csv -d process_row_orig.so

# invocation then via e.g. ./runner -i /disk/data/flights/flights_on_time_performance_2009_12.csv -o test.csv shared_obj
