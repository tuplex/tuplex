#!/usr/bin/env bash

# invoke i.e. via nohup bash pgo-experiment.sh 2> pgo.stderr 1> pgo.stdout &
echo "Starting flights PGO specialization experiment"

INPUT_ROOT='/hot/data/flights_all/*.csv'

# test
#INPUT_ROOT='/hot/data/flights_all/*lights_on_time_performance_1999_01.csv'
RESULT_DIR='pgo-experiment-results'
PROF_DIR=$RESULT_DIR/profiles
LIBEXEC_DIR=$RESULT_DIR/libexec

echo "Step 0: prepping experiment, compiling runner..."
export PATH=/opt/llvm@6/bin:$PATH
clang++ -std=c++17 -msse4.2 -mcx16 -Wall -Wextra -O3 -march=native -DNDEBUG -o runner src/runner.cc -ldl
echo "done!"

mkdir -p $PROF_DIR
mkdir -p $LIBEXEC_DIR
echo "Step 1: Create profile generating shared-object"
SO_FILE=process_row_profiler.so
time clang++ -shared -fPIC -g -fprofile-instr-generate -fcoverage-mapping -o ${SO_FILE} src/process_row/process_row_orig.cc
mv $SO_FILE $LIBEXEC_DIR/

echo "Step 2: Running profiles for each file"
for file in `ls $INPUT_ROOT`; do
    echo "Profiling $(basename $file)"
    
    SO_FILE=$LIBEXEC_DIR/process_row_profiler.so
    PROF_FILE=profile_$(basename $file .csv).profraw
    LLVM_PROFILE_FILE="profile.profraw" ./runner -i $file -o test.csv -d $SO_FILE
    cp "profile.profraw" $PROF_DIR/$PROF_FILE
    
    # convert using llvm-merge
    echo "-- converting to llvm usable profile"
    llvm-profdata merge -output=code.profdata "profile.profraw"
    PROF_FILE=profile_$(basename $file .csv).profdata
    cp code.profdata $PROF_DIR/$PROF_FILE
    
    echo "-- compile specialized version using profile for file $(basename $file)"
    SO_OPTIMIZED=process_row_pgo_$(basename $file .csv).so
    time clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -fprofile-instr-use=code.profdata -o ${SO_OPTIMIZED} src/process_row/process_row_orig.cc
    mv $SO_OPTIMIZED $LIBEXEC_DIR
    echo "done, stored in ${SO_OPTIMIZED}"
done

echo "Step 3: Creating general shared obj, no PGO - just all optimizations on"
time clang++ -shared -fPIC -O3 -msse4.2 -mcx16 -march=native -DNDEBUG -o process_row_orig_general.so src/process_row/process_row_orig.cc
mv process_row_orig_general.so $LIBEXEC_DIR/

echo "Step 4: Running benchmark"
NUM_RUNS=5

OUTPUT_DIR='pgo-test-output'
LOG_DIR=$RESULT_DIR/logs
mkdir -p $LOG_DIR
for file in `ls $INPUT_ROOT`; do
    name=$(basename $file .csv)
    echo "general case for $name"
    for ((r = 1; r <= NUM_RUNS; r++)); do
        
        LOG="general-case-run-$r-$name.txt"
        LOG=$LOG_DIR/$LOG
        ./runner -i $file -o $OUTPUT_DIR -d $LIBEXEC_DIR/process_row_orig_general.so 2>&1 | tee $LOG
        echo "-- run $r"
    done
    
    echo "PGO optimized case for $name"
    for ((r = 1; r <= NUM_RUNS; r++)); do
        
        LOG="pgo-case-run-$r-$name.txt"
        LOG=$LOG_DIR/$LOG
        SO_OPTIMIZED=process_row_pgo_$(basename $file .csv).so
        ./runner -i $file -o $OUTPUT_DIR -d $LIBEXEC_DIR/$SO_OPTIMIZED 2>&1 | tee $LOG
        echo "-- run $r"
    done
done
