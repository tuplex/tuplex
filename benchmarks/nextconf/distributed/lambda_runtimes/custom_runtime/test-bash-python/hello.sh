function handler () {
    EVENT_DATA=$1
    READY_TS=$(date +%s.%N)

    export PYTHONHOME=$LAMBDA_TASK_ROOT
    ./bin/python3 lambda_function.py "$EVENT_DATA" "$READY_TS"
}
