import json
import subprocess
import os
import time
import cloudpickle
import base64
import sys


if __name__ == "__main__":
    READY_TS = time.time()
    event = json.loads(sys.argv[1])

    # run pickled function
    func = cloudpickle.loads(base64.b64decode(event["function"]))
    func_res = func()

    FUNCTION_TS = time.time()

    res = {
        "READY_TS": sys.argv[2],
        "PYTHON_READY_TS": READY_TS,
        "FUNCTION_TS": FUNCTION_TS,
    }
    print(json.dumps({"statusCode": 200, "func_res": func_res, "body": res}))
