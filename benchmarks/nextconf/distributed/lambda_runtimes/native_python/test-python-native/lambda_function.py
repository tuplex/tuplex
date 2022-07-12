import json
import subprocess
import os
import time
import cloudpickle
import base64


def lambda_handler(event, context):
    READY_TS = time.time()

    # run pickled function
    func = cloudpickle.loads(base64.b64decode(event["function"]))
    func_res = func()

    FUNCTION_TS = time.time()

    res = {
        "READY_TS": READY_TS,
        "PYTHON_READY_TS": READY_TS,
        "FUNCTION_TS": FUNCTION_TS,
    }
    return {"statusCode": 200, "func_res": func_res, "body": res}
