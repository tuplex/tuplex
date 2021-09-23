import json
import subprocess
import os
import time
import cloudpickle
import base64


def lambda_handler(func_str):
    # run pickled function
    func = cloudpickle.loads(base64.b64decode(func_str))
    func_res = func()
    return func_res
