import json
import subprocess
import os
import time
import cloudpickle
import base64
import ctypes
import pathlib

c_lib = None
MAIN_TS = 0


def init_lambda():
    global c_lib
    global MAIN_TS
    MAIN_TS = time.time()
    # load in our shared library
    LIB_NAME = "libctypes-tuplex.so"

    # try 1
    libdir = pathlib.Path().absolute() / "lib"
    libname = libdir / LIB_NAME

    # try 2
    # libname = os.path.abspath(LIB_NAME)
    # print(f"libname 2: {libname}")

    # check for access
    #print(f"access: {os.access(libname, os.R_OK)}")

    # try opening it
    #import subprocess
    #import platform 
    #print("env")
    #print(os.environ)
    #print(platform.processor())
    #all_info = subprocess.check_output('cat /proc/cpuinfo', shell=True).strip()
    #print(all_info)

    c_lib = ctypes.PyDLL(libname)
    c_lib.py_lambda_handler.argtypes=[ctypes.c_char_p]
    c_lib.py_lambda_handler.restype = ctypes.py_object # return python string

    # call init
    c_lib.global_init()


def lambda_handler(event, context):
    global c_lib
    global MAIN_TS
    if c_lib is None:
        init_lambda()
    payload = json.dumps(event)
    res = c_lib.py_lambda_handler(payload.encode("utf-8"))
    return {"statusCode": 200, "MAIN_TS": MAIN_TS, "data": res}


# TODO: I don't think this is actually ever used
if __name__ == "__main__":
    init_lambda()  # only call on cold start
