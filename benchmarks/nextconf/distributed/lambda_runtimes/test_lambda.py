import boto3

import json
import time
import cloudpickle
import base64
import math


NUM_RUNS = 4
TEST_LAMBDAS = ["test-python-native", "test-ctypes-python", "test-bash-python", "test-python"]
if __name__ == "__main__":
    # construct request
    lambda_client = boto3.client("lambda")

    # pickle function
    USE_SLOW = False
    def fast_test_function():
        r = 0
        for i in range(100000):
            r += i
        return float(r)
    def slow_test_function():
        import random

        INTERVAL = 1000
        circle_points = 0
        square_points = 0

        for i in range(INTERVAL ** 2):

            # Randomly generated x and y values from a uniform distribution [-1,1]
            rand_x = random.uniform(-1, 1)
            rand_y = random.uniform(-1, 1)

            # Distance between (x, y) and the origin
            origin_dist = rand_x ** 2 + rand_y ** 2

            # Checking if (x, y) lies inside the circle
            if origin_dist <= 1:
                circle_points += 1

            square_points += 1

            # Estimating value of pi,
            pi = 4 * circle_points / square_points

        return pi
    test_function = slow_test_function if USE_SLOW else fast_test_function

    encoded_func = base64.b64encode(cloudpickle.dumps(test_function)).decode("ascii")
    lambda_payload = json.dumps({"function": encoded_func})
    decoded_func = cloudpickle.loads(
        base64.b64decode(json.loads(lambda_payload)["function"])
    )
    expected_result = decoded_func()
    print(f"Expected: {expected_result}")
    def verify_result(x):
        if USE_SLOW:
            return 3 < x < 4
        else:
            return x == expected_result

    results = {}
    # bad names; this is what the different functions do:
    # test-python-native - pure python
    # test-ctypes-python - python opens c with ctypes
    # test-bash-python - bash opens python
    # test-python - cpp opens python
    for func in TEST_LAMBDAS:
        print(f"TESTING {func} -----")
        timings = []
        for _ in range(NUM_RUNS):
            # invoke
            REQUEST_TS = time.time()
            res = lambda_client.invoke(
                FunctionName=func,
                InvocationType="RequestResponse",
                Payload=bytes(lambda_payload, "utf-8"),
            )
            RETURN_TS = time.time()

            # decode payload
            payload_str = res["Payload"].read().decode("utf-8")
            print(payload_str)
            payload = json.loads(payload_str)
            # payload = json.load(res["Payload"])
            print(payload)
            body = payload["body"] if "body" in payload else json.loads(payload["data"])["body"]
            func_res = payload["func_res"] if "func_res" in payload else json.loads(payload["data"])["func_res"]

            print(f"PAYLOAD: {payload}")
            if not verify_result(func_res):
                print("FUNCTION didn't execute correctly!")

            # save result
            sf = 1e9 if func in ["test-python", "test-ctypes-python"] else 1
            MAIN_TS = sf * payload["MAIN_TS"] if "MAIN_TS" in payload else body["MAIN_TS"] if "MAIN_TS" in body else body["READY_TS"]
            timings.append(
                {
                    "REQUEST_TS": REQUEST_TS,
                    "MAIN_TS": float(MAIN_TS) / sf,
                    "READY_TS": float(body["READY_TS"]) / sf,
                    "PYTHON_READY_TS": float(body["PYTHON_READY_TS"]) / sf,
                    "FUNCTION_TS": float(body["FUNCTION_TS"]) / sf,
                    "RETURN_TS": RETURN_TS,
                }
            )
        results[func] = timings

    with open("results.json", "w") as f:
        json.dump(results, f)

    for f in results:
        print(f"{f} RESULTS ----------")
        for i, r in enumerate(results[f]):
            print(f"RUN {i}")
            s = r["REQUEST_TS"]
            slen = 20
            for k in r:
                padding = " " * (slen - len(k))
                print(f"{k}:{padding}{r[k]}\t{r[k]-s}")
