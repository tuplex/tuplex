import glob
import argparse
import re

parser = argparse.ArgumentParser(description="compare outputs")
parser.add_argument(
    "--sure",
    action="store_true",
    help="this loads both output files in memory. are you sure you want to do this!?",
)
args = parser.parse_args()

assert args.sure, "are you sure you want to load all output files in memory?"

output_types = [
    {"desc": "spark(e2e)", "path": "pyspark_e2e/*.csv", "content": []},
    {"desc": "spark(cache)", "path": "pyspark_cached/*.csv", "content": []},
    {"desc": "sparksql(e2e)", "path": "pysparksql_e2e/*.csv", "content": []},
    {"desc": "sparksql(cache)", "path": "pysparksql_cached/*.csv", "content": []},
    {"desc": "tuplex(e2e)", "path": "tuplex_e2e/*.csv", "content": []},
    {"desc": "tuplex(cache)", "path": "tuplex_cached/*.csv", "content": []},
    {"desc": "dask(e2e)", "path": "dask_e2e/*.part", "content": []},
    {"desc": "dask(cache)", "path": "dask_cached/*.part", "content": []},
    {"desc": "weld", "path": "weld/*.csv", "content": []},
]

for o in output_types:
    o["path"] = f"/results/output/311/{o['path']}"

# gather all of the output rows
for case in output_types:
    print(f"***{case['desc'].upper()} FILES ***")
    for filename in glob.glob(case["path"]):
        print(filename)
        with open(filename, "r") as f:
            case["content"].extend(f.readlines()[1:])
    print()

# TODO: need to deal with an empty row for Tuplex
# for case in output_types:
# case["content"] = list(filter(lambda x: len(x) == 6, case["content"]))

# compare
print(" *** COMPARE LENGTHS *** ")
for case in output_types:
    print(f"{case['desc']} output size: " + str(len(case["content"])))

# for idx in range(len(output_types) - 1):
# assert len(output_types[idx]["content"]) == len(
# output_types[idx + 1]["content"]
# ), f"{output_types[idx]['desc']}, {output_types[idx+1]['desc']} not the same length!"


print("*** PREPROCESSING DATA***")
for case in output_types:
    case["content"].sort()

print(" *** COMPARING ***")
print("Elements in dask but not in spark")
for c in output_types[2]["content"]:
    if c not in output_types[0]["content"]:
        print(" - " + c[:-1])
print("Elements in spark but not in dask")
for c in output_types[0]["content"]:
    if c not in output_types[2]["content"]:
        print(" - " + c[:-1])
print("Elements in tuplex but not in spark")
for c in output_types[1]["content"]:
    if c not in output_types[0]["content"]:
        print(" - " + c[:-1])
print("Elements in spark but not in tuplex")
for c in output_types[0]["content"]:
    if c not in output_types[1]["content"]:
        print(" - " + c[:-1])


print("*** COMPARING DATA***")
for idx in range(len(output_types) - 1):
    for lidx, (line1, line2) in enumerate(
        zip(output_types[idx]["content"], output_types[idx + 1]["content"])
    ):
        prev_line1 = output_types[idx]["content"][lidx - 1] if lidx > 0 else "\n"
        prev_line2 = output_types[idx + 1]["content"][lidx - 1] if lidx > 0 else "\n"
        next_line1 = (
            output_types[idx]["content"][lidx + 1]
            if lidx < len(output_types[idx]["content"]) - 1
            else "\n"
        )
        next_line2 = (
            output_types[idx + 1]["content"][lidx + 1]
            if lidx < len(output_types[idx + 1]["content"]) - 1
            else "\n"
        )

        debug_str = (
            f"mismatched line {lidx} in {output_types[idx]['desc']}, {output_types[idx+1]['desc']}!\n"
            + line1
            + line2
            + "\n"
            + "previous lines:\n"
            + prev_line1
            + prev_line2
            + "next lines:\n"
            + next_line1
            + next_line2
        )
        assert line1 == line2, debug_str
