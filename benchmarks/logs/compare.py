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

OUTPUT_DIR="/results/output/weblogs"
output_types = [
    {"desc": "tuplex(split)", "path": "tuplex_split_output/*.csv", "content": []},
    {"desc": "tuplex(strip)", "path": "tuplex_strip_output/*.csv", "content": []},
    {"desc": "tuplex(regex)", "path": "tuplex_regex_output/*.csv", "content": []},
    {"desc": "tuplex(split_regex)", "path": "tuplex_splitregex_output/*.csv", "content": []},
    {"desc": "sparksql(regex)", "path": "sparksql_regex_output/*.csv", "content": []},
    {"desc": "sparksql(split)", "path": "sparksql_split_output/*.csv", "content": []},
    {"desc": "spark(regex)", "path": "pyspark_regex_output/*.csv", "content": []},
    {"desc": "spark(strip)", "path": "pyspark_strip_output/*.csv", "content": []},
    {"desc": "dask(regex)", "path": "dask_regex_output/*.part", "content": []},
    {"desc": "dask(strip)", "path": "dask_strip_output/*.part", "content": []},
]

# add prefix to output paths
for o in output_types:
    o["path"] = f"{OUTPUT_DIR}/{o['path']}"

# gather all of the output rows
for case in output_types:
    print(f"***{case['desc'].upper()} FILES ***")
    for filename in glob.glob(case["path"]):
        print(filename)
        with open(filename, "r") as f:
            case["content"].extend(f.readlines()[1:])
    print()

# compare
print(" *** COMPARE LENGTHS *** ")
for case in output_types:
    print(f"{case['desc']} output size: " + str(len(case["content"])))

for idx in range(len(output_types) - 1):
    assert len(output_types[idx]["content"]) == len(
        output_types[idx + 1]["content"]
    ), f"{output_types[idx]['desc']}, {output_types[idx+1]['desc']} not the same length!"


print("*** PREPROCESSING DATA***")
for case in output_types:  # remove the randomly generated strings
    case["content"] = list(
        map(lambda x: re.sub("/~[^/]+/(.*),", "/~/\\1,", x), case["content"])
    )
for case in output_types:
    case["content"].sort()

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
