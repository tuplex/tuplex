import glob
import argparse

parser = argparse.ArgumentParser(description="compare outputs")
parser.add_argument(
    "--sure",
    action="store_true",
    help="this loads both output files in memory. are you sure you want to do this!?",
)
args = parser.parse_args()

assert args.sure, "are you sure you want to load all output files in memory?"

output_types = [
    {
        "desc": "spark(native regex)",
        "path": "spark_output_spark_regex/*.csv",
        "content": [],
    },
    {"desc": "tuplex(regex)", "path": "./tuplex_output_regex/*.csv", "content": []},
]

# gather all of the output rows
for case in output_types:
    print(f"***{case['desc'].upper()} FILES ***")
    for filename in glob.glob(case["path"]):
        print(filename)
        with open(filename, "r") as f:
            case["content"].extend(f.readlines()[1:])
    print()

# compare
print(" *** COMPARE *** ")
for case in output_types:
    print(f"{case['desc']} output size: " + str(len(case["content"])))

for idx in range(len(output_types)-1):
    assert len(output_types[idx]["content"]) == len(
        output_types[idx + 1]["content"]
    ), f"{output_types[idx]['desc']}, {output_types[idx+1]['desc']} not the same length!"


for case in output_types:
    case["content"].sort()
for idx in range(len(output_types)-1):
    for lidx, (line1, line2) in enumerate(
        zip(output_types[idx]["content"], output_types[idx + 1]["content"])
    ):
        assert line1 == line2 or (line1 == '""\n' and line2 == '\n'), (
            f"mismatched line {lidx} in {output_types[idx]['desc']}, {output_types[idx+1]['desc']}!\n"
            + line1
            + line2
        )
