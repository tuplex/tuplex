from tqdm import tqdm
import argparse
from collections import defaultdict

parser = argparse.ArgumentParser(description="Preprocess data for TPC-H Q19")
parser.add_argument(
    "--weld", action="store_true", help="whether to convert strings to ints",
)
args = parser.parse_args()

# take out lineitem.tbl columns
L_PARTKEY_IDX = 1
L_QUANTITY_IDX = 4
L_EXTENDEDPRICE_IDX = 5
L_DISCOUNT_IDX = 6
L_SHIPINSTRUCT_IDX = 13
L_SHIPMODE_IDX = 14
L_COLUMNS = [
    L_PARTKEY_IDX,
    L_QUANTITY_IDX,
    L_EXTENDEDPRICE_IDX,
    L_DISCOUNT_IDX,
    L_SHIPINSTRUCT_IDX,
    L_SHIPMODE_IDX,
]
L_STRING_COLUMNS = [L_SHIPINSTRUCT_IDX, L_SHIPMODE_IDX]

# take out part.tbl columns
P_PARTKEY_IDX = 0
P_BRAND_IDX = 3
P_SIZE_IDX = 5
P_CONTAINER_IDX = 6
P_COLUMNS = [
    P_PARTKEY_IDX,
    P_BRAND_IDX,
    P_SIZE_IDX,
    P_CONTAINER_IDX,
]
P_STRING_COLUMNS = [P_BRAND_IDX, P_CONTAINER_IDX]


def convert(path, output_path, columns, string_columns):
    format_str = "|".join(["{}" for _ in range(len(columns))])
    format_str = f"{format_str}\n"

    weld_counters = {c: 0 for c in string_columns}
    weld_strings = {c: {} for c in string_columns}
    with open(path, "r") as ip:
        with open(output_path, "w") as fp:
            lines = ip.readlines()

            for line in tqdm(lines):
                row = line.split("|")

                # preprocess the string columns
                if args.weld:
                    for col in string_columns:
                        if row[col] not in weld_strings[col]:
                            if row[col][:6] == "Brand#":
                                weld_strings[col][row[col]] = int(row[col][6:])
                            else:
                                weld_strings[col][row[col]] = weld_counters[col]
                                weld_counters[col] += 1
                        row[col] = weld_strings[col][row[col]]

                fp.write(format_str.format(*[row[i] for i in columns]))
    return weld_strings if args.weld else None


def l_convert(path, output_path):
    r = convert(path, output_path, L_COLUMNS, L_STRING_COLUMNS)
    print("LINEITEM DICTIONARIES")
    print(r)


def p_convert(path, output_path):
    r = convert(path, output_path, P_COLUMNS, P_STRING_COLUMNS)
    print("PART DICTIONARIES")
    print(r)


print("writing q19 specialized lineitem file")
l_convert("data/lineitem.tbl", "data/lineitem_preprocessed.tbl")
# l_convert('/hot/data/tpch/sf-1/lineitem.tbl', 'lineitem_q19-sf1.tbl')
# l_convert('/hot/data/tpch/sf-10/lineitem.tbl', 'lineitem_q19-sf10.tbl')


print("writing q19 specialized part file")
p_convert("data/part.tbl", "data/part_preprocessed.tbl")
# p_convert('/hot/data/tpch/sf-1/part.tbl', 'part_q19-sf1.tbl')
# p_convert('/hot/data/tpch/sf-10/part.tbl', 'part_q19-sf10.tbl')
