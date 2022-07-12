from tqdm import tqdm


# take out lineitem.tbl columns
QUANTITY_IDX = 4
EXTENDED_PRICE_IDX = 5
DISCOUNT_IDX = 6
SHIPDATE_IDX = 10

print('writing q6 specialized input file')
def convert(path, output_path):
    with open(path, 'r') as ip:
        with open(output_path, 'w') as fp:
            lines = ip.readlines()

            for line in tqdm(lines):
                row = line.split('|')
                shipdate = int(row[SHIPDATE_IDX].replace('-', ''))
                fp.write('{}|{}|{}|{}\n'.format(row[QUANTITY_IDX], row[EXTENDED_PRICE_IDX], row[DISCOUNT_IDX], shipdate))

convert('data/lineitem.tbl', 'data/lineitem_preprocessed.tbl')
# convert('/hot/data/tpch/sf-1/lineitem.tbl', 'lineitem_q6-sf1.tbl')
# convert('/hot/data/tpch/sf-10/lineitem.tbl', 'lineitem_q6-sf10.tbl')