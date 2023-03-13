import csv
import os
import glob
import logging
import json
import pandas as pd
import json
import csv
import io
import cloudpickle
import functools
from tqdm import tqdm
from udfs import *

# reference implementation to carry out the same compute as in Tuplex (to determine correct row counts)

def log_setup():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("reference_implementation.log"),
            logging.StreamHandler()
        ]
    )

ref_output_dir = 'reference_output'


def csv2tuples(path, header=True):
    with open(path, 'r') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        rows = [tuple(row) for row in reader]
        if header:
            return rows[1:]
        else:
            return rows


def to_bool(value):
    valid = {'true': True, 't': True, 'yes': True, 'y': True, 'false': False, 'f': False, 'no': False, 'n': False, }

    if isinstance(value, bool):
        return value

    if not isinstance(value, str):
        raise ValueError('invalid literal for boolean. Not a string.')

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)


def parse(s):
    assert isinstance(s, str)
    # try to parse s as different types
    if s in ['', ]:
        return None
    try:
        return to_bool(s.strip())
    except:
        pass
    try:
        return int(s.strip())
    except:
        pass
    try:
        return float(s.strip())
    except:
        pass
    try:
        return json.loads(s.strip())
    except:
        pass
    # return as string, final option remaining...
    return s


# helper row object to allow fancy integer and column based string access within UDFs!
class Row:
    def __init__(self, data, columns=None):
        assert (isinstance(data, (tuple, list)))
        assert (isinstance(columns, (tuple, list)) or columns is None)
        self.data = tuple(data)
        self.columns = tuple(columns[:len(data)]) if columns is not None else None

    def __getitem__(self, key):
        # check for int also works for bool!
        if isinstance(key, int):
            return self.data[key]
        # getitem either gets a key or slice object
        elif isinstance(key, slice):
            return self.data[key.start:key.stop:key.step]
        elif isinstance(key, str):
            if self.columns is None:
                raise KeyError("no columns defined, can't access column '{}'".format(key))
            elif key not in self.columns:
                raise KeyError("could not find column column '{}'".format(key))
            return self.data[self.columns.index(key)]
        else:
            raise IndexError()

    def __repr__(self):
        if self.columns:
            if len(self.columns) < len(self.data):
                self.columns = self.columns + [None] * (len(self.data) - len(self.columns))
            return '(' + ','.join(['{}={}'.format(c, d) for c, d in zip(self.columns, self.data)]) + ')'
        else:
            return '(' + ','.join(['{}'.format(d) for d in self.data]) + ')'


# recursive expansion of Row objects potentially present in data.
def expand_row(x):
    # Note: need to use here type construction, because isinstance fails for dict input when checking for list
    if hasattr(type(x), '__iter__') and not isinstance(x, str):
        if type(x) is tuple:
            return tuple([expand_row(el) for el in x])
        elif type(x) is list:
            return [expand_row(el) for el in x]
        elif type(x) is dict:
            return {expand_row(key): expand_row(val) for key, val in x.items()}
        else:
            raise TypeError("custom sequence type used, can't convert to data representation")
    return x.data if isinstance(x, Row) else x


def result_to_row(res, columns=None):
    # convert result to row object, i.e. deal with unpacking etc.
    # is result a dict?
    if type(res) is dict:
        # are all keys strings? If so, then unpack!
        # else, keep it as dict return object!
        if all(map(lambda k: type(k) == str, res.keys())):
            # columns become keys, values
            columns = tuple(res.keys())
            data = tuple(map(lambda k: res[k], columns))
            return Row(data, columns)

    # is it a row object?
    # => convert to tuple!
    r = expand_row(res)

    if type(r) is not tuple:
        r = (r,)
    else:
        if len(r) == 0:
            r = ((),)  # special case, empty tuple

    return Row(r, columns)


def apply_func(f, row):
    if len(row.data) != 1:
        # check how many positional arguments function has.
        # if not one, expand row into multi args!
        nargs = f.__code__.co_argcount
        if nargs != 1:
            return f(*tuple([row[i] for i in range(nargs)]))
        else:
            return f(row)
    else:
        # unwrap single element tuples.
        return f(row.data[0])

def file_line_count(path):
    def _count_generator(reader):
        b = reader(1024 * 1024)
        while b:
            yield b
            b = reader(1024 * 1024)

    with open(path, 'rb') as fp:
        c_generator = _count_generator(fp.raw.read)
        count = sum(buffer.count(b'\n') for buffer in c_generator)
        return count


def pipeline_stage_0(input_row, parse_cells=False):
    res = {'outputRows': []}
    for _ in range(1):
        row = input_row
        try:
            f = extract_feature_vector
            call_res = apply_func(f, row)
            if row.columns and 'features' in row.columns:
                col_idx = row.columns.index('features')
                tmp = list(row.data)
                tmp[col_idx] = expand_row(call_res)
                row.data = tuple(tmp)
            else:
                row.columns = row.columns + ('features',) if row.columns is not None else tuple(
                    [None] * len(row.data)) + ('features',)
                row.data = row.data + result_to_row(call_res).data
        except Exception as e:
            res['exception'] = e
            res['exceptionOperatorID'] = 100001
            res['inputRow'] = input_row
            return res
        try:
            f = fill_in_delays
            call_res = apply_func(f, row)
            row = result_to_row(call_res, row.columns)
        except Exception as e:
            res['exception'] = e
            res['exceptionOperatorID'] = 100002
            res['inputRow'] = input_row
            return res
        try:
            f = lambda row: 2000 <= row['year'] <= 2005
            call_res = apply_func(f, row)
            if not call_res:
                continue
        except Exception as e:
            res['exception'] = e
            res['exceptionOperatorID'] = 100003
            res['inputRow'] = input_row
            return res
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        w.writerow(row.data)
        csvSerialized = buf.getvalue()
        res['outputRows'] += [csvSerialized]
        res['outputColumns'] = row.columns
    return res
def process_path(path):
    logging.info(f"Processing {path}")
    os.makedirs(ref_output_dir, exist_ok=True)
    os.makedirs(os.path.join(ref_output_dir, 'csv'), exist_ok=True)
    output_path = os.path.join(ref_output_dir, 'csv', os.path.basename(path) + ".ref.csv")

    header = True
    columns = []
    num_input_rows = 0
    num_output_rows = 0
    num_lines = file_line_count(path)
    logging.info(f"File has {num_lines} lines")
    path_entry = {}
    with open(output_path, 'w') as out_f:
        with open(path, 'r') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            for csv_row in tqdm(reader, total=num_lines):
                if header:
                    columns = tuple(csv_row)
                    header = False
                    continue
                row = tuple(csv_row)

                # perform type conversions
                data = tuple([parse(el) for el in row])

                row = Row(data, columns)
                pip_res = pipeline_stage_0(row)
                if len(pip_res['outputRows']) > 0:
                    num_output_rows += 1
                    out_f.write(pip_res['outputRows'][0])
                # perform pipeline on top...
                num_input_rows += 1
    path_entry['input_rows'] = num_input_rows
    path_entry['output_rows'] = num_output_rows
    path_entry['input_path'] = path
    path_entry['output_path'] = output_path
    logging.info(f"num_input_rows={num_input_rows}, num_output_rows={num_output_rows}")
    logging.info(f"output written to {output_path}")
    return path_entry

if __name__ == '__main__':
    log_setup()

    input_pattern = '/hot/data/flights_all/flights*.csv'

    logging.info('Reference implementation for Flights query')
    paths = sorted(glob.glob(input_pattern))
    logging.info(f"found {len(paths)} paths to process")

    # now process each path & check number of output rows
    rows = []
    for path in paths:
        ref_entry = process_path(path)
        rows.append(ref_entry)
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(ref_output_dir, 'stats.csv'), index=None)
    logging.info('Processing done.')

