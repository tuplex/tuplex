import json
import csv
import io
import cloudpickle
import functools

# helper row object to allow fancy integer and column based string access within UDFs!
class Row:
    def __init__(self, data, columns=None):
        assert(isinstance(data, (tuple, list)))
        assert(isinstance(columns, (tuple, list)) or columns is None)
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
    def get(self, key, default=None):
        if isinstance(key, int):
            if key < 0:
                key += len(self.data)
            if key < 0 or key >= len(self.data):
                return default
            return self.data[key]
        if isinstance(key, str):
            if not self.columns:
                return default
            try:
                key = self.columns.index(key)
                return self.data[key]
            except ValueError:
                return default
        return default
# recursive expansion of Row objects potentially present in data.
def expand_row(x):
    # Note: need to use here type construction, because isinstance fails for dict input when checking for list
    if hasattr(type(x), '__iter__') and not isinstance(x, str):
        if type(x) is tuple:
            return tuple([expand_row(el) for el in x])
        elif type(x) is list:
            return [expand_row(el) for el in x]
        elif type(x) is dict:
            return {expand_row(key) : expand_row(val) for key, val in x.items()}
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
            r = ((),) # special case, empty tuple
    
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

class NullGet:
    def __init__(self, row):
        self.row = row
        

    def __getitem__(self, key):
        if not isinstance(key, (int, slice, str)):
            raise IndexError()
        try:
            return self.row[key]
        except (KeyError, IndexError):
            return None
        
    def __repr__(self):
        return repr(self.row)
    
    @property
    def data(self):
        return self.row.data
    
    @data.setter
    def data(self, value):
        self.row.data = value
        
    @property
    def columns(self):
        return self.row.columns
    
    @columns.setter
    def columns(self, value):
        self.row.columns = value

@functools.cache
def decodeUDF(n):
	if 0 == n:
		code = b'\x80\x05\x95\x64\x02\x00\x00\x00\x00\x00\x00\x8c\x17\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x2e\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x94\x8c\x0d\x5f\x62\x75\x69\x6c\x74\x69\x6e\x5f\x74\x79\x70\x65\x94\x93\x94\x8c\x0a\x4c\x61\x6d\x62\x64\x61\x54\x79\x70\x65\x94\x85\x94\x52\x94\x28\x68\x02\x8c\x08\x43\x6f\x64\x65\x54\x79\x70\x65\x94\x85\x94\x52\x94\x28\x4b\x01\x4b\x00\x4b\x00\x4b\x01\x4b\x04\x4b\x53\x43\x16\x74\x00\x7c\x00\x64\x01\x19\x00\xa0\x01\x64\x02\xa1\x01\x64\x03\x19\x00\x83\x01\x53\x00\x94\x28\x4e\x8c\x0a\x63\x72\x65\x61\x74\x65\x64\x5f\x61\x74\x94\x8c\x01\x2d\x94\x4b\x00\x74\x94\x8c\x03\x69\x6e\x74\x94\x8c\x05\x73\x70\x6c\x69\x74\x94\x86\x94\x8c\x01\x78\x94\x85\x94\x8c\x66\x2f\x68\x6f\x6d\x65\x2f\x6c\x65\x6f\x6e\x68\x61\x72\x64\x73\x2f\x70\x72\x6f\x6a\x65\x63\x74\x73\x2f\x74\x75\x70\x6c\x65\x78\x2d\x70\x75\x62\x6c\x69\x63\x2f\x62\x65\x6e\x63\x68\x6d\x61\x72\x6b\x73\x2f\x6e\x65\x78\x74\x63\x6f\x6e\x66\x2f\x68\x79\x70\x65\x72\x73\x70\x65\x63\x69\x61\x6c\x69\x7a\x61\x74\x69\x6f\x6e\x2f\x67\x69\x74\x68\x75\x62\x2f\x72\x75\x6e\x74\x75\x70\x6c\x65\x78\x2d\x6e\x65\x77\x2e\x70\x79\x94\x8c\x08\x3c\x6c\x61\x6d\x62\x64\x61\x3e\x94\x4b\xa4\x43\x00\x94\x29\x29\x74\x94\x52\x94\x7d\x94\x28\x8c\x0b\x5f\x5f\x70\x61\x63\x6b\x61\x67\x65\x5f\x5f\x94\x4e\x8c\x08\x5f\x5f\x6e\x61\x6d\x65\x5f\x5f\x94\x8c\x08\x5f\x5f\x6d\x61\x69\x6e\x5f\x5f\x94\x8c\x08\x5f\x5f\x66\x69\x6c\x65\x5f\x5f\x94\x68\x12\x75\x4e\x4e\x4e\x74\x94\x52\x94\x8c\x1c\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x2e\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x5f\x66\x61\x73\x74\x94\x8c\x12\x5f\x66\x75\x6e\x63\x74\x69\x6f\x6e\x5f\x73\x65\x74\x73\x74\x61\x74\x65\x94\x93\x94\x68\x1d\x7d\x94\x7d\x94\x28\x68\x19\x68\x13\x8c\x0c\x5f\x5f\x71\x75\x61\x6c\x6e\x61\x6d\x65\x5f\x5f\x94\x8c\x21\x67\x69\x74\x68\x75\x62\x5f\x70\x69\x70\x65\x6c\x69\x6e\x65\x2e\x3c\x6c\x6f\x63\x61\x6c\x73\x3e\x2e\x3c\x6c\x61\x6d\x62\x64\x61\x3e\x94\x8c\x0f\x5f\x5f\x61\x6e\x6e\x6f\x74\x61\x74\x69\x6f\x6e\x73\x5f\x5f\x94\x7d\x94\x8c\x0e\x5f\x5f\x6b\x77\x64\x65\x66\x61\x75\x6c\x74\x73\x5f\x5f\x94\x4e\x8c\x0c\x5f\x5f\x64\x65\x66\x61\x75\x6c\x74\x73\x5f\x5f\x94\x4e\x8c\x0a\x5f\x5f\x6d\x6f\x64\x75\x6c\x65\x5f\x5f\x94\x68\x1a\x8c\x07\x5f\x5f\x64\x6f\x63\x5f\x5f\x94\x4e\x8c\x0b\x5f\x5f\x63\x6c\x6f\x73\x75\x72\x65\x5f\x5f\x94\x4e\x8c\x17\x5f\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x5f\x73\x75\x62\x6d\x6f\x64\x75\x6c\x65\x73\x94\x5d\x94\x8c\x0b\x5f\x5f\x67\x6c\x6f\x62\x61\x6c\x73\x5f\x5f\x94\x7d\x94\x75\x86\x94\x86\x52\x30\x2e'
		f = cloudpickle.loads(code)
		return f
	if 1 == n:
		code = b'\x80\x05\x95\x26\x02\x00\x00\x00\x00\x00\x00\x8c\x17\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x2e\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x94\x8c\x0d\x5f\x62\x75\x69\x6c\x74\x69\x6e\x5f\x74\x79\x70\x65\x94\x93\x94\x8c\x0a\x4c\x61\x6d\x62\x64\x61\x54\x79\x70\x65\x94\x85\x94\x52\x94\x28\x68\x02\x8c\x08\x43\x6f\x64\x65\x54\x79\x70\x65\x94\x85\x94\x52\x94\x28\x4b\x01\x4b\x00\x4b\x00\x4b\x01\x4b\x03\x4b\x43\x43\x10\x7c\x00\x64\x01\x19\x00\x7c\x00\x64\x02\x19\x00\x66\x02\x53\x00\x94\x4e\x4b\x00\x4b\x08\x87\x94\x29\x8c\x01\x74\x94\x85\x94\x8c\x08\x3c\x73\x74\x72\x69\x6e\x67\x3e\x94\x8c\x08\x3c\x6c\x61\x6d\x62\x64\x61\x3e\x94\x4b\x01\x43\x00\x94\x29\x29\x74\x94\x52\x94\x7d\x94\x28\x8c\x0b\x5f\x5f\x70\x61\x63\x6b\x61\x67\x65\x5f\x5f\x94\x4e\x8c\x08\x5f\x5f\x6e\x61\x6d\x65\x5f\x5f\x94\x8c\x08\x5f\x5f\x6d\x61\x69\x6e\x5f\x5f\x94\x8c\x08\x5f\x5f\x66\x69\x6c\x65\x5f\x5f\x94\x8c\x66\x2f\x68\x6f\x6d\x65\x2f\x6c\x65\x6f\x6e\x68\x61\x72\x64\x73\x2f\x70\x72\x6f\x6a\x65\x63\x74\x73\x2f\x74\x75\x70\x6c\x65\x78\x2d\x70\x75\x62\x6c\x69\x63\x2f\x62\x65\x6e\x63\x68\x6d\x61\x72\x6b\x73\x2f\x6e\x65\x78\x74\x63\x6f\x6e\x66\x2f\x68\x79\x70\x65\x72\x73\x70\x65\x63\x69\x61\x6c\x69\x7a\x61\x74\x69\x6f\x6e\x2f\x67\x69\x74\x68\x75\x62\x2f\x72\x75\x6e\x74\x75\x70\x6c\x65\x78\x2d\x6e\x65\x77\x2e\x70\x79\x94\x75\x4e\x4e\x4e\x74\x94\x52\x94\x8c\x1c\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x2e\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x5f\x66\x61\x73\x74\x94\x8c\x12\x5f\x66\x75\x6e\x63\x74\x69\x6f\x6e\x5f\x73\x65\x74\x73\x74\x61\x74\x65\x94\x93\x94\x68\x19\x7d\x94\x7d\x94\x28\x68\x14\x68\x0e\x8c\x0c\x5f\x5f\x71\x75\x61\x6c\x6e\x61\x6d\x65\x5f\x5f\x94\x68\x0e\x8c\x0f\x5f\x5f\x61\x6e\x6e\x6f\x74\x61\x74\x69\x6f\x6e\x73\x5f\x5f\x94\x7d\x94\x8c\x0e\x5f\x5f\x6b\x77\x64\x65\x66\x61\x75\x6c\x74\x73\x5f\x5f\x94\x4e\x8c\x0c\x5f\x5f\x64\x65\x66\x61\x75\x6c\x74\x73\x5f\x5f\x94\x4e\x8c\x0a\x5f\x5f\x6d\x6f\x64\x75\x6c\x65\x5f\x5f\x94\x68\x15\x8c\x07\x5f\x5f\x64\x6f\x63\x5f\x5f\x94\x4e\x8c\x0b\x5f\x5f\x63\x6c\x6f\x73\x75\x72\x65\x5f\x5f\x94\x4e\x8c\x17\x5f\x63\x6c\x6f\x75\x64\x70\x69\x63\x6b\x6c\x65\x5f\x73\x75\x62\x6d\x6f\x64\x75\x6c\x65\x73\x94\x5d\x94\x8c\x0b\x5f\x5f\x67\x6c\x6f\x62\x61\x6c\x73\x5f\x5f\x94\x7d\x94\x75\x86\x94\x86\x52\x30\x2e'
		f = cloudpickle.loads(code)
		return f
	return None

def pipeline_stage_0(input_row, parse_cells=True):
    res = {'outputRows':[]}
    for _ in range(1):
        if parse_cells:
            if not isinstance(input_row[0], str):
                res['exception'] = TypeError('json input must be of string type')
                res['exceptionOperatorID'] = 100000
                res['inputRow'] = input_row
                return res
                return res
            parsed_row = json.loads(input_row[0])
            keys = ['type','public','actor','created_at','payload','id','repo','org',]
            tmp_row = parsed_row
            left_over_keys = list(set(tmp_row.keys()) - set(keys))
            parsed_row = [tmp_row.get(k, None) for k in keys] + [tmp_row.get(k, None) for k in left_over_keys]
            row = Row(parsed_row, keys + left_over_keys)
            res['outputColumns'] = keys + left_over_keys
        else:
            parsed_row = input_row
            row = Row(parsed_row, ['type','public','actor','created_at','payload','id','repo','org',])
            res['outputColumns'] = ['type','public','actor','created_at','payload','id','repo','org',]
            row = NullGet(row)
        try:
            f = decodeUDF(0)
            call_res = apply_func(f, row)
            if row.columns and 'year' in row.columns:
                col_idx = row.columns.index('year')
                tmp = list(row.data)
                tmp[col_idx] = expand_row(call_res)
                row.data = tuple(tmp)
            else:
                row.columns = row.columns + ('year',) if row.columns is not None else tuple([None] * len(row.data)) + ('year',)
                row.data = row.data + result_to_row(call_res).data
        except Exception as e:
            res['exception'] = e
            res['exceptionOperatorID'] = 100001
            res['inputRow'] = input_row
            return res
        try:
            f = decodeUDF(1)
            call_res = apply_func(f, row)
            row = result_to_row(call_res, row.columns)
        except Exception as e:
            res['exception'] = e
            res['exceptionOperatorID'] = 100002
            res['inputRow'] = input_row
            return res
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        w.writerow(row.data)
        csvSerialized = buf.getvalue()
        res['outputRows'] += [csvSerialized]
        res['outputColumns'] = row.columns
    return res

