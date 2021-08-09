import json
import csv
import io
import cloudpickle

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
            columns = res.keys()
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
        try:
            # call with default mode using tuple as base element essentially
            return f(row)
        except Exception as te:
            # single op error?
            # try unwrapped...
            return f(row.data[0])
def pipeline_stage_0(input_row, parse_cells=False):
    res = {'outputRows':[]}
    for _ in range(1):
        if not isinstance(input_row, (tuple, list)):
            res['exception'] = TypeError('cell input must be of string type')
            res['exceptionOperatorID'] = 100000
            res['inputRow'] = input_row
        # special conversion function for boolean necessary
        def to_bool(value):
            valid = {'true':True, 't':True, 'yes':True, 'y':True, 'false':False, 'f':False, 'no':False, 'n':False,              }   
        
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
            if s in ['Unspecified','NO CLUE','NA','N/A','0','',]:
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
        parsed_row = [parse(el) for el in input_row] if parse_cells else list(input_row)
        row = Row(parsed_row, ['YEAR','QUARTER','MONTH','DAY_OF_MONTH','DAY_OF_WEEK','FL_DATE','OP_UNIQUE_CARRIER','OP_CARRIER_AIRLINE_ID','OP_CARRIER','TAIL_NUM','OP_CARRIER_FL_NUM','ORIGIN_AIRPORT_ID','ORIGIN_AIRPORT_SEQ_ID','ORIGIN_CITY_MARKET_ID','ORIGIN','ORIGIN_CITY_NAME','ORIGIN_STATE_ABR','ORIGIN_STATE_FIPS','ORIGIN_STATE_NM','ORIGIN_WAC','DEST_AIRPORT_ID','DEST_AIRPORT_SEQ_ID','DEST_CITY_MARKET_ID','DEST','DEST_CITY_NAME','DEST_STATE_ABR','DEST_STATE_FIPS','DEST_STATE_NM','DEST_WAC','CRS_DEP_TIME','DEP_TIME','DEP_DELAY','DEP_DELAY_NEW','DEP_DEL15','DEP_DELAY_GROUP','DEP_TIME_BLK','TAXI_OUT','WHEELS_OFF','WHEELS_ON','TAXI_IN','CRS_ARR_TIME','ARR_TIME','ARR_DELAY','ARR_DELAY_NEW','ARR_DEL15','ARR_DELAY_GROUP','ARR_TIME_BLK','CANCELLED','CANCELLATION_CODE','DIVERTED','CRS_ELAPSED_TIME','ACTUAL_ELAPSED_TIME','AIR_TIME','FLIGHTS','DISTANCE','DISTANCE_GROUP','CARRIER_DELAY','WEATHER_DELAY','NAS_DELAY','SECURITY_DELAY','LATE_AIRCRAFT_DELAY','FIRST_DEP_TIME','TOTAL_ADD_GTIME','LONGEST_ADD_GTIME','DIV_AIRPORT_LANDINGS','DIV_REACHED_DEST','DIV_ACTUAL_ELAPSED_TIME','DIV_ARR_DELAY','DIV_DISTANCE','DIV1_AIRPORT','DIV1_AIRPORT_ID','DIV1_AIRPORT_SEQ_ID','DIV1_WHEELS_ON','DIV1_TOTAL_GTIME','DIV1_LONGEST_GTIME','DIV1_WHEELS_OFF','DIV1_TAIL_NUM','DIV2_AIRPORT','DIV2_AIRPORT_ID','DIV2_AIRPORT_SEQ_ID','DIV2_WHEELS_ON','DIV2_TOTAL_GTIME','DIV2_LONGEST_GTIME','DIV2_WHEELS_OFF','DIV2_TAIL_NUM','DIV3_AIRPORT','DIV3_AIRPORT_ID','DIV3_AIRPORT_SEQ_ID','DIV3_WHEELS_ON','DIV3_TOTAL_GTIME','DIV3_LONGEST_GTIME','DIV3_WHEELS_OFF','DIV3_TAIL_NUM','DIV4_AIRPORT','DIV4_AIRPORT_ID','DIV4_AIRPORT_SEQ_ID','DIV4_WHEELS_ON','DIV4_TOTAL_GTIME','DIV4_LONGEST_GTIME','DIV4_WHEELS_OFF','DIV4_TAIL_NUM','DIV5_AIRPORT','DIV5_AIRPORT_ID','DIV5_AIRPORT_SEQ_ID','DIV5_WHEELS_ON','DIV5_TOTAL_GTIME','DIV5_LONGEST_GTIME','DIV5_WHEELS_OFF','DIV5_TAIL_NUM','',])
        res['outputColumns'] = ['YEAR','QUARTER','MONTH','DAY_OF_MONTH','DAY_OF_WEEK','FL_DATE','OP_UNIQUE_CARRIER','OP_CARRIER_AIRLINE_ID','OP_CARRIER','TAIL_NUM','OP_CARRIER_FL_NUM','ORIGIN_AIRPORT_ID','ORIGIN_AIRPORT_SEQ_ID','ORIGIN_CITY_MARKET_ID','ORIGIN','ORIGIN_CITY_NAME','ORIGIN_STATE_ABR','ORIGIN_STATE_FIPS','ORIGIN_STATE_NM','ORIGIN_WAC','DEST_AIRPORT_ID','DEST_AIRPORT_SEQ_ID','DEST_CITY_MARKET_ID','DEST','DEST_CITY_NAME','DEST_STATE_ABR','DEST_STATE_FIPS','DEST_STATE_NM','DEST_WAC','CRS_DEP_TIME','DEP_TIME','DEP_DELAY','DEP_DELAY_NEW','DEP_DEL15','DEP_DELAY_GROUP','DEP_TIME_BLK','TAXI_OUT','WHEELS_OFF','WHEELS_ON','TAXI_IN','CRS_ARR_TIME','ARR_TIME','ARR_DELAY','ARR_DELAY_NEW','ARR_DEL15','ARR_DELAY_GROUP','ARR_TIME_BLK','CANCELLED','CANCELLATION_CODE','DIVERTED','CRS_ELAPSED_TIME','ACTUAL_ELAPSED_TIME','AIR_TIME','FLIGHTS','DISTANCE','DISTANCE_GROUP','CARRIER_DELAY','WEATHER_DELAY','NAS_DELAY','SECURITY_DELAY','LATE_AIRCRAFT_DELAY','FIRST_DEP_TIME','TOTAL_ADD_GTIME','LONGEST_ADD_GTIME','DIV_AIRPORT_LANDINGS','DIV_REACHED_DEST','DIV_ACTUAL_ELAPSED_TIME','DIV_ARR_DELAY','DIV_DISTANCE','DIV1_AIRPORT','DIV1_AIRPORT_ID','DIV1_AIRPORT_SEQ_ID','DIV1_WHEELS_ON','DIV1_TOTAL_GTIME','DIV1_LONGEST_GTIME','DIV1_WHEELS_OFF','DIV1_TAIL_NUM','DIV2_AIRPORT','DIV2_AIRPORT_ID','DIV2_AIRPORT_SEQ_ID','DIV2_WHEELS_ON','DIV2_TOTAL_GTIME','DIV2_LONGEST_GTIME','DIV2_WHEELS_OFF','DIV2_TAIL_NUM','DIV3_AIRPORT','DIV3_AIRPORT_ID','DIV3_AIRPORT_SEQ_ID','DIV3_WHEELS_ON','DIV3_TOTAL_GTIME','DIV3_LONGEST_GTIME','DIV3_WHEELS_OFF','DIV3_TAIL_NUM','DIV4_AIRPORT','DIV4_AIRPORT_ID','DIV4_AIRPORT_SEQ_ID','DIV4_WHEELS_ON','DIV4_TOTAL_GTIME','DIV4_LONGEST_GTIME','DIV4_WHEELS_OFF','DIV4_TAIL_NUM','DIV5_AIRPORT','DIV5_AIRPORT_ID','DIV5_AIRPORT_SEQ_ID','DIV5_WHEELS_ON','DIV5_TOTAL_GTIME','DIV5_LONGEST_GTIME','DIV5_WHEELS_OFF','DIV5_TAIL_NUM','',]
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        w.writerow(row.data)
        csvSerialized = buf.getvalue()
        res['outputRows'] += [csvSerialized]
        res['outputColumns'] = row.columns
    return res

