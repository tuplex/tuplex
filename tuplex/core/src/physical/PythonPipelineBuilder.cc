//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/PythonPipelineBuilder.h>
#include <StringUtils.h>
#include <PythonHelpers.h>

namespace tuplex {

    static std::string codegenRowClassCode() {
        // code for the row class, which allows to conveniently pass data to functions
        // written in either dict or tuple syntax
        auto rowClassCode = "# helper row object to allow fancy integer and column based string access within UDFs!\n"
                            "class Row:\n"
                            "    def __init__(self, data, columns=None):\n"
                            "        assert(isinstance(data, (tuple, list)))\n"
                            "        assert(isinstance(columns, (tuple, list)) or columns is None)\n"
                            "        self.data = tuple(data)\n"
                            "        self.columns = tuple(columns[:len(data)]) if columns is not None else None \n"
                            "        \n"
                            "    def __getitem__(self, key):\n"
                            "        # check for int also works for bool!\n"
                            "        if isinstance(key, int):\n"
                            "            return self.data[key]\n"
                            "        # getitem either gets a key or slice object\n"
                            "        elif isinstance(key, slice):\n"
                            "                   return self.data[key.start:key.stop:key.step]\n"
                            "        elif isinstance(key, str):\n"
                            "            if self.columns is None:\n"
                            "                raise KeyError(\"no columns defined, can't access column '{}'\".format(key))\n"
                            "            elif key not in self.columns:\n"
                            "                raise KeyError(\"could not find column column '{}'\".format(key))\n"
                            "            return self.data[self.columns.index(key)]\n"
                            "        else:\n"
                            "            raise IndexError()\n"
                            "            \n"
                            "    def __repr__(self):\n"
                            "        if self.columns:\n"
                            "            if len(self.columns) < len(self.data):\n"
                            "                self.columns = self.columns + [None] * (len(self.data) - len(self.columns))\n"
                            "            return '(' + ','.join(['{}={}'.format(c, d) for c, d in zip(self.columns, self.data)]) + ')'\n"
                            "        else:\n"
                            "            return '(' + ','.join(['{}'.format(d) for d in self.data]) + ')'\n";
        return rowClassCode;
    }

    static std::string codegenRowConversionCode() {
        auto propagateCode = "# recursive expansion of Row objects potentially present in data.\n"
                             "def expand_row(x):\n"
                             "    # Note: need to use here type construction, because isinstance fails for dict input when checking for list\n"
                             "    if hasattr(type(x), '__iter__') and not isinstance(x, str):\n"
                             "        if type(x) is tuple:\n"
                             "            return tuple([expand_row(el) for el in x])\n"
                             "        elif type(x) is list:\n"
                             "            return [expand_row(el) for el in x]\n"
                             "        elif type(x) is dict:\n"
                             "            return {expand_row(key) : expand_row(val) for key, val in x.items()}\n"
                             "        else:\n"
                             "            raise TypeError(\"custom sequence type used, can't convert to data representation\")\n"
                             "    return x.data if isinstance(x, Row) else x\n"
                             "\n"
                             "def result_to_row(res, columns=None):\n"
                             "    # convert result to row object, i.e. deal with unpacking etc.\n"
                             "    # is result a dict?\n"
                             "    if type(res) is dict:\n"
                             "        # are all keys strings? If so, then unpack!\n"
                             "        # else, keep it as dict return object!\n"
                             "        if all(map(lambda k: type(k) == str, res.keys())):\n"
                             "            # columns become keys, values \n"
                             "            columns = tuple(res.keys())\n"
                             "            data = tuple(map(lambda k: res[k], columns))\n"
                             "            return Row(data, columns)\n"
                             "    \n"
                             "    \n"
                             "    # is it a row object?\n"
                             "    # => convert to tuple!\n"
                             "    r = expand_row(res)\n"
                             "    \n"
                             "    if type(r) is not tuple:\n"
                             "        r = (r,)\n"
                             "    else:\n"
                             "        if len(r) == 0:\n"
                             "            r = ((),) # special case, empty tuple\n"
                             "    \n"
                             "    return Row(r, columns)\n\n";
        return propagateCode;
    }

    static std::string codegenApplyFuncSingleArg() {
        // special apply func code (gets special case, single element done!)
        auto applyCode = "def apply_func(f, row):\n"
                         "    if len(row.data) != 1:\n"
                         #ifndef NDEBUG
                         "        # check how many positional arguments function has.\n"
                         "        # if not one, expand row into multi args!\n"
                         #endif
                         "        nargs = f.__code__.co_argcount\n"
                         "        if nargs != 1:\n"
                         "            return f(*tuple([row[i] for i in range(nargs)]))\n"
                         "        else:\n"
                         "            return f(row)\n"
                         "    else:\n"
                         "        # unwrap single element tuples.\n"
                         "        return f(row.data[0])\n";
        return applyCode;
    }

    static std::string codegenApplyFuncTwoArg() {
        // special apply func code (gets special case, single element done!)
        auto applyCode = "def apply_func2(f, row_lhs, row_rhs):\n"
                         "    arg_lhs = row_lhs\n"
                         "    arg_rhs = row_rhs\n"
                         "    if len(row_lhs.data) == 1:\n"
                         "        # unwrap single element tuples.\n"
                         "        arg_lhs = row_lhs.data[0]\n"
                         "    if len(row_rhs.data) == 1:\n"
                         "        # unwrap single element tuples.\n"
                         "        arg_rhs = row_rhs.data[0]\n"
                         "    return f(arg_lhs, arg_rhs)\n";
        return applyCode;
    }


    PythonPipelineBuilder::PythonPipelineBuilder(const std::string &funcName) : _funcName(funcName), _indentLevel(0), _lastInputRowName(inputRowName()), _lastRowName("row"), _envCounter(0), _parseCells(false), _pipelineDone(false) {

        auto rowClassCode = codegenRowClassCode();

        // need to add ALL the python operators for overloading...
        // or do special fallback testing when calling python func...
        _header += rowClassCode;

        auto propagateCode = codegenRowConversionCode();

        _header += propagateCode;


        // special apply func code (gets special case, single element done!)
        auto applyCode = codegenApplyFuncSingleArg();
        _header += applyCode;

        // some standard packages to import so stuff works...
        _imports += "import json\n"
                    "import csv\n"
                    "import io\n"
                    "import cloudpickle\n";

        // reset code collector to lazy write later func head
        indent();
        writeLine("res = {'outputRows':[]}\n"); // empty result dictionary to store processed information, i.e. exceptions & Co


        // add dummy loop so continue works for filter.
        writeLine("for _ in range(1):\n");
        addTailCode("return res\n"); // regular result return
        indent(); // indent now everything
    }

    std::string PythonPipelineBuilder::replaceTabs(const std::string &s) const {
        std::string res;
        res = "";
        for(auto c : s) {
            if(c == '\t')
                res += std::string(_tabFactor, ' ');
            else {
                char buf[2] = {c, '\0'};
                res += buf;
            }

        }
        return res;
    }

    std::string PythonPipelineBuilder::indentLines(int indentLevel, const std::string &s) const {
        std::stringstream ss;

        indentLevel = std::max(indentLevel, 0);

        // split into lines and add spaces (4 spaces per level)
        auto lines = splitToLines(s);

        for(const auto& line : lines) {
            // trick: can construct string of simple char c repeated n times via std::string(n, c)
            // replace \t with _tabFactor spaces!
            ss<<std::string(indentLevel * _tabFactor, ' ')<<replaceTabs(line)<<"\n";
        }

        return ss.str();
    }

    void PythonPipelineBuilder::writeLine(const std::string &s) {
        _ss<<indentLines(_indentLevel, s);
    }

    std::string vecToList(const std::vector<std::string> &columns) {
        // convert to list of strings object as source code!
        std::string list = "[";
        for(const auto& c : columns)
        list += "'" + c + "',";
        return list + "]";
    }

    std::string vecToList(const std::vector<size_t> &column_indices) {
        // convert to list of strings object as source code!
        std::string list = "[";
        for(const auto& c : column_indices)
            list += std::to_string(c) + ",";
        return list + "]";
    }

    std::string PythonPipelineBuilder::columnsToList(const std::vector<std::string> &columns) {
        return vecToList(columns);
    }

    static std::string create_to_bool_function() {
        std::stringstream ss;
        ss<<"# special conversion function for boolean necessary\n"
            "def to_bool(value):\n";
        // generate valid str looking like this:
        // "    valid = {'true': True, 't': True, '1': True,\n"
        // "             'false': False, 'f': False, '0': False,\n"
        // "             }   \n";
        ss<<"    valid = {";
        for(const auto& tv : booleanTrueStrings()) {
            ss<<"'"<<tv<<"':True, ";
        }
        for(const auto& tv : booleanFalseStrings()) {
            ss<<"'"<<tv<<"':False, ";
        }
        ss<<"             }   \n";
        ss<<"\n"
            "    if isinstance(value, bool):\n"
            "        return value\n"
            "\n"
            "    if not isinstance(value, str):\n"
            "        raise ValueError('invalid literal for boolean. Not a string.')\n"
            "\n"
            "    lower_value = value.lower()\n"
            "    if lower_value in valid:\n"
            "        return valid[lower_value]\n"
            "    else:\n"
            "        raise ValueError('invalid literal for boolean: \"%s\"' % value)\n"
            "\n";
        return ss.str();
    }

    std::string create_parse_function(const std::string& name, const std::vector<std::string>& na_values) {
        // add conversion functions
        std::stringstream ss;

        // note: to_bool function should be based on the function in StringUtils:
        ss<<create_to_bool_function()<<
                         "def parse(s):\n"
                         "    assert isinstance(s, str)\n"
                         "    # try to parse s as different types\n";
                        // try parse via na_values if they exist
                        if(!na_values.empty()) {
                            ss<<"    if s in "<<vecToList(na_values)<<":\n";
                            ss<<"        return None\n";
                        }
        ss<<"    try:\n"
                         "        return to_bool(s.strip())\n"
                         "    except:\n"
                         "        pass\n"
                         "    try:\n"
                         "        return int(s.strip())\n"
                         "    except:\n"
                         "        pass\n"
                         "    try:\n"
                         "        return float(s.strip())\n"
                         "    except:\n"
                         "        pass\n"
                         "    try:\n"
                         "        return json.loads(s.strip())\n"
                         "    except:\n"
                         "        pass\n"
                         "    # return as string, final option remaining...\n"
                         "    return s";
        ss<<"\n";
        return ss.str();
    }


void PythonPipelineBuilder::cellInput(int64_t operatorID, std::vector<std::string> columns,
                                         const std::vector<std::string> &na_values,
                                         const std::unordered_map<size_t, python::Type>& typeHints,
                                         size_t numColumns, const std::unordered_map<int, int>& projectionMap) {

    _lastProjectionMap = projectionMap;
    _lastColumns = columns;
    _numUnprojectedColumns = numColumns;

    if(!columns.empty())
        assert(columns.size() == numColumns);

    std::stringstream code;
    code<<"if not isinstance("<<lastInputRowName()<<", (tuple, list)):\n";
    exceptInnerCode(code, operatorID, "TypeError('cell input must be of string type')", "", 1);
    writeLine(code.str());

    auto conv_code = create_parse_function("parse", na_values);

    writeLine(conv_code);

    // check keyword param on whether to parse cells or not (fast/slow case require that)

    // any explicit type hints given?
    if(typeHints.empty()) {
        // auto convert values to types
        writeLine("parsed_row = [parse(el) for el in " + lastInputRowName() + "] if parse_cells else list(" + lastInputRowName() + ")\n");
    } else {
        // allocate None array and then fill in
        writeLine("parsed_row = [None] * " + std::to_string(numColumns) + " if parse_cells else list(" + lastInputRowName() + ")\n");
        writeLine("if parse_cells:\n");
        indent();
        for(size_t i = 0; i < numColumns; ++i) {
            auto it = typeHints.find(i);
            if(it == typeHints.end())
                writeLine("parsed_row[" + std::to_string(i) + "] = parse(" + lastInputRowName() + "[" + std::to_string(i) + "])\n");
            else {
                // use specific type to parse for...
                auto t = it->second.withoutOptions();

                // additional if for option type
                if(it->second.isOptionType()) {
                    writeLine("if " + lastInputRowName() + "[" + std::to_string(i) + "] in " + vecToList(na_values) + ":\n");
                    writeLine("\tparsed_row[" + std::to_string(i) + "] = None\n");
                    writeLine("else:\n");
                    indent();
                }

                if(python::Type::STRING == t) {
                    writeLine("parsed_row[" + std::to_string(i) + "] = " + lastInputRowName() + "[" + std::to_string(i) + "]\n");
                } else if(python::Type::BOOLEAN == t) {
                    writeLine("parsed_row[" + std::to_string(i) + "] = to_bool(" + lastInputRowName() + "[" + std::to_string(i) + "].strip())\n");
                } else if(python::Type::I64 == t) {
                    writeLine("parsed_row[" + std::to_string(i) + "] = int(" + lastInputRowName() + "[" + std::to_string(i) + "].strip())\n");
                } else if(python::Type::F64 == t) {
                    writeLine("parsed_row[" + std::to_string(i) + "] = float(" + lastInputRowName() + "[" + std::to_string(i) + "].strip())\n");
                } else {
                    throw std::runtime_error("unsupported type hint " + it->second.desc() + " found in python codegen. Abort.");
                }

                if(it->second.isOptionType()) {
                    dedent();
                }
            }
        }
        dedent();
    }

    // projection map defined?
    if(!projectionMap.empty()) {

        // bug here, need to reverse order:
        // i.e. projection map is original_idx -> new_idx
        int min_idx = std::numeric_limits<int>::max();
        int max_idx = 0;
        std::map<int, int> m(projectionMap.begin(), projectionMap.end()); // use a map so code looks nicer...
        for(auto kv : m) {
            min_idx = std::min(min_idx, kv.second);
            max_idx = std::max(max_idx, kv.second);
        }
        int num_projected_columns = max_idx + 1;
        assert(num_projected_columns <= numColumns);

        assert(numColumns >= projectionMap.size()); // also should hold for max element in projectionMap!
        writeLine("projected_row = [None] * " + std::to_string(numColumns) + "\n"); // fill with None as dummy element
        // project elements & column names
        for(const auto& keyval: projectionMap)
            writeLine("projected_row[" + std::to_string(keyval.first) + "] = parsed_row[" + std::to_string(keyval.second) + "]\n");

        if(!columns.empty()) {
            std::vector<std::string> projected_columns(numColumns, "");
            for(const auto& keyval : projectionMap)
                projected_columns[keyval.first] = columns[keyval.second];
            columns = projected_columns;
        }
        writeLine("parsed_row = projected_row\n");
    }

    // are there columns present? If so, add to row representation!
    if(!columns.empty()) {
        writeLine(row() + " = Row(parsed_row, " + columnsToList(columns) + ")");
        writeLine("res['outputColumns'] = " + columnsToList(columns));
    }
    else
        writeLine(row() + " = Row(parsed_row)");
}

    void PythonPipelineBuilder::csvInput(int64_t operatorID,
                                         const std::vector<std::string>& columns,
                                         const std::vector<std::string>& na_values) {

        _parseCells = true;
        _lastColumns = columns;

        std::stringstream code;
        code<<"if not isinstance("<<lastInputRowName()<<", str):\n";
        exceptInnerCode(code, operatorID, "TypeError('csv input must be of string type')", "", 1);
        code<<"\n"
            <<"parsed_rows = list(csv.reader(io.StringIO("<<lastInputRowName()<<")))\n"
            <<"if len(parsed_rows) != 1:\n";
        exceptInnerCode(code, operatorID, "ValueError('csv input row yielded more than one row')", "", 1);
        code<<"\n";

        writeLine(code.str());

        // add conversion functions
        auto conv_code = create_parse_function("parse", na_values);

        writeLine(conv_code);

        // auto convert values to types
        writeLine("parsed_row = [parse(el) for el in parsed_rows[0]] if parse_cells else list(parsed_rows[0])\n");

        // are there columns present? If so, add to row representation!
        if(!columns.empty()) {
            writeLine(row() + " = Row(parsed_row, " + columnsToList(columns) + ")");
            writeLine("res['outputColumns'] = " + columnsToList(columns));
        }
        else
            writeLine(row() + " = Row(parsed_row)");
    }

    std::vector<std::string> PythonPipelineBuilder::reproject_columns(const std::vector<std::string>& columns) {
        assert(!columns.empty());

        if(!_lastProjectionMap.empty()) {
            // check that #columns is the same as reproject map
            assert(columns.size() == _lastProjectionMap.size());

            // basically update _lastColumns based on new columns & projection map
            for(const auto& kv: _lastProjectionMap) {
                assert(kv.first < _lastColumns.size());
                assert(kv.second < columns.size());
                _lastColumns[kv.first] = columns[kv.second];
            }
        } else {
            assert(columns.size() == _lastColumns.size());
            _lastColumns = columns;
        }
        return _lastColumns;
    }

    void PythonPipelineBuilder::mapOperation(int64_t operatorID, const tuplex::UDF &udf, const std::vector<std::string>& output_columns) {

        // flush last function
        flushLastFunction();

        // special case: rename, there is no UDF code here. Save the space.
        if(udf.empty()) {
            assert(!output_columns.empty());

            // project columns with current map
            auto columns = reproject_columns(output_columns);

            _lastFunction._udfCode = "";
            auto cols = columnsToList(columns);
            _lastFunction._code = row() + ".columns = (" + cols.substr(1, cols.length() - 2) + ")\n"; // use tuple!
        } else {
            // setup function
            _lastFunction._udfCode = "code = " + udfToByteCode(udf) + "\n"
                                     "f = cloudpickle.loads(code)\n";
            _lastFunction._udfCode += emitClosure(udf);

            _lastFunction._code =  "call_res = apply_func(f, " + row() + ")\n"
                                   +row()+ " = result_to_row(call_res, " + row() + ".columns)\n";

            // overwrite projection map & columns
            _lastProjectionMap = {};
            _lastColumns = output_columns;
            _numUnprojectedColumns = output_columns.size();
        }
        _lastFunction._operatorID = operatorID;
    }

    void PythonPipelineBuilder::filterOperation(int64_t operatorID, const tuplex::UDF &udf) {

        flushLastFunction();

        _lastFunction._udfCode = "code = " + udfToByteCode(udf) + "\n"
                                 "f = cloudpickle.loads(code)\n";
        _lastFunction._udfCode += emitClosure(udf);
        _lastFunction._code = "call_res = apply_func(f, " + row() + ")\n"
                              "if not call_res:\n"
                              "\tcontinue\n";

        _lastFunction._operatorID = operatorID;
    }

    std::string PythonPipelineBuilder::toByteCode(const std::string& s) {
        // return byte code representation
        std::stringstream ss;
        for(int i = 0; i < s.size(); ++i) {
            int val = (int)(s[i]) & 0xff;
            ss << "\\x" << std::hex << std::setfill('0') << std::setw(2) << val;
        }

        return "b'" + ss.str() + "'";
    }

    std::string PythonPipelineBuilder::udfToByteCode(const tuplex::UDF &udf) {
        assert(!python::holdsGIL());
        assert(python::isInterpreterRunning());
        auto code = toByteCode(udf.getPickledCode());
        return code;
    }

    void PythonPipelineBuilder::withColumn(int64_t operatorID, const std::string &columnName, const tuplex::UDF &udf) {

        // update column tracers
        if(indexInVector(columnName, _lastColumns) >= 0) {
            // replacement, no change
        } else {
            // only update if not the default map (empty)
            if(!_lastProjectionMap.empty())
                _lastProjectionMap[_numUnprojectedColumns] = _lastProjectionMap.size();
            _numUnprojectedColumns++;
            _lastColumns.push_back(columnName);
        }

       flushLastFunction();
        _lastFunction._udfCode = "code = " + udfToByteCode(udf) + "\n"
                                 "f = cloudpickle.loads(code)\n";
        _lastFunction._udfCode += emitClosure(udf);

        std::stringstream code;
        code<<"call_res = apply_func(f, " + row() + ")\n"
            <<"if "<<row()<<".columns and '"<<columnName<<"' in " + row() + ".columns:\n"
            <<"\tcol_idx = " + row() + ".columns.index('"<<columnName<<"')\n"
            <<"\ttmp = list(" + row() + ".data)\n"
            <<"\ttmp[col_idx] = expand_row(call_res)\n"
            <<"\t"<<row()<<".data = tuple(tmp)\n"
            <<"else:\n"
            <<"\t"<<row()<<".columns = "<<row()<<".columns + ('"<<columnName<<"',) if "<<row()<<".columns is not None else tuple([None] * len("<<row()<<".data)) + ('"<<columnName<<"',)\n"
            <<"\t"<<row()<<".data = "<<row()<<".data + result_to_row(call_res).data\n";
        _lastFunction._code = code.str();

        _lastFunction._operatorID = operatorID;
    }

    void PythonPipelineBuilder::mapColumn(int64_t operatorID, const std::string &columnName, const tuplex::UDF &udf) {

        flushLastFunction();

        _lastFunction._udfCode = "code = " + udfToByteCode(udf) + "\n"
                                 "f = cloudpickle.loads(code)\n";
        _lastFunction._udfCode += emitClosure(udf);

        std::stringstream code;

        code<<"col_idx = "<<row()<<".columns.index('"<<columnName<<"')\n"
            <<"arg = "<<row()<<".data[col_idx]\n"
            <<"call_res = f(arg)\n" // this here is fine, mapColumn is special case and ALWAYS operates over the element type.
            <<"tmp = list("<<row()<<".data)\n"
            <<"tmp[col_idx] = expand_row(call_res)\n"
            <<row()<<".data = tuple(tmp)\n";
        _lastFunction._code = code.str();

        _lastFunction._operatorID = operatorID;
    }

    void PythonPipelineBuilder::csvOutput(char delimiter, char quotechar, bool newLineDelimited) {
        // skip code emit if already output called
        if(_pipelineDone)
            return;
        _pipelineDone = true;

        flushLastFunction();

        std::stringstream code;
        std::string delimiterStr(1, delimiter);
        std::string quoteStr(1, quotechar);

        code<<"buf = io.StringIO()\n"
            <<"w = csv.writer(buf, delimiter='"<<delimiterStr<<"', quotechar='"<<quoteStr
            <<"', quoting=csv.QUOTE_MINIMAL, lineterminator='\\n')\n"
            <<"w.writerow("<<row()<<".data)\n"
            <<"csvSerialized = buf.getvalue()"
            <<(newLineDelimited ? "\n" : ".rstrip() # strip away trailing newline!\n");

        // create return incl. columns!
        code<<"res['outputRows'] += [csvSerialized]\n"
            <<"res['outputColumns'] = "<<row()<<".columns\n";
        // could use yield here as well...

        writeLine(code.str());
    }

    void PythonPipelineBuilder::tuplexOutput(int64_t operatorID, const python::Type &finalOutputType) {
        // skip code emit if already output called
        if(_pipelineDone)
            return;
        _pipelineDone = true;

        flushLastFunction();

        std::string code = "res['outputRows'] += [" + row() + ".data]\n"
                           "res['outputColumns'] = " + row() + ".columns\n";

#warning "type check is missing here! should be done based on finalOutputType!!! This is important when using e.g. sinks like ORC, parquet, etc. - everything that has structured types."
        // could use yield here as well...
        writeLine(code);
    }

    void PythonPipelineBuilder::pythonOutput() {
        // skip code emit if already output called
        if(_pipelineDone)
            return;
        _pipelineDone = true;

        flushLastFunction();

        std::string code = "res['outputRows'] += [" + row() + ".data]\n"
                                                              "res['outputColumns'] = " + row() + ".columns\n";
        // could use yield here as well...
        writeLine(code);
    }

    template<typename K, typename V> std::unordered_map<K, V> transform_pairs(const std::unordered_map<K, V>& m,
            const std::function<std::pair<K,V>(const std::pair<K,V>& p)>& f=[](const std::pair<K, V>& p) { return p; }) {
        std::unordered_map<K, V> ans;
        for(const auto& old_p : m) {
            auto p = f(old_p);
            ans[p.first] = p.second;
        }
        return ans;
    }

    void PythonPipelineBuilder::innerJoinDict(int64_t operatorID, const std::string &hashmap_name,
                                              tuplex::option<std::string> leftColumn,
                                              tuplex::option<std::string> rightColumn,
                                              const std::vector<std::string>& bucketColumns,
                                              option<std::string> leftPrefix,
                                              option<std::string> leftSuffix,
                                              option<std::string> rightPrefix,
                                              option<std::string> rightSuffix) {
        updateMappingForJoin(leftColumn, rightColumn, bucketColumns, leftPrefix, leftSuffix, rightPrefix, rightSuffix);


        // codegen python code for join
        flushLastFunction();

        // only string column join supported yet...
        assert(leftColumn.has_value());

        // add hashmap as var
        _optArgs.push_back(hashmap_name);
        std::stringstream code;

        // error when leftColumn is not in row
        // can ignore rightColumn

        code<<"if '"<<leftColumn.value()<<"' not in "<<row()<<".columns:\n";
        exceptInnerCode(code, operatorID, "Exception('INTERNAL: key column \"" + leftColumn.value() + "\" not in " + row() + "')", row(), 1);

        // not necessary for right column, there is also already a hashmap
        code<<"try:\n";
        code<<"\tcol_idx"<<envSuffix()<<" = "<<row()<<".columns.index('"<<leftColumn.value()<<"')\n";
        code<<"\tkey"<<envSuffix()<<" = "<<row()<<".data[col_idx"<<envSuffix()<<"]\n";
        code<<"\tkey_column"<<envSuffix()<<" = "<<row()<<".columns[col_idx"<<envSuffix()<<"] if "<<row()<<".columns else None\n";
        code<<"\tmatch"<<envSuffix()<<" = "<<hashmap_name<<"[key"<<envSuffix()<<"]\n";

        // // debug: print
        // code<<"\tprint('match is: ' + str(match"<<envSuffix()<<"))\n";

        // combine row
        code<<"\tleft_tmp"<<envSuffix()<<" = list("<<row()<<".data)\n";
        // combine columns and append prefix, suffix if desired
        code<<"\tleft_cols"<<envSuffix()<<" = list("<<row()<<".columns)\n";
        if(leftSuffix.has_value() || leftPrefix.has_value()) {
            code<<"\tleft_cols"<<envSuffix()<<" = list(map(lambda c: '"<<leftPrefix.value_or("")<<"' + c + "
                <<"'"<<leftSuffix.value_or("")<<"' if c else None, left_cols"<<envSuffix()<<"))\n";
        }

        // debug:
        // code<<"\tprint('left cols are: ' + str(left_cols"<<envSuffix()<<"))\n";

        if(bucketColumns.empty())
            code<<"\tright_cols"<<envSuffix()<<" = None\n";
        else
            code<<"\tright_cols"<<envSuffix()<<" = "<<columnsToList(bucketColumns)<<"\n";
        if(rightSuffix.has_value() || rightPrefix.has_value()) {
            code<<"\tright_cols"<<envSuffix()<<" = list(map(lambda c: '"<<rightPrefix.value_or("")<<"' + c + "
                <<"'"<<rightSuffix.value_or("")<<"' if c else None, right_cols"<<envSuffix()<<"))\n";
        }

        // start a for loop b.c. join has bucket of multiple rows, i.e. hashmap should return
        code<<"\tfor right_tmp"<<envSuffix()<<" in match"<<envSuffix()<<":\n";
        code<<"\t\tright_tmp"<<envSuffix()<<" = list(right_tmp"<<envSuffix()<<".data)"
            <<" if isinstance(right_tmp"<<envSuffix()<<", Row) else list(right_tmp"<<envSuffix()<<")\n";
#ifndef NDEBUG
        // code<<"\t\tprint(right_tmp"<<envSuffix()<<")\n";
#endif
        // --> update last input row with
        setLastInputRowName(row());
        // get tuple + columns and assign to newly named row variable.
        setRow("row" + envSuffix());
        code<<"\t\ttmp_data = tuple(left_tmp"<<envSuffix()<<"[:col_idx"<<envSuffix()<<"] + left_tmp"<<envSuffix()<<"[col_idx"<<envSuffix()<<"+1:] + [key"<<envSuffix()<<"] + right_tmp"<<envSuffix()<<")\n";
        code<<"\t\ttmp_columns = tuple(left_cols"<<envSuffix()<<"[:col_idx"<<envSuffix()<<"] + left_cols"<<envSuffix()<<"[col_idx"<<envSuffix()<<"+1:] + [left_cols"<<envSuffix()<<"[col_idx"<<envSuffix()<<"]] + right_cols"<<envSuffix()<<")\n";
        code<<"\t\t"<<row()<<" = Row(tmp_data, tmp_columns)\n";

        nextEnv(); // for the next join, new vars

        // add new tail code
        addTailCode("except KeyError:\n"
                    "\tcontinue\n"); // no output => jump to next pos in loop

        writeLine(code.str());
        // inc indent level by +2 b.c. of try + for!
        indent(); indent();

        // NOTE: could make this even easier by using Nodes + volcano style iteration....
    }

    void PythonPipelineBuilder::updateMappingForJoin(const option <std::string> &leftColumn,
                                                     const tuplex::option<std::string>& rightColumn,
                                                     const std::vector<std::string> &bucketColumns,
                                                     const option <std::string> &leftPrefix,
                                                     const option <std::string> &leftSuffix,
                                                     const option <std::string> &rightPrefix,
                                                     const option <std::string> &rightSuffix) {
        // join is a pipeline breaker, so the projection map is lost after applying it.

        // find key_column in current columns
        auto left_key_idx = indexInVector(leftColumn.value_or(""), _lastColumns);
        auto right_key_idx = indexInVector(rightColumn.value_or(""), _lastColumns);
        if(left_key_idx < 0 && right_key_idx < 0) {
            Logger::instance().defaultLogger().error("failure to generate join renaming. Could not find key column on either left or right side.");
        }

        auto key_column_idx = std::max(left_key_idx, right_key_idx);
        auto key_column = _lastColumns[key_column_idx];

        key_column = leftColumn.value_or(rightColumn.value_or(""));

        auto build_right = right_key_idx >= 0; // because always the "left" column is taken, can infer build direction
        std::__1::vector<std::string> result_columns;
        if(build_right) {
            // the bucket columns come first
            std::__1::transform(bucketColumns.begin(), bucketColumns.end(), std::back_inserter(result_columns),
                                [&](const std::string& name) { return leftPrefix.value_or("") + name + leftSuffix.value_or("");});
            result_columns.push_back(key_column); // no prefixing for key column

            // the other columns come first
            for(unsigned i = 0; i < _lastColumns.size(); ++i) {
                if(i != key_column_idx)
                    result_columns.push_back(rightPrefix.value_or("") + _lastColumns[i] + rightSuffix.value_or(""));
            }
        } else {
            // the other columns come first
            for(unsigned i = 0; i < _lastColumns.size(); ++i) {
                if(i != key_column_idx)
                    result_columns.push_back(leftPrefix.value_or("") + _lastColumns[i] + leftSuffix.value_or(""));
            }
            result_columns.push_back(key_column); // no prefixing for key column

            if(right_key_idx >= 0)
                result_columns.push_back(_lastColumns[right_key_idx]);
            std::__1::transform(bucketColumns.begin(), bucketColumns.end(), std::back_inserter(result_columns),
                                [&](const std::string& name) { return rightPrefix.value_or("") + name + rightSuffix.value_or("");});
        }

        // update the key column projection pair
        // map is original column -> projected column
        if(!_lastProjectionMap.empty()) {

            // TODO: need to update with previous column assignment...

            _lastProjectionMap = transform_pairs<int,int>(_lastProjectionMap,
                                                          [&](const std::pair<int,int>& pair) -> std::pair<int,int> {
                                                              if(pair.first == key_column_idx) {
                                                                  // gets moved to end
                                                                  return std::make_pair((int) _numUnprojectedColumns - 1, (int) _lastProjectionMap.size() - 1);
                                                              } else if(pair.first > key_column_idx) {
                                                                  return std::make_pair((int)pair.first - 1, (int)pair.second - 1);
                                                              } else
                                                                  return pair;
                                                          });

            // add bucket column pairs now
            auto num_projected = _lastProjectionMap.size();
            for(unsigned i = 0; i < bucketColumns.size(); ++i) {
                _lastProjectionMap[_numUnprojectedColumns++] = num_projected + i;
            }
            assert(_numUnprojectedColumns == result_columns.size());
        }

        _lastColumns = result_columns;
        _numUnprojectedColumns == result_columns.size();
    }

    void PythonPipelineBuilder::leftJoinDict(int64_t operatorID, const std::string &hashmap_name,
                                             tuplex::option<std::string> leftColumn,
                                             tuplex::option<std::string> rightColumn,
                                             const std::vector<std::string> &bucketColumns,
                                             option<std::string> leftPrefix, option<std::string> leftSuffix,
                                             option<std::string> rightPrefix, option<std::string> rightSuffix) {
        updateMappingForJoin(leftColumn, rightColumn, bucketColumns, leftPrefix, leftSuffix, rightPrefix, rightSuffix);

        flushLastFunction();

        // only string column join supported yet...
        assert(leftColumn.has_value());

        // add hashmap as var
        _optArgs.push_back(hashmap_name);
        std::stringstream code;

        // error when leftColumn is not in row
        // can ignore rightColumn
        code<<"if '"<<leftColumn.value()<<"' not in "<<row()<<".columns:\n";
        exceptInnerCode(code, operatorID, "Exception('INTERNAL: key column \"" + leftColumn.value() + "\" not in " + row() + "')", row(), 1);

        // logic is here slightly different than for innerJoinDict
        // -> there's always at least one match.
        // declare vars upfront with NULL match

        code<<"col_idx"<<envSuffix()<<" = "<<row()<<".columns.index('"<<leftColumn.value()<<"')\n";
        code<<"key"<<envSuffix()<<" = "<<row()<<".data[col_idx"<<envSuffix()<<"]\n";
        code<<"key_column"<<envSuffix()<<" = "<<row()<<".columns[col_idx"<<envSuffix()<<"] if "<<row()<<".columns else None\n";
        code<<"match"<<envSuffix()<<" = [(None,) * "<<bucketColumns.size()<<"]\n"; // tuple of Nones
        // not necessary for right column, there is also already a hashmap
        code<<"try:\n";
        code<<"\tmatch"<<envSuffix()<<" = "<<hashmap_name<<"[key"<<envSuffix()<<"]\n";
        code<<"except:\n";
        code<<"\tpass\n";

        // debug: print
        // code<<"print('match is: ' + str(match"<<envSuffix()<<"))\n";

        // combine row
        code<<"left_tmp"<<envSuffix()<<" = list("<<row()<<".data)\n";
        // combine columns and append prefix, suffix if desired
        code<<"left_cols"<<envSuffix()<<" = list("<<row()<<".columns)\n";
        if(leftSuffix.has_value() || leftPrefix.has_value()) {
            code<<"left_cols"<<envSuffix()<<" = list(map(lambda c: '"<<leftPrefix.value_or("")<<"' + c + "
                <<"'"<<leftSuffix.value_or("")<<"' if c else None, left_cols"<<envSuffix()<<"))\n";
        }

        // debug:
        // code<<"print('left cols are: ' + str(left_cols"<<envSuffix()<<"))\n";

        if(bucketColumns.empty())
            code<<"right_cols"<<envSuffix()<<" = None\n";
        else
            code<<"right_cols"<<envSuffix()<<" = "<<columnsToList(bucketColumns)<<"\n";
        if(rightSuffix.has_value() || rightPrefix.has_value()) {
            code<<"right_cols"<<envSuffix()<<" = list(map(lambda c: '"<<rightPrefix.value_or("")<<"' + c + "
                <<"'"<<rightSuffix.value_or("")<<"' if c else None, right_cols"<<envSuffix()<<"))\n";
        }

        // start a for loop b.c. join has bucket of multiple rows, i.e. hashmap should return
        code<<"for right_tmp"<<envSuffix()<<" in match"<<envSuffix()<<":\n";
        code<<"\tright_tmp"<<envSuffix()<<" = list(right_tmp"<<envSuffix()<<".data)"
            <<" if isinstance(right_tmp"<<envSuffix()<<", Row) else list(right_tmp"<<envSuffix()<<")\n";
#ifndef NDEBUG
        // code<<"\tprint(right_tmp"<<envSuffix()<<")\n";
#endif
        // --> update last input row with
        setLastInputRowName(row());
        // get tuple + columns and assign to newly named row variable.
        setRow("row" + envSuffix());
        code<<"\ttmp_data = tuple(left_tmp"<<envSuffix()<<"[:col_idx"<<envSuffix()<<"] + left_tmp"<<envSuffix()<<"[col_idx"<<envSuffix()<<"+1:] + [key"<<envSuffix()<<"] + right_tmp"<<envSuffix()<<")\n";
        code<<"\ttmp_columns = tuple(left_cols"<<envSuffix()<<"[:col_idx"<<envSuffix()<<"] + left_cols"<<envSuffix()<<"[col_idx"<<envSuffix()<<"+1:] + [left_cols"<<envSuffix()<<"[col_idx"<<envSuffix()<<"]] + right_cols"<<envSuffix()<<")\n";
        code<<"\t"<<row()<<" = Row(tmp_data, tmp_columns)\n";

        nextEnv(); // for the next join, new vars

        writeLine(code.str());
        // inc indent level by +1 b.c. of for!
        indent();
    }

    void PythonPipelineBuilder::resolve(int64_t operatorID, tuplex::ExceptionCode ec, const tuplex::UDF &udf) {

        std::stringstream code;
        code<<"code = " + udfToByteCode(udf) + "\n";
        code<<"f = cloudpickle.loads(code)\n";
        code<<emitClosure(udf);

        // try except block for calling the concrete
        code<<"try:\n";
        // indent lines!
        code<<indentLines(1, _lastFunction._code)<<"\n";

        // remove exceptions from res to continue processing...
        std::stringstream ss;
        ss<<"del res['exception']\n";
        ss<<"del res['exceptionOperatorID']\n";
        ss<<"del res['inputRow']\n";
        code<<indentLines(1, ss.str())<<"\n";

        exceptCode(code, operatorID);
//        code<<"except Exception as re:\n"; // resolver exception
//        // exception return
//        code<<"\tres['exception'] = re\n";
//        code<<"\tres['exceptionOperatorID'] = " + std::to_string(operatorID) + "\n";
//        code<<"\tres['inputRow'] = obj\n";
//        code<<"\treturn res\n"; // jump out of pipeline, i.e. resolver produced exception!

        // very simple, add to lastFunction
        _lastFunction._handlers.push_back(std::make_tuple(ec, operatorID, code.str()));
    }

    void PythonPipelineBuilder::ignore(int64_t operatorID, tuplex::ExceptionCode ec) {
        _lastFunction._handlers.push_back(std::make_tuple(ec, operatorID, ""));
    }

    void PythonPipelineBuilder::objInput(int64_t operatorID, const std::vector<std::string> &columns) {
        _parseCells = false;
        _lastColumns = columns;

        // simple: input is tuple or list
        // ==> convert to row + assign columns if given
        if(!columns.empty())
            writeLine(row() + " = Row(" + inputRowName() + ", " + columnsToList(columns) + ")\n");
        else
            writeLine(row() + " = Row(" + inputRowName() + ")\n");
    }

    void PythonPipelineBuilder::flushLastFunction() {

        // skip if no code there
        if (_lastFunction._code.empty())
            return;

        // try...except block
        writeLine("try:\n");
        indent();
        writeLine(_lastFunction._udfCode);
        writeLine(_lastFunction._code);
        dedent();
        writeLine("except Exception as e:\n");

        // save current exception info
        {
            indent();
            std::stringstream ss;
            auto exception_id = "e";
            auto opID = _lastFunction._operatorID;
            auto inputRow = "input_row";
            ss<<"res['exception'] = "<<exception_id<<"\n";
            ss<<"res['exceptionOperatorID'] = "<<opID<<"\n";
            ss<<"res['inputRow'] = "<<inputRow<<"\n";
            writeLine(ss.str());
            dedent();
        }


        // handlers there?
        if(_lastFunction._handlers.empty()) {
            // always report as exception
            std::stringstream ss;
            //exceptInnerCode(ss, _lastFunction._operatorID, "e", "", 1);
            ss<<"return res"<<std::endl;
            indent();
            //writeLine(ss.str());
            dedent();
        } else {

            assert(_lastFunction._handlers.size() >= 1); // must be because of if...else...

            indent();
            // if handler is empty, it's an ignore!
            for(int i = 0; i < _lastFunction._handlers.size(); ++i) {
                auto& h = _lastFunction._handlers[i];
                auto ecCode = std::get<0>(h);
                auto opID = std::get<1>(h);
                auto handler_code = std::get<2>(h);

                // note: simple == not correct when trying to observe exception hierarchy
                // writeLine("if 'exception' in res.keys() and type(res['exception']) == " + exceptionCodeToPythonClass(ecCode) + ":\n");
                // instead use issubclass
                writeLine("if 'exception' in res.keys() and issubclass(type(res['exception']), " + exceptionCodeToPythonClass(ecCode) + "):\n");
                indent();

                // check if there is handler code, else it's an ignore!
                if(handler_code.size() == 0) {
                    // ignore
                    // clear out exceptions and continue processing of next row
                    // just exit pipeline with success
                    writeLine("del res['exception']");
                    writeLine("del res['exceptionOperatorID']");
                    writeLine("del res['inputRow']");
                    writeLine("continue\n");
                } else {
                    writeLine(handler_code);
                }

                dedent();
            }

            // if exception still exists, return, else continue processing.
            std::stringstream ss;
            ss<<"if 'exception' in res.keys():\n";
            ss<<"\treturn res\n";

            dedent(); // dedent to put this if check outside of except:
            writeLine(ss.str());


//            std::stringstream ss;
//            exceptInnerCode(ss, _lastFunction._operatorID, "e", "", 1);
//            writeLine(ss.str());
//            writeLine("return res");
            // for the else dedent again
//            dedent();
        }

        // reset
        _lastFunction._code = "";
        _lastFunction._operatorID = -1;
        _lastFunction._handlers.clear();
        assert(_lastFunction._handlers.empty());
    }

    std::string PythonPipelineBuilder::emitClosure(const UDF &udf) {

        // cloudpickle handles this, else the code adding import statements is below
        return "";


        using namespace std;

        if(udf.empty())
            return "";

        // import all modules, define all globals
        // => res, input row etc. may be a problem!
        auto forbidden_names = std::set<std::string>{"f", "row", "input_row", "res", "parse_cells"};
        for(auto arg : _optArgs)
            forbidden_names.insert(arg);

        // go through
        std::stringstream ss;
        auto ce = udf.getAnnotatedAST().globals();

        // 1. modules (import)
        for(auto m: ce.modules()) {
            if(m.identifier == m.original_identifier)
                ss<<"import "<<m.identifier<<endl;
            else
                ss<<"import "<<m.original_identifier<<" as "<<m.identifier<<endl;
        }

        // 2. functions
        for(auto f: ce.functions()) {
            ss<<"from "<<f.package<<" import "<<f.qualified_name<<" as "<<f.identifier<<endl;
        }

        // 3. constants
        for(auto c : ce.constants()) {
            ss<<c.identifier<<" = "<<c.value.toPythonString()<<endl;
        }
        return ss.str();
    }


    void PythonPipelineBuilder::pythonAggByKey(int64_t operatorID,
                                               const std::string& hashmap_name,
                                               const tuplex::UDF &aggUDF,
                                               const std::vector<size_t> &aggColumns,
                                               const Row& initial_value) {

        flushLastFunction();

        // add hashmap as var
        _optArgs.push_back(hashmap_name);

        // perform aggregate function on current output row & saved aggregate
        // also yield key...
        // fetch current key
        std::stringstream ss;

        // create at beginning of pipeline function lookup into aggregate value
        std::stringstream header;
        header<<"agg_value = None\n";

        // check if key exists in hashmap, if not create! Else, call aggregate function on top!
        ss<<"agg_key = ["<<row()<<"[key] for key in "<<vecToList(aggColumns)<<"]\n";
        ss<<"agg_key = tuple(agg_key) if len(agg_key) != 1 else agg_key[0]\n";
        ss<<"if agg_value is None:\n";
        ss<<"\tagg_value = "<<hashmap_name<<".setdefault(agg_key, result_to_row("<<initial_value.toPythonString()<<"))\n";

        // add aggregate initialization to header
        _headCode += header.str();

        // decode function
        ss<<"code = "<<udfToByteCode(aggUDF)<<"\n";
        ss<<"f_agg = cloudpickle.loads(code)\n";
        ss<<"agg_value = "<<"apply_func2(f_agg, result_to_row(agg_value), "<<row()<<")\n";

        // output aggregate value and key (b.c. special treatment necessary!)
        // update row to be agg value
        ss<<row()<<" = result_to_row(agg_value)\n";
        ss<<"res['key'] = agg_key\n";

        _header += codegenApplyFuncTwoArg();

        auto code = ss.str();

        // could use yield here as well...
        writeLine(code);
    }

    void PythonPipelineBuilder::pythonAggGeneral(int64_t operatorID, const std::string& agg_intermediate_name,
                                                 const tuplex::UDF &aggUDF, const Row& initial_value) {

        // there's no key, but just a global aggregate. Hence, update the intermediate being passed down!
        flushLastFunction();

        // add hashmap as var
        _optArgs.push_back(agg_intermediate_name);


        // perform aggregate function on current output row & saved aggregate
        // also yield key...
        // fetch current key
        std::stringstream ss;

        // create at beginning of pipeline function lookup into aggregate value
        std::stringstream header;
        header<<"agg_value = "<<agg_intermediate_name<<"\n";

        // add aggregate initialization to header
        _headCode += header.str();


        // decode function
        ss<<"code = "<<udfToByteCode(aggUDF)<<"\n";
        ss<<"f_agg = cloudpickle.loads(code)\n";
        ss<<"agg_value = "<<"apply_func2(f_agg, result_to_row(agg_value), "<<row()<<")\n";
        // output aggregate value and key (b.c. special treatment necessary!)
        // update row to be agg value
        ss<<row()<<" = result_to_row(agg_value)\n";

        _header += codegenApplyFuncTwoArg();

        auto code = ss.str();

        // could use yield here as well...
        writeLine(code);
    }

    std::string codegenPythonCombineAggregateFunction(const std::string& function_name,
                                                      int64_t operatorID,
                                                      const AggregateType& agg_type,
                                                      const Row& initial_value,
                                                      const UDF& combine_udf) {

        std::stringstream ss;

        auto py_initial_value = initial_value.toPythonString();

        ss<<"import cloudpickle\n\n";
        ss<<codegenRowClassCode()<<"\n";
        ss<<codegenRowConversionCode()<<"\n";
        ss<<codegenApplyFuncTwoArg()<<"\n";
        ss<<"def "<<function_name<<"(a, b=None):\n";
        ss<<"\tres = {'exceptionOperatorID': "<<operatorID<<"}\n";
        ss<<"\ttry:\n";
        ss<<"\t\tcode = "<<PythonPipelineBuilder::udfToByteCode(combine_udf)<<"\n";
        ss<<"\t\tf = cloudpickle.loads(code)\n";
        // emit code to combine aggregates depending on agg type
        switch(agg_type) {
            case AggregateType::AGG_UNIQUE: {
                // this a simple combine (only keys really matter)
                ss<<"\t\tcombined_agg = a.copy()\n";
                ss<<"\t\tcombined_agg.update(b)\n";
                break;
            }
            case AggregateType::AGG_BYKEY: {
                // iterate over all keys and combine
                // in order to preserve semantics, combine has to be run for each group at least once (i.e. with initial value).

                // special case: b is None
                // this means we have to call combine over each key of a
                ss<<"\t\tagg0 = result_to_row("<<py_initial_value<<")\n";
                ss<<"\t\tif b is None:\n";
                ss<<"\t\t\tcombined_agg = a.copy()\n";
                ss<<"\t\t\tfor k in a.keys():\n";
                ss<<"\t\t\t\tcombined_agg[k] = apply_func2(f, result_to_row(a[k]), agg0)\n";

                // regular case => combine for all keys
                ss<<"\t\telse:\n";
                ss<<"\t\t\tcombined_agg = {}\n";
                ss<<"\t\t\tfor k in b.keys() & a.keys():\n";
                ss<<"\t\t\t\tcombined_agg[k] = apply_func2(f, result_to_row(a[k]), result_to_row(b[k]))\n";
                ss<<"\t\t\tfor k in b.keys():\n";
                ss<<"\t\t\t\tif k not in combined_agg.keys():\n";
                ss<<"\t\t\t\t\tcombined_agg[k] = apply_func2(f, agg0, result_to_row(b[k]))\n";
                ss<<"\t\t\tfor k in a.keys():\n";
                ss<<"\t\t\t\tif k not in combined_agg.keys():\n";
                ss<<"\t\t\t\t\tcombined_agg[k] = apply_func2(f, result_to_row(a[k]), agg0)\n";
                break;
            }
            case AggregateType::AGG_GENERAL: {
                // simply call combine once
                // special case: if b is None (so not a dict), run per group default combine.
                //ss<<"\t\tcombined_agg = f(a, b)\n";
                throw std::runtime_error("not yet supported");
                break;
            }
            default:
                throw std::runtime_error("unknown aggregate type " + std::to_string(static_cast<int>(agg_type)) + " encountered.");
        }

        ss<<"\texcept Exception as e:\n";
        ss<<"\t\tres['input_lhs'] = a\n";
        ss<<"\t\tres['input_rhs'] = b\n";
        ss<<"\t\tres['exception'] = e\n";
        ss<<"\t\treturn res\n";
        ss<<"\tres['aggregate'] = combined_agg\n";
        ss<<"\treturn res\n";

        return ss.str();
    }
}