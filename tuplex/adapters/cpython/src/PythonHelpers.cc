//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonHelpers.h>
#include <Utils.h>
#include <StringUtils.h>
#include <PythonVersions.h>
#include <TypeHelper.h>

#ifndef NDEBUG
#include <boost/stacktrace.hpp>
#endif
#include <regex>

#include <unordered_map>

// module specific vars
static std::unordered_map<std::string, PyObject*> cached_functions;

static std::string generateLambdaName(int& counter) {
    auto name = "lamFun" + std::to_string(counter);
    while(true) {
        auto mod = python::getMainModule();
        auto main_dict = PyModule_GetDict(mod);
        if(PyDict_Contains(main_dict, python::PyString_FromString(name.c_str()))) {
            counter++;
            name = "lamFun" + std::to_string(counter);
        } else {
            counter++;
            break;
        }
    }

    return name;
}

namespace python {

    void python_home_setup(const std::string& home_dir) {
        // convert to wchar
        std::vector<wchar_t> vec;
        auto len = strlen(home_dir.c_str());
        vec.resize(len + 1);
        mbstowcs(&vec[0], home_dir.c_str(), len);
        // set python home
        Py_SetPythonHome(&vec[0]);
    }

    void handle_and_throw_py_error() {
        if(PyErr_Occurred()) {
            // PyObject *ptype = NULL, *pvalue = NULL, *ptraceback = NULL;
            // PyErr_Fetch(&ptype,&pvalue,&ptraceback);
            // PyErr_NormalizeException(&ptype,&pvalue,&ptraceback);

            std::stringstream ss;
            PyObject *extype = nullptr, *value=nullptr, *traceback=nullptr;
            PyErr_Fetch(&extype, &value, &traceback);
            if (!extype)
                throw std::runtime_error("could not obtain error");
//            if (!value)
//                throw std::runtime_error("could not obtain value");
//            if (!traceback)
//                throw std::runtime_error("could not obtain traceback");
//            // value and traceback may be nullptr (?)

            auto mod_traceback = PyImport_ImportModule("traceback");
            if(!mod_traceback) {
                PyErr_Clear();
                throw std::runtime_error("failed to import internal traceback module");
            }

            auto mod_traceback_dict = PyModule_GetDict(mod_traceback);
            if(!mod_traceback_dict) {
                PyErr_Clear();
                throw std::runtime_error("failed to retrieve namespace dict from internal traceback module");
            }

            auto format_exception_func = PyObject_GetAttrString(mod_traceback_dict, "format_exception");
            if(!format_exception_func) {
#ifndef NDEBUG
                std::cerr<<"traceback module doesn't contain format_exception function."<<std::endl;
                std::cerr<<PyString_AsString(mod_traceback_dict)<<std::endl;
                PyObject_Print(mod_traceback_dict, stderr, 0);
                std::cerr<<std::endl;
#endif
            }
            assert(PyCallable_Check(format_exception_func));

            // call function, set all args
            auto args = PyTuple_New(3);
            PyTuple_SET_ITEM(args, 0, extype);
            PyTuple_SET_ITEM(args, 1, value);
            PyTuple_SET_ITEM(args, 2, traceback);

            // get list object & convert to strings
            auto lines_obj = PyObject_Call(format_exception_func, args, nullptr);
            if(!mod_traceback_dict) {
                PyErr_Clear();
                throw std::runtime_error("failed to call format_exception from internal traceback module");
            }
            assert(PyList_Check(lines_obj));
            auto line_count = PyList_Size(lines_obj);
            for (unsigned i = 0; i < line_count; ++i) {
                auto item = PyList_GET_ITEM(lines_obj, i);
                Py_XINCREF(item);
                auto line = PyString_AsString(item);
                ss << line;
            }
            Py_XDECREF(lines_obj);

            throw std::runtime_error(ss.str());
        }
    }


    // regex to extract function name
    std::string extractPythonFunctionName(const std::string& code) {
        // regex to extract function name
        using namespace std;

        regex frgx(R"([\t ]*def\s*(.*)\(.*\)\s*:[\t ]*\n)");

        std::smatch matches;
        // only first match will be recorded.
        if(regex_search(code, matches, frgx)) {

            assert(matches.size() == 2); // must be two: one for the full string, second for the name
            return matches[1].str();
        }

        return "";
    }


    PyObject* PyString_FromString(const char* str) {
       auto unicode_obj = PyUnicode_DecodeUTF8(str, strlen(str), nullptr);

       if(!unicode_obj) {
           PyErr_Clear();

           // string was not a unicode object. use per default iso_8859_1 (latin-1 supplement) to utf8 conversion
           auto utf8_str = tuplex::iso_8859_1_to_utf8(std::string(str));
           assert(tuplex::utf8_check_is_valid);
           return PyString_FromString(utf8_str.c_str());
       }

#ifndef NDEBUG
       handle_and_throw_py_error();
#endif
       return unicode_obj;
    }

    std::string PyString_AsString(PyObject* obj) {
        assert(holdsGIL());
        assert(obj);

        auto str = PyObject_Str(obj); // cast to str
        assert(PyUnicode_Check(str));
        return PyUnicode_AsUTF8(str);
    }

// helper functions to handle python code
    PyObject* getMainModule() {
        assert(isInterpreterRunning());

#ifndef NDEBUG
        // print stack trace
        if(!python::holdsGIL()) {
            std::cerr<<boost::stacktrace::stacktrace();
            std::abort();
        }
#endif

        assert(python::holdsGIL());
        PyObject *mainModule = PyImport_AddModule("__main__");

        // check for error
        if(PyErr_Occurred()) {
            PyErr_Print();
            std::cout<<std::endl;
            exit(1);
        }

        // import gc module to access debug garbage collector information
        //PyObject* gcModule = PyImport_AddModule("gc");
        // call gc.set_debug(gc.DEBUG_LEAK)
        // PyRun_SimpleString("import gc");
        // PyRun_SimpleString("gc.set_debug(gc.DEBUG_LEAK)");
        // PyRun_SimpleString("gc.disable()");

        // import cloudpickle for serialized functions
        PyObject *cloudpickleModule = PyImport_ImportModule("cloudpickle");

        // check whether cloudpickle module exists or errors occured!
        if(!cloudpickleModule) {
            // quit program
            Logger::instance().defaultLogger().error("cloudpickle module could not be found. Please install first.");
            exit(1);
        }

        PyModule_AddObject(mainModule, "cloudpickle", cloudpickleModule);

        // TOOD: fix this according to doc https://docs.python.org/3/c-api/module.html
        #warning "fix this!!!"

        // import submodules
        PyObject *subModules = PyList_New(0);
        PyList_Append(subModules, PyString_FromString("loads"));
        PyList_Append(subModules, PyString_FromString("dumps"));
        PyObject * cloudpickleImports = PyImport_ImportModuleEx("cloudpickle", nullptr, nullptr, subModules);
        PyObject * loadsModule = PyObject_GetAttr(cloudpickleImports, PyString_FromString("loads"));
        PyObject * dumpsModule = PyObject_GetAttr(cloudpickleImports, PyString_FromString("dumps"));
        PyModule_AddObject(mainModule, "loads", loadsModule);
        PyModule_AddObject(mainModule, "dumps", dumpsModule);

        return mainModule;
    }

    PyObject* deserializePickledFunction(PyObject* mainModule, const char* mem, const size_t size) {

        assert(mem);

        assert(holdsGIL());

        auto pFunc = PyObject_GetAttrString(mainModule, "loads");
        assert(pFunc);

        auto pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, PyBytes_FromStringAndSize(mem, size));
        auto pFuncLambda = PyObject_CallObject(pFunc, pArgs);
        assert(pFuncLambda);

        assert(PyCallable_Check(pFuncLambda));
        return pFuncLambda;
    }

    std::vector<std::string> getArgNames(PyObject *pFunc) {
        assert(pFunc);
        assert(holdsGIL());
        assert(PyCallable_Check(pFunc));
        assert(PyObject_HasAttrString(pFunc, "__code__"));
        PyObject *codeObj = PyObject_GetAttrString(pFunc, "__code__");
        if(PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }
        assert(PyObject_HasAttrString(codeObj, "co_argcount"));
        assert(PyObject_HasAttrString(codeObj, "co_varnames"));
        PyObject *argcount = PyObject_GetAttrString(codeObj, "co_argcount");
        PyObject *argNames = PyObject_GetAttrString(codeObj, "co_varnames");

        size_t numArgs = PyLong_AsLong(argcount);
        // could error if too big
        if(PyErr_Occurred()) {
            PyErr_Clear();
            throw std::runtime_error("too big integer object");
        }
        // fetch argument names
        size_t tupleSize = PyTuple_Size(argNames);

        assert(numArgs == tupleSize);

        std::vector<std::string> argnames;
        for(unsigned i = 0; i < tupleSize; ++i) {
            std::string argname = PyUnicode_AsUTF8(PyTuple_GetItem(argNames, i));
            argnames.push_back(argname);
        }

        return argnames;
    }

    void handlePythonErrors(MessageHandler& logger = Logger::instance().defaultLogger()) {
        if(PyErr_Occurred()) {
#ifndef NDEBUG
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            // uncomment these lines to debug what happened in detail
            PyErr_Restore(type, value, traceback);
            PyErr_Print();
            std::cout.flush();

            Py_XDECREF(type);
            Py_XDECREF(value);
            Py_XDECREF(traceback);
#endif
            logger.error("error while trying to compile python function.");
            PyErr_Clear();
        }
    }

    PyObject* compileFunction(PyObject* mainModule, const std::string& code) {
        MessageHandler& logger = Logger::instance().logger("python");

        assert(mainModule);

        assert(holdsGIL());


        // there is a weird bug here when the same lambda gets compiled, hence cache it.
        #warning "solve the bug here..."

        auto it = cached_functions.find(code);
        if(it == cached_functions.end()) {
            // compile the code
            auto moduleDict = PyModule_GetDict(mainModule);

            assert(moduleDict);

            // check whether function is lambda or def
            std::string funcName = extractPythonFunctionName(code);

            PyObject* func = nullptr;

            // make sure it is not dumps or loads
            if(funcName == "dumps" || funcName == "loads") {
                logger.error("can not serialize function '" + funcName + "' in module because of conflict with cloudpickle functions");
                return nullptr;
            }

            // check if lambda or not
            if(funcName.length() == 0) {
                // create artificial name
                int counter = 0;
                auto name = generateLambdaName(counter);


                // count initial whitespace lines removed (need to adjust the lines in debug code accordingly)
                auto offset = tuplex::leading_line_count(code);

                std::string lamDefCode = name + " = " + tuplex::trim(code); // make sure to strip whitespace from code.

                PyRun_String(lamDefCode.c_str(), Py_file_input, moduleDict, moduleDict);
                if(PyErr_Occurred()) {
                    PyErr_Print();
                    PyErr_Clear();
                }
                func = PyDict_GetItemString(moduleDict, name.c_str());

                // set helper field in func object
                // i.e. f.__dict__['line_offset'] = offset
                if(offset != 0) {
                    auto dict = PyObject_GetAttrString(func, "__dict__");
                    PyDict_SetItemString(dict, "line_offset", PyLong_FromLong(offset));
                }
            } else {
                PyRun_String(code.c_str(), Py_file_input, moduleDict, moduleDict);
                if(PyErr_Occurred()) {
                    PyErr_Print();
                    PyErr_Clear();
                }

                func = PyDict_GetItemString(moduleDict, funcName.c_str());
            }
            assert(func);
            assert(PyCallable_Check(func));
            cached_functions[code] = func;

            Py_INCREF(func);
            return func;
        } else  {
            Py_INCREF(it->second);
            return it->second;
        }
    }

    std::string serializeFunction(PyObject* mainModule, const std::string& code) {
        auto func = compileFunction(mainModule, code);
        if(!func)
            return "";

        assert(func);

        // now serialize the damn thing
        auto pDumpsFunc = PyObject_GetAttrString(mainModule, "dumps");
        if(!pDumpsFunc) {
            Logger::instance().defaultLogger().info("dumps not found in mainModule!");
            return "";
        }
        assert(pDumpsFunc);

        auto pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, func);
        auto pBytesObj = PyObject_CallObject(pDumpsFunc, pArgs);

        std::string res;
        char *mem = PyBytes_AsString(pBytesObj);
        size_t mem_size = PyBytes_Size(pBytesObj);
        res.reserve(mem_size);
        res.assign(mem, mem_size); // important to add here size! Else, full code won't get copied (there may be '\0' bytes)

        assert(res.length() == mem_size);
        return res;
    }

    /*!
     * silly CPython uses two references for boolean values.... Why :( ?
     * @param obj
     * @return
     */
    bool PyBool_AsBool(PyObject* obj) {
        assert(holdsGIL());
        // make sure python interpreter is initialized
        assert(Py_IsInitialized());

        if(!PyBool_Check(obj)) {
            Logger::instance().defaultLogger().error("Python object is not a valid boolean object!");
        }

        if(obj == Py_False)
            return false;

        // this is so silly
        assert(obj == Py_True);
        return true;
    }

    std::string tuplexTypeToCharacter(python::Type type) {
        if(type == python::Type::STRING) return "s";
        else if(type == python::Type::I64) return "i";
        else if(type == python::Type::F64) return "f";
        else if(type == python::Type::BOOLEAN) return "b";
        else {
            Logger::instance().defaultLogger().error("Unsupported type for dictionary key/value: " + type.desc());
            return "";
        }
    }

    tuplex::Field createPickledField(PyObject* obj) {
        using namespace tuplex;

        // try to serialize via cloudpickle, use None for non-cloudpickable objects...
        auto pickled_obj = python::pickleObject(python::getMainModule(), obj);
        // error?
        if(PyErr_Occurred()) {
            PyErr_Clear();

            // get name of object
            auto type_obj = PyObject_Type(obj); assert(type_obj);
            auto name = python::PyString_AsString(type_obj);
            Logger::instance().defaultLogger().error("tried to cloudpickle object of type " + name + ", did not succeed. Replacing with None.");
            return Field::null();
        }

        auto f = Field::from_pickled_memory(reinterpret_cast<const uint8_t*>(pickled_obj.c_str()), pickled_obj.size());
        return f;
    }

    tuplex::Field pythonToField(PyObject *obj, bool autoUpcast) {
        using namespace tuplex;
        using namespace std;

        assert(holdsGIL());

        // @Todo: if obj == nullptr, return None!
        assert(obj);

        // make sure python interpreter is initialized
        assert(Py_IsInitialized());

        // do exact checks here
        // check if obj is a tuple or not
        if(PyTuple_CheckExact(obj)) {
            // recursive
            auto numElements = PyTuple_Size(obj);

            // special case: empty tuple?
            if(0 == numElements)
                return Field::empty_tuple();

            vector<Field> v;
            v.reserve(numElements);
            for(unsigned i = 0; i < numElements; ++i) {
                v.push_back(pythonToField(PyTuple_GetItem(obj, i), autoUpcast));
            }
            return Field(Tuple::from_vector(v));
        } else if(PyBool_Check(obj)) { // important to call this before isinstance long since isinstance also return long for bool
            // boolean
            return Field(PyBool_AsBool(obj));
        }
        else if(PyLong_CheckExact(obj)) {
            // is it an integer that doesn't fit into 8 bytes?
            static_assert(sizeof(long long) == sizeof(int64_t), "long long is not 64bit");
            auto value = PyLong_AsLongLong(obj);
            if(PyErr_Occurred()) {
                // too long, mark as pyobject
                PyErr_Clear();
                return createPickledField(obj);
            }
            // i64
            return Field((int64_t)value);

        } else if(PyFloat_CheckExact(obj)) {
            // f64
            return Field(PyFloat_AS_DOUBLE(obj));
        }  else if(PyUnicode_Check(obj)) { // allow subtypes because of general access
            // string
            // maybe check in future also for PyBytes_Type
            return Field(PyUnicode_AsUTF8(obj));
        } else if (PyDict_Check(obj)) { // allow subtypes because of general access
            // represent now as struct dict (or homogenous dict if possible)
            return py_dict_to_field(obj, autoUpcast);

//            auto dictType = mapPythonClassToTuplexType(obj, autoUpcast);
//            std::string dictStr;
//            PyObject *key = nullptr, *val = nullptr;
//            Py_ssize_t pos = 0;  // must be initialized to 0 to start iteration, however internal iterator variable. Don't use semantically.
//            dictStr += "{";
//            while(PyDict_Next(obj, &pos, &key, &val)) {
//                // create key
//                auto keyStr = PyString_AsString(PyObject_Str(key));
//                auto keyType = mapPythonClassToTuplexType(key, autoUpcast);
//                python::Type valType;
//
//                // create value, mimicking cJSON printing standards
//                std::string valStr;
//                if(PyObject_IsInstance(val, (PyObject*)&PyUnicode_Type)) {
//                    valType = python::Type::STRING;
//                    valStr = PyString_AsString(val);
//                    // explicitly escape the escape characters
//                    for(int i = valStr.length() - 1; i>=0; i--) {
//                        char escapeChar = '0';
//                        switch(valStr[i]) {
//                            case '\"': escapeChar = '\"'; break;
//                            case '\\': escapeChar = '\\'; break;
//                            case '\b': escapeChar = 'b'; break;
//                            case '\f': escapeChar = 'f'; break;
//                            case '\n': escapeChar = 'n'; break;
//                            case '\r': escapeChar = 'r'; break;
//                            case '\t': escapeChar = 't'; break;
//                        }
//                        if(escapeChar != '0') {
//                            valStr[i] = '\\';
//                            valStr.insert(i+1, 1, escapeChar);
//                        }
//                    }
//                    valStr = "\"" + valStr + "\"";
//                } else if (PyObject_IsInstance(val, (PyObject *) &PyLong_Type) ||
//                           PyObject_IsInstance(val, (PyObject *) &PyFloat_Type)) {
//                    double dval;
//                    if(PyObject_IsInstance(val, (PyObject *) &PyLong_Type)) {
//                        valType = python::Type::I64;
//                        dval = PyLong_AsDouble(val);
//                    }
//                    else {
//                        valType = python::Type::F64;
//                        dval = PyFloat_AsDouble(val);
//                    }
//
//                    // TODO: cJSON does not support nan/inf
//                    // taken from cJSON::print_number
//                    unsigned char number_buffer[32];
//                    int length = 0;
//                    if (dval * 0 != 0) { // check for nan or +/- infinity. This special vals are encoded as NULL
//                        length = sprintf((char*)number_buffer, "null");
//                    } else {
//                        double test = 0.0;
//                        // Try 15 decimal places of precision to avoid nonsignificant nonzero digits
//                        length = sprintf((char*)number_buffer, "%1.15g", dval);
//
//                        // Check whether the original double can be recovered
//                        if ((sscanf((char*)number_buffer, "%lg", &test) != 1) || ((double)test != dval)) {
//                            // If not, print with 17 decimal places of precision
//                            length = sprintf((char*)number_buffer, "%1.17g", dval);
//                        }
//                    }
//                    // sprintf failed or buffer overrun occurred
//                    if ((length < 0) || (length > (int)(sizeof(number_buffer) - 1))) {
//                        Logger::instance().defaultLogger().error("Failure when serializing python dictionary to cJSON");
//                        throw std::runtime_error("Failure when serializing python dictionary to cJSON");
//                    }
//
//                    valStr = std::string((char*)number_buffer);
//                } else if(PyObject_IsInstance(obj, (PyObject*)&PyBool_Type)) {
//                    valType = python::Type::BOOLEAN;
//                    valStr = (PyBool_AsBool(val)) ? "true" : "false";
//                } else {
//                    Logger::instance().defaultLogger().error("Unsupported type for dictionary value: " + valType.desc());
//                    throw std::runtime_error("Unsupported type for dictionary value: " + valType.desc());
//                }
//                std::string newKeyStr = tuplexTypeToCharacter(keyType) + tuplexTypeToCharacter(valType) + keyStr;
//
//                dictStr += "\"";
//                dictStr += newKeyStr;
//                dictStr += "\":";
//                dictStr += valStr;
//                dictStr += ",";
//            }
//            dictStr = dictStr.substr(0, dictStr.length() - 1) + "}";
//            return Field::from_str_data(dictStr, dictType);
        } else if(PyList_Check(obj)) {
            // recursive
            auto numElements = PyList_Size(obj);
            if(0 == numElements)
                return Field::empty_list();

            vector<Field> v;
            v.reserve(numElements);
            for(unsigned i = 0; i < numElements; ++i) {
                v.push_back(pythonToField(PyList_GET_ITEM(obj, i), autoUpcast));
            }
            return Field(List::from_vector(v));
        } else if(obj == Py_None) {
            return Field::null();
        } else {
            return createPickledField(obj);
        }
    }

    tuplex::Field py_dict_to_field(PyObject* dict_obj, bool autoUpcast) {
        using namespace tuplex;
        using namespace std;

        assert(PyDict_Check(dict_obj));

        // shortcut: empty dict?
        if(PyDict_Size(dict_obj) == 0)
            return Field::empty_dict();

        // @TODO: could also just write a function in python to decode this complicated structure (i.e. getting the type).
        // may save dev time.


        // below is likely some complex code, write later. For now - treat as pyobjects.
        Py_XINCREF(dict_obj);
        auto serialized = python::pickleObject(python::getMainModule(), dict_obj);
        return Field::from_pickled_memory(reinterpret_cast<const uint8_t *>(serialized.c_str()), serialized.size());

//        // iterate over dictionary and map accordingly
//        std::stringstream ss;
//        std::vector<StructEntry> entries;
//        PyObject *key = nullptr, *val = nullptr;
//        Py_ssize_t pos = 0;  // must be initialized to 0 to start iteration, however internal iterator variable. Don't use semantically.
//        ss << "{";
//        while(PyDict_Next(dict_obj, &pos, &key, &val)) {
//            // convert key to str
//            Py_XINCREF(key);
//            auto key_str = PyString_AsString(key);
//            Py_XINCREF(key);
//            auto key_type = py
//        }
//
//        auto dict_str = ss.str();
//        auto dict_type = python::Type::makeStructuredDictType(entries);
//        return Field::from_str_data(dict_str, dict_type);
    }

    tuplex::Field fieldCastTo(const tuplex::Field& f, const python::Type& type) {
        if(f.getType() == type)
            return f;

        // bool to i64? or f64?
        if(f.getType() == python::Type::BOOLEAN) {
            if(type == python::Type::I64) {
                return tuplex::Field((int64_t)f.getInt());
            }
            if(type == python::Type::F64) {
                return tuplex::Field((double)f.getInt());
            }
        }

        // i64 to f64?
        if(f.getType() == python::Type::I64 && type == python::Type::F64)
            return tuplex::Field((double)f.getInt());

        throw std::runtime_error("Casting field " + f.getType().desc() + " to " + type.desc() + " failed.");
    }

    /*!
     * converts object to field of specified type.
     * @param obj
     * @param type
     * @param autoUpcast whether to upcast numeric types to a unified type when type conflicts, false by default
     * @return Field object
     */
    tuplex::Field pythonToField(PyObject *obj, const python::Type &type, bool autoUpcast=false) {
        assert(obj);

        // TODO: check assumptions about whether nonempty tuple can be an option
        if(type.isOptionType()) {
            if (obj == Py_None) {
                return tuplex::Field::null(type);
            } else {
                tuplex::Field f;
                auto rtType = type.getReturnType();
                if(rtType.isListType() || rtType.isTupleType()) {
                    // type still needed to correctly construct field
                    f = pythonToField(obj, rtType, autoUpcast);
                } else {
                    // simple types
                    f = pythonToField(obj, autoUpcast);
                    f = autoUpcast? fieldCastTo(f, type.getReturnType()) : f;
                }
                f.makeOptional();
                return f;
            }
        } else if(type.isTupleType() && type != python::Type::EMPTYTUPLE) {
            // recursive
            auto numElements = PyTuple_Size(obj);

            std::vector<tuplex::Field> v;
            for(unsigned i = 0; i < numElements; ++i) {
                v.push_back(pythonToField(PyTuple_GetItem(obj, i), type.parameters()[i], autoUpcast));
            }
            return tuplex::Field(tuplex::Tuple::from_vector(v));
        } else if(type.isListType() && type != python::Type::EMPTYLIST) {
            auto numElements = PyList_Size(obj);
            auto elementType = type.elementType();
            std::vector<tuplex::Field> v;
            v.reserve(numElements);
            for(unsigned i = 0; i < numElements; ++i) {
                auto currListItem = PyList_GetItem(obj, i);
                v.push_back(pythonToField(currListItem, elementType, autoUpcast));
                Py_IncRef(currListItem);
            }
            return tuplex::Field(tuplex::List::from_vector(v));
        } else {
            auto f = pythonToField(obj, autoUpcast);
            return autoUpcast ? fieldCastTo(f, type) : f;
        }
    }

    tuplex::Row pythonToRow(PyObject *obj, const python::Type &type, bool autoUpcast) {
        assert(obj);

        tuplex::Field f = pythonToField(obj, type, autoUpcast);

        // unpack the tuples one level
        if(f.getType().isTupleType() && f.getType() != python::Type::EMPTYTUPLE) {
            tuplex::Tuple t = *((tuplex::Tuple*)f.getPtr());
            std::vector<tuplex::Field> fields;
            for(unsigned i = 0; i < t.numElements(); ++i)
                fields.push_back(t.getField(i));
            return tuplex::Row::from_vector(fields);
        } else return tuplex::Row(f);
    }

    tuplex::Row pythonToRow(PyObject* obj) {
        using namespace tuplex;

        assert(obj);

        assert(holdsGIL());

        // need to unnest one level (a little stupid but oh well)
        Field f = pythonToField(obj);

        if(f.getType().isTupleType() && f.getType() != python::Type::EMPTYTUPLE) {
            Tuple t = *((Tuple*)f.getPtr());
            std::vector<Field> fields;
            for(unsigned i = 0; i < t.numElements(); ++i)
                fields.push_back(t.getField(i));
            return Row::from_vector(fields);
        } else return Row(f);
    }

    PyObject* PyObj_FromCJSONKey(const char* serializedKey) {
        assert(serializedKey);
        assert(strlen(serializedKey) >= 2);
        const char *keyString = serializedKey + 2;
        switch(serializedKey[0]) {
            case 's':
                return PyString_FromString(keyString);
            case 'b':
                if(strcmp(keyString, "True") == 0) {
                    Py_XINCREF(Py_True);
                    return Py_True;
                }
                if(strcmp(keyString, "False") == 0) {
                    Py_XINCREF(Py_False);
                    return Py_False;
                }
                Logger::instance().defaultLogger().error("invalid boolean key: " + std::string(keyString) + ", returning Py_None");
                Py_XINCREF(Py_None);
                return Py_None;
            case 'i':
                return PyLong_FromString(keyString, nullptr, 10);
            case 'f':
                return PyFloat_FromDouble(strtod(keyString, nullptr));
            default:
                Logger::instance().defaultLogger().error("unknown type " + std::string(serializedKey)
                                                         + " in field encountered. Returning Py_None");
                Py_XINCREF(Py_None);
                return Py_None;
        }
    }

    PyObject* PyObj_FromCJSONVal(const cJSON* obj, const char type) {
        switch(type) {
            case 's':
                return PyString_FromString(obj->valuestring);
            case 'b':
                if(cJSON_IsTrue(obj)) {
                    Py_XINCREF(Py_True);
                    return Py_True;
                } else {
                    Py_XINCREF(Py_False);
                    return Py_False;
                }
            case 'i':
                return PyLong_FromDouble(obj->valuedouble);
            case 'f':
                return PyFloat_FromDouble(obj->valuedouble);
//            auto pipFunc = pip.getFunction();
//
//            if(!pipFunc)
//                return nullptr;
//
//            // debug
//#define PRINT_EXCEPTION_PROCESSING_DETAILS
//
//            auto generalCaseType = pip.inputRowType();
            default:
                std::string errorString = "unknown type identifier" + std::string(&type) + " in field encountered. Returning Py_None";
                Logger::instance().defaultLogger().error(errorString);
                Py_XINCREF(Py_None);
                return Py_None;
        }
    }

    PyObject* PyDict_FromCJSON(const cJSON* dict) {
        auto dictObj = PyDict_New();
        cJSON* cur_item = dict->child;
        while(cur_item) {
            char *key = cur_item->string;
            auto keyObj = PyObj_FromCJSONKey(key);
            auto valObj = PyObj_FromCJSONVal(cur_item, key[1]);
            PyDict_SetItem(dictObj, keyObj, valObj);
            cur_item = cur_item->next;
        }
        return dictObj;
    }

    PyObject* fieldToPython(const tuplex::Field& f) {
        using namespace tuplex;

        assert(python::holdsGIL());

        auto t = f.getType();

        // option type
        if(t.isOptionType() && f.isNull()) {
            Py_XINCREF(Py_None);
            return Py_None;
        } else {
            t = t.isOptionType() ? t.getReturnType() : t;
        }

        if(t == python::Type::BOOLEAN) {
            if(f.getInt() > 0) {
                Py_XINCREF(Py_True);
                return Py_True;
            } else {
                Py_XINCREF(Py_False);
                return Py_False;
            }
        } else if(t == python::Type::I64) {
            return PyLong_FromLongLong(f.getInt());
        } else if(t == python::Type::F64) {
            return PyFloat_FromDouble(f.getDouble());
        } else if(t == python::Type::STRING) {
            return PyString_FromString((const char *) f.getPtr());
        } else if(t == python::Type::NULLVALUE) {
            Py_XINCREF(Py_None);
            return Py_None;
        } else if(t == python::Type::EMPTYTUPLE) {
            return PyTuple_New(0);
        } else if(t == python::Type::EMPTYDICT) {
            return PyDict_New();
        } else if(t == python::Type::GENERICDICT || f.getType().isDictionaryType()) {
            if(f.getType().isStructuredDictionaryType())
                throw std::runtime_error("struct dict not yet implemented in fieldToPython - fix this!");

            cJSON* dptr = cJSON_Parse((char*)f.getPtr());
            return PyDict_FromCJSON(dptr);
        } else if(t.isListType()) {
            auto list = (List*)f.getPtr();
            auto numElements = list->numElements();

            auto listObj = PyList_New(numElements);
            for(unsigned i = 0; i < numElements; ++i) {
                PyList_SET_ITEM(listObj, i, fieldToPython(list->getField(i)));
            }

#ifndef NDEBUG
            // check
            for(unsigned i = 0; i < numElements; ++i) {
                auto item = PyList_GET_ITEM(listObj, i);
                assert(item);
                assert(item->ob_refcnt > 0);
            }
#endif

            return listObj;
        } else if(t.isTupleType()) {
            // recursive construction
            auto params = f.getType().parameters();
            auto numElements = params.size();

            auto tuple = (Tuple*)f.getPtr();
            assert(tuple);
            assert(tuple->numElements() == numElements);

            auto tupleObj = PyTuple_New(numElements);
            for(unsigned i = 0; i < tuple->numElements(); ++i) {
                PyTuple_SetItem(tupleObj, i, fieldToPython(tuple->getField(i)));
            }

#ifndef NDEBUG
            // check
            for(unsigned i = 0; i < numElements; ++i) {
                auto item = PyTuple_GET_ITEM(tupleObj, i);
                assert(item);
                assert(item->ob_refcnt > 0);
            }
#endif

            return tupleObj;
        } else if(t == python::Type::PYOBJECT) {
            // use cloudpickle to deserialize
            assert(f.getPtr());
            auto obj = python::deserializePickledObject(python::getMainModule(), static_cast<const char *>(f.getPtr()), f.getPtrSize());
            if(PyErr_Occurred()) {
                PyErr_Clear();
                Py_XINCREF(Py_None);
                return Py_None;
            }
            assert(obj); assert(obj->ob_refcnt > 0);
            return obj;
        } else {
            Logger::instance().defaultLogger().error("unknown type " + f.getType().desc()
            + " in field encountered. Returning Py_None");
            Py_XINCREF(Py_None);
            return Py_None;
        }

#ifndef NDEBUG
        std::cerr<<"fieldToPython returning nullptr, this should not happen..."<<std::endl;
#endif
        return nullptr;
    }

    PyObject* rowToPython(const tuplex::Row& row, bool unpackPrimitives) {
        //assert(holdsGIL());

        // always return a tuple
        auto type = row.getRowType();

        // row type is always a tuple type.
        assert(type.isTupleType());

        if(type.parameters().size() == 1 && unpackPrimitives) {
            // simple. primitive type stored
            // unpack
            return fieldToPython(row.get(0));
        } else {
            // a tuple is stored
            auto numElements = type.parameters().size();
            auto tupleObj = PyTuple_New(numElements);
            for(unsigned i = 0; i < numElements; ++i) {
                PyObject* field = fieldToPython(row.get(i));
                PyTuple_SetItem(tupleObj, i, field); // set steals reference, so inc by one
            }
            return tupleObj;
        }
    }

    tuplex::ExceptionCode translatePythonExceptionType(PyObject* type) {
        assert(type);
        using namespace tuplex;

        assert(holdsGIL());

        // make sure python interpreter is initialized
        assert(Py_IsInitialized());

        // @Todo: Use here PyErr_ExceptionMatches --> more important!!!
#warning "use here PyErr_ExceptionMatches"
        // confer https://docs.python.org/3/library/exceptions.html#bltin-exceptions
        // and https://docs.python.org/3/c-api/exceptions.html
        if(type == PyExc_AssertionError)
            return ExceptionCode::ASSERTIONERROR;
        if(type == PyExc_AttributeError)
            return ExceptionCode::ATTRIBUTEERROR;
        if(type == PyExc_EOFError)
            return ExceptionCode::EOFERROR;
        // version specific, see for definition in patchlevel.h file
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 6)
        if(type == PyExc_ModuleNotFoundError)
            return ExceptionCode::MODULENOTFOUNDERROR;
#endif

        if(type == PyExc_ImportError)
            return ExceptionCode::IMPORTERROR;

        if(type == PyExc_NameError)
            return ExceptionCode::NAMEERROR;
        if(type == PyExc_TypeError)
            return ExceptionCode::TYPEERROR;

        // translation to errors also raised by compiled code
        if(type == PyExc_ZeroDivisionError)
            return ExceptionCode::ZERODIVISIONERROR;
        if(type == PyExc_IndexError)
            return ExceptionCode::INDEXERROR;
        if(type == PyExc_KeyError)
            return ExceptionCode::KEYERROR;

        if(type == PyExc_ValueError)
            return ExceptionCode::VALUEERROR;

        auto etype = PyObject_GetAttrString(type, "__name__");
        auto etype_name = python::PyString_AsString(etype);

        Py_XDECREF(etype);

        // try to translate via name lookup
        auto ec = pythonClassToExceptionCode(etype_name);
        if(ec != ExceptionCode::UNKNOWN)
            return ec;

        Logger::instance().defaultLogger().error("Unknown Python Exception '" + etype_name + "' occurred, flagging as unknown");
        return ExceptionCode::UNKNOWN;
    }

    PyObject* callFunction(PyObject* pFunc, PyObject* pArgs, tuplex::ExceptionCode& ec) {
        // make sure python interpreter is initialized
        assert(Py_IsInitialized());
        assert(holdsGIL());

        assert(pFunc);
        assert(PyCallable_Check(pFunc));
        assert(pArgs);
        assert(PyTuple_Check(pArgs));
        assert(!PyErr_Occurred());

        // there are different ways to call a function
        // either, the function has a single positional argument --> wrap in another tuple
        // or multiple ones
        size_t numPositionalArguments = pythonFunctionPositionalArgCount(pFunc);
        PyObject* args = nullptr;
        PyObject* resObj = nullptr;
        PyObject *type=nullptr, *value=nullptr, *traceback=nullptr;

        // need to translate pArgs into args
        if(numPositionalArguments > 1) {
            args = pArgs; // should be correct?
        } else if(1 == numPositionalArguments) {
            // exactly one argument!

            // is pArgs having more than one element? need to wrap in tuple
            if(PyTuple_Size(pArgs) > 1) {
                args = PyTuple_New(1);
                Py_INCREF(pArgs);
                PyTuple_SET_ITEM(args, 0, pArgs);
            } else {
                args = pArgs;
            }
        } else {
            // 0 positional arguments?
            // ==> i.e. constant expression
            args = PyTuple_New(0); // call with no params
        }

        assert(args);
        resObj = PyObject_CallObject(pFunc, args);
        ec = tuplex::ExceptionCode::SUCCESS;

        // check whether exceptions are raised
        // translate to Tuplex exception code & clear python error trace.
        if(PyErr_Occurred()) {
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            ec = translatePythonExceptionType(type);

             // // uncomment these lines to debug what happened in detail
             // std::cout<<"pArgs is: "; PyObject_Print(pArgs, stdout, 0);
             // std::cout<<"args is: "; PyObject_Print(args, stdout, 0);
             // PyErr_Restore(type, value, traceback);
             // PyErr_Print();
             // // flush
             // std::cout.flush();

            Py_XDECREF(type);
            Py_XDECREF(value);
            Py_XDECREF(traceback);
            PyErr_Clear();
        }

        // decref if args != pArgs
        if(args != pArgs)
            Py_XDECREF(args);

        return resObj;
    }


    PyObject* callFunctionWithDict(PyObject* pFunc,
            PyObject* pArgs,
            const std::vector<std::string>& columns,
            tuplex::ExceptionCode& ec) {
        assert(!PyErr_Occurred());

        assert(pFunc);
        assert(pArgs);

        // call function by supplying a single argument, i.e. the dict constructed out of pArgs and columns
        if(!PyTuple_Check(pArgs))
            return nullptr;

        auto num_columns = PyTuple_Size(pArgs);
        if(num_columns != columns.size()) {
            std::stringstream ss;
            ss<<"column number mismatch in callFunctionWithDict: "
              <<"number of columns provided is "<<columns.size()<<" but PyObject* arg has "<<num_columns<<" elements.";
            Logger::instance().defaultLogger().error(ss.str());
            throw std::runtime_error(ss.str());
            return nullptr;
        }

        // create python dict
        PyObject *dictObj = PyDict_New();
        for(int i = 0; i < num_columns; ++i) {
            auto item = PyTuple_GET_ITEM(pArgs, i);
            Py_XINCREF(item);
            PyDict_SetItemString(dictObj, columns[i].c_str(), item);
        }


        // create tuple arg object
        PyObject *args = PyTuple_New(1);
        PyTuple_SET_ITEM(args, 0, dictObj);

        // call function...
        PyObject *resObj = PyObject_CallObject(pFunc, args);
        ec = tuplex::ExceptionCode::SUCCESS;

        // check errs & translate to exception code...
        // check whether exceptions are raised
        // translate to Tuplex exception code & clear python error trace.
        if(PyErr_Occurred()) {
            PyObject *type, *value, *traceback;
            PyErr_Fetch(&type, &value, &traceback);
            ec = translatePythonExceptionType(type);

            Py_XDECREF(type);
            Py_XDECREF(value);
            Py_XDECREF(traceback);
            PyErr_Clear();
        }

        Py_XDECREF(args); // steals ref from dict

        return resObj;
    }


    python::Type detectTypeWithSample(PyObject* pFunc, const std::vector<tuplex::Row>& vSample) {
        using namespace tuplex;
        assert(pFunc);
        assert(holdsGIL());

        if(!PyCallable_Check(pFunc)) {
            Logger::instance().defaultLogger().error("supplied python object is not a function");
            return python::Type::UNKNOWN;
        }

        std::vector<python::Type> retrieved_types;
        for(const auto& row : vSample) {
            // decode as python object
            auto obj = rowToPython(row);

            ExceptionCode ec;
            auto res = callFunction(pFunc, obj, ec);
            if(ec == ExceptionCode::SUCCESS) {
                assert(res);
                // only count successful examples
                auto resType = pythonToRow(res).getRowType();
                retrieved_types.push_back(resType);
            }
        }

        if(retrieved_types.empty()) {
            Logger::instance().defaultLogger().error("provided sample only produced exceptions, could not determine type");
            return python::Type::UNKNOWN;
        }

        assert(retrieved_types.size() > 0);
        auto majorityType = mostFrequentItem(retrieved_types);

        return majorityType;
    }

    size_t pythonFunctionPositionalArgCount(PyObject* func) {
        assert(holdsGIL());

        if(!PyCallable_Check(func)) {
            Logger::instance().defaultLogger().error("python object is not a function. Can't determine argcount");
            return 0;
        }

        PyObject* codeObj = PyObject_GetAttrString(func, "__code__");
        assert(codeObj);
        PyObject* argcountObj = PyObject_GetAttrString(codeObj, "co_argcount");
        assert(argcountObj);
        return PyLong_AsLong(argcountObj);
    }

    std::string pythonFunctionGetName(PyObject* func) {
        assert(holdsGIL());

        if(!PyCallable_Check(func)) {
            Logger::instance().defaultLogger().error("python object is not a function. Can't determine argcount");
            return "";
        }

        PyObject* codeObj = PyObject_GetAttrString(func, "__code__");
        assert(codeObj);
        PyObject* nameObj = PyObject_GetAttrString(codeObj, "co_name");
        assert(nameObj);

        std::string name(PyUnicode_AsUTF8(nameObj));

        if(name == "<lambda>")
            name = "lambda";

        return name;
    }

    bool isInterpreterRunning() {
        return Py_IsInitialized();
    }


    void tracebackAndClearError(PythonCallResult& pcr, PyObject* func) {
        PyObject *type, *value, *traceback;
        PyErr_Fetch(&type, &value, &traceback);

        assert(type && value); // traceback might be nullptr for one liners.

        using namespace std;

        // https://docs.python.org/3/c-api/object.html
        PyObject* e_msg = PyObject_Str(value);
        PyObject* e_type = PyObject_GetAttrString(type, "__name__");
        PyObject* e_lineno = traceback ? PyObject_GetAttrString(traceback, "tb_lineno") : nullptr;
        pcr.exceptionMessage = python::PyString_AsString(e_msg);
        pcr.exceptionClass = python::PyString_AsString(e_type);
        pcr.exceptionLineNo = e_lineno ? PyLong_AsLong(e_lineno) : 0;
        pcr.exceptionCode = translatePythonExceptionType(type);
        Py_XDECREF(e_msg);
        Py_XDECREF(e_type);
        Py_XDECREF(e_lineno);
        // walk up traceback
        // PyObject *tbnext = traceback;
        // while(tbnext) {
        //     cout<<"tb line no"<<PyLong_AsLong(PyObject_GetAttrString(tbnext, "tb_lineno"))<<endl;
        //     cout<<"tb frame"<<python::PyString_AsString(PyObject_Str(PyObject_GetAttrString(tbnext, "tb_frame")))<<endl;
        //     tbnext = PyObject_GetAttrString(tbnext, "tb_next");
        // }
        // could use
        // try:
        //    ...
        //except:
        //    exc_type, exc_value, exc_traceback = sys.exc_info()
        //    t = traceback.extract_tb(exc_traceback)

        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(traceback);
        PyErr_Clear();

        // fetch potential offset
        if(func) {
            auto dict = PyObject_GetAttrString(func, "__dict__");
            auto offset = PyDict_GetItemString(dict, "line_offset");

            if(offset) {

              // special case: func name is lambda
              if(pcr.functionName == "<lambda>" && pcr.exceptionLineNo < 0)
                pcr.exceptionLineNo = 0;

              // Python 3.10+ changed exception counting. I.e., it's relative to start, 1 indexed now.
              int i_offset = PyLong_AsLong(offset);
              // want to yield exception number relative to full file!
              // -> i.e., let's say exceptionLineNo is 1 and i_offset is 2.
              // then pcr.exceptionLineNo should be 1 + 2 = 3
              pcr.exceptionLineNo += i_offset;
              pcr.functionFirstLineNo += i_offset;
            }
        }
    }

    PythonCallResult callFunctionWithDictEx(PyObject* pFunc, PyObject* pArgs, const std::vector<std::string>& columns) {
        // make sure python interpreter is initialized
        assert(Py_IsInitialized());
        assert(holdsGIL());

        assert(pFunc);
        assert(PyCallable_Check(pFunc));
        assert(pArgs);
        assert(PyTuple_Check(pArgs));
        assert(!PyErr_Occurred());

        // check size of tuple & columns match
        assert(PyTuple_Size(pArgs) ==  columns.size());

        PythonCallResult pcr;
        auto pFuncName = PyObject_GetAttrString(pFunc, "__name__");
        auto pFuncCodeObj = PyObject_GetAttrString(pFunc, "__code__");
        // get file & firstlineno
        auto pFuncFile = PyObject_GetAttrString(pFuncCodeObj, "co_filename");
        auto pFuncFirstLineNo = PyObject_GetAttrString(pFuncCodeObj, "co_firstlineno");
        assert(pFuncName);
        pcr.functionName = python::PyString_AsString(pFuncName);
        pcr.functionFirstLineNo = PyLong_AsLong(pFuncFirstLineNo);
        pcr.file = python::PyString_AsString(pFuncFile);
        Py_XDECREF(pFuncName);
        Py_XDECREF(pFuncCodeObj);
        Py_XDECREF(pFuncFile);
        Py_XDECREF(pFuncFirstLineNo);

        // create python dict
        PyObject *dictObj = PyDict_New();
        for(int i = 0; i < columns.size(); ++i)
            PyDict_SetItemString(dictObj, columns[i].c_str(), PyTuple_GET_ITEM(pArgs, i));

        // create tuple arg object
        PyObject *args = PyTuple_New(1);
        PyTuple_SET_ITEM(args, 0, dictObj);

        // call function...
        PyObject *resObj = PyObject_CallObject(pFunc, args);

        // check errs & translate to exception code...
        // check whether exceptions are raised
        // translate to Tuplex exception code & clear python error trace.
        if(PyErr_Occurred())
            tracebackAndClearError(pcr, pFunc);

        Py_XDECREF(args); // steals ref from dict
        pcr.res = resObj;
        return pcr;
    }


    PythonCallResult callFunctionEx(PyObject* pFunc, PyObject* pArgs, PyObject* kwargs) {
        // make sure python interpreter is initialized
        assert(Py_IsInitialized());
        assert(holdsGIL());

        assert(pFunc);
        assert(PyCallable_Check(pFunc));
        assert(pArgs);
        assert(PyTuple_Check(pArgs));
        assert(!PyErr_Occurred());

        PythonCallResult pcr;
        auto pFuncName = PyObject_GetAttrString(pFunc, "__name__");
        assert(pFuncName);
        auto pFuncCodeObj = PyObject_GetAttrString(pFunc, "__code__");
        assert(pFuncCodeObj);
        // get file & firstlineno
        auto pFuncFile = PyObject_GetAttrString(pFuncCodeObj, "co_filename");
        assert(pFuncFile);
        auto pFuncFirstLineNo = PyObject_GetAttrString(pFuncCodeObj, "co_firstlineno");
        assert(pFuncFirstLineNo);
        assert(pFuncName);
        pcr.functionName = python::PyString_AsString(pFuncName);
        pcr.functionFirstLineNo = PyLong_AsLong(pFuncFirstLineNo);
        pcr.file = python::PyString_AsString(pFuncFile);
        Py_XDECREF(pFuncName);
        Py_XDECREF(pFuncCodeObj);
        Py_XDECREF(pFuncFile);
        Py_XDECREF(pFuncFirstLineNo);

        // there are different ways to call a function
        // either, the function has a single positional argument --> wrap in another tuple
        // or multiple ones
        size_t numPositionalArguments = python::pythonFunctionPositionalArgCount(pFunc);
        PyObject* args = nullptr;
        PyObject* resObj = nullptr;
        PyObject *type=nullptr, *value=nullptr, *traceback=nullptr;


        // need to translate pArgs into args
        if(numPositionalArguments > 1) {
            args = pArgs; // should be correct?
        } else if(1 == numPositionalArguments) {
            // exactly one argument!

            // is pArgs having more than one element? need to wrap in tuple
            if(PyTuple_Size(pArgs) > 1) {
                args = PyTuple_New(1);
                PyTuple_SET_ITEM(args, 0, pArgs);
            } else {
                args = pArgs;
            }
        } else {
            // 0 positional arguments?
            // ==> i.e. constant expression
            args = PyTuple_New(0); // call with no params
        }

        assert(args);
        if(kwargs)
            pcr.res = PyObject_Call(pFunc, args, kwargs);
        else
            pcr.res = PyObject_CallObject(pFunc, args);

        // check whether exceptions are raised
        // translate to Tuplex exception code & clear python error trace.
        if(PyErr_Occurred())
            tracebackAndClearError(pcr, pFunc);

        // decref if args != pArgs
        if(args != pArgs)
            Py_XDECREF(args);

        return pcr;
    }

    // mapping type to internal types, unknown as default
    python::Type mapPythonClassToTuplexType(PyObject *o, bool autoUpcast) {
        assert(o);

        if(Py_None == o)
            return python::Type::NULLVALUE;

        if(PyBool_Check(o))
            return python::Type::BOOLEAN;

        if(PyLong_CheckExact(o))
            return python::Type::I64;

        if(PyFloat_CheckExact(o))
            return python::Type::F64;

        if(PyUnicode_CheckExact(o))
            return python::Type::STRING;

        if(PyTuple_CheckExact(o)) {
            std::vector<python::Type> elementTypes;

            // extract further, break for unsupported types
            int numElements = PyTuple_Size(o);

            if(numElements == 0)
                return python::Type::EMPTYTUPLE;

            for(int j = 0; j < numElements; j++) {
                auto item = PyTuple_GET_ITEM(o, j); // borrowed reference
                assert(item->ob_refcnt > 0); // important!!!
                elementTypes.push_back(mapPythonClassToTuplexType(item, autoUpcast));
            }
            return python::TypeFactory::instance().createOrGetTupleType(elementTypes);
        }

        // subdivide type for this into a fixed key type/fixed arg type dict and a variable one
        // if(cls.compare("dict") == 0) {}
        if(PyDict_CheckExact(o)) {
            int numElements = PyDict_Size(o);
            if(numElements == 0)
                return python::Type::EMPTYDICT; // be specific, empty dict!

            python::Type keyType, valType;
            PyObject *key = nullptr, *val = nullptr;
            Py_ssize_t pos = 0; // must be initialized to 0 to start iteration, however internal iterator variable. Don't use semantically.
            bool types_set = false; // need extra var here b/c vals could be unknown.
            while(PyDict_Next(o, &pos, &key, &val)) {
                auto curKeyType = mapPythonClassToTuplexType(key, autoUpcast);
                auto curValType = mapPythonClassToTuplexType(val, autoUpcast);
                if(!types_set) {
                    types_set = true;
                    keyType = curKeyType;
                    valType = curValType;
                } else {
                    if(keyType != curKeyType) return python::Type::GENERICDICT;
                    if(valType != curValType) return python::Type::GENERICDICT;
                }
            }
            return python::TypeFactory::instance().createOrGetDictionaryType(keyType, valType);
        }

        // subdivide this into a list with identical elements and a variable typed list...
        // if(cls.compare("list") == 0) {}
        if(PyList_CheckExact(o)) {
            int numElements = PyList_Size(o);
            if(numElements == 0)
                return python::Type::EMPTYLIST;

            python::Type elementType = mapPythonClassToTuplexType(PyList_GetItem(o, 0), autoUpcast);
            // verify that all elements have the same type
            for(int j = 0; j < numElements; j++) {
                python::Type currElementType = mapPythonClassToTuplexType(PyList_GetItem(o, j), autoUpcast);
                if(elementType != currElementType) {
                    // possible to use nullable type as element type?
                    tuplex::TypeUnificationPolicy policy; policy.allowAutoUpcastOfNumbers = autoUpcast;
                    auto newElementType = tuplex::unifyTypes(elementType, currElementType, policy);
                    if (newElementType == python::Type::UNKNOWN) {
                        Logger::instance().defaultLogger().error("list with variable element type " + elementType.desc() + " and " + currElementType.desc() + " not supported.");
                        return python::Type::PYOBJECT;
                    }
                    elementType = newElementType;
                }
            }
            return python::Type::makeListType(elementType);
        }

        // match object
        auto name = typeName(o);
        if(o->ob_type->tp_name == "re.Match")
            return python::Type::MATCHOBJECT;

        if("module" ==  name)
            return python::Type::MODULE;

        // check if serializable via cloudpickle, if so map!
#ifndef NDEBUG
        auto pickled_obj = python::pickleObject(python::getMainModule(), o);
        if(!PyErr_Occurred()) {
            return python::Type::PYOBJECT;
        } else {
            PyErr_Clear();
        }
#endif

        return python::Type::PYOBJECT;
    }

    // @TODO: inc type objects??
    PyObject* encodePythonSchema(const python::Type& t) {
        // unknown?
        // maybe special type?
        // -> use None!
        if(python::Type::UNKNOWN == t) {
            Py_XINCREF(Py_None);
            return Py_None;
        }
        if(python::Type::BOOLEAN == t)
            return reinterpret_cast<PyObject *>(&PyBool_Type);
        if(python::Type::I64 == t)
            return reinterpret_cast<PyObject*>(&PyLong_Type);
        if(python::Type::F64 == t)
            return reinterpret_cast<PyObject*>(&PyFloat_Type);
        if(python::Type::STRING == t)
            return reinterpret_cast<PyObject*>(&PyUnicode_Type);
        if(python::Type::NULLVALUE == t) {
            Py_XINCREF(Py_None);
            auto none = Py_None;
            auto typeobj = reinterpret_cast<PyObject*>(Py_None->ob_type);
            Py_XDECREF(none);
            return typeobj;
        }

        // empty tuple handled below...
        if(python::Type::GENERICTUPLE == t)
            return reinterpret_cast<PyObject*>(&PyTuple_Type);
        if(python::Type::EMPTYLIST == t || python::Type::GENERICLIST == t)
            return reinterpret_cast<PyObject*>(&PyList_Type);
        if(python::Type::EMPTYDICT == t || python::Type::GENERICDICT == t)
            return reinterpret_cast<PyObject*>(&PyDict_Type);

        // fetch typing module for composable types
        auto mod = python::getMainModule();
        auto typing_mod = PyImport_AddModule("typing");
        assert(typing_mod);
        auto typing_dict = PyModule_GetDict(typing_mod);
        assert(typing_dict);

        // option?
        if(t.isOptionType()) {
            auto tobj = encodePythonSchema(t.getReturnType());

            // encode via typing.Optional, e.g.
            // typing.Optional[str] which is equal to typing.Union[str, NoneType]
            auto typing_optional = PyDict_GetItemString(typing_dict, "Optional");
            assert(typing_optional);
            if (t.getReturnType().isTupleType()) {
                // https://docs.python.org/3/library/typing.html#typing.Tuple
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 9)
                // use builtin.tuple[...]
                auto builtin_mod = PyImport_AddModule("builtins");
                assert(builtin_mod);
                auto builtin_dict = PyModule_GetDict(builtin_mod);
                assert(builtin_dict);
                auto tuple = PyDict_GetItemString(builtin_dict, "tuple");
                tobj = PyObject_GetItem(tuple, tobj);
#else
                // use Tuple[...]
                auto typing_tuple = PyDict_GetItemString(typing_dict, "Tuple");
                assert(typing_tuple);
                tobj = PyObject_GetItem(typing_tuple, tobj);
#endif
            }
            auto opt_type = PyObject_GetItem(typing_optional, tobj);
            return opt_type;
        }

        if(t.isDictionaryType()) {
            auto tkey = encodePythonSchema(t.keyType());
            auto tval = encodePythonSchema(t.valueType());

            auto typing_dictobj = PyDict_GetItemString(typing_dict, "Dict");
            auto targ = PyTuple_New(2);
            PyTuple_SET_ITEM(targ, 0, tkey);
            PyTuple_SET_ITEM(targ, 1, tval);

            auto dict_type = PyObject_GetItem(typing_dictobj, targ);
            return dict_type;
        }

        if(t.isListType()) {
            auto tel = encodePythonSchema(t.elementType());

            auto typing_listobj = PyDict_GetItemString(typing_dict, "List");
            auto list_type = PyObject_GetItem(typing_listobj, tel);
            return list_type;
        }

        if(t.isFunctionType()) {
            auto targs = encodePythonSchema(t.getParamsType());
            auto tret = encodePythonSchema(t.getReturnType());

            // callable is encoded as list. I.e. convert tuple!
            if(PyTuple_Check(targs)) {
                auto listobj = PyList_New(PyTuple_Size(targs));
                for(unsigned i = 0; i < PyTuple_Size(targs); ++i)
                    PyList_SetItem(listobj, i, PyTuple_GetItem(targs, i));
                targs = listobj;
            } else {
                // propagate to list!
                auto listobj = PyList_New(1);
                PyList_SetItem(listobj, 0, targs);
                targs = listobj;
            }

            auto typing_callable = PyDict_GetItemString(typing_dict, "Callable");
            auto arg = PyTuple_New(2);
            PyTuple_SET_ITEM(arg, 0, targs);
            PyTuple_SET_ITEM(arg, 1, tret);

            auto func_type = PyObject_GetItem(typing_callable, arg);
            return func_type;
        }

        if(t.isTupleType()) {
            // Note: there are multiple ways to encode tuples.
            // either implicit via (...)
            // or using typing.Tuple (deprecated for python >= 3.9)
            // or via tuple[int,...,str] (from Python 3.9)
            // ==> use (...) syntax.
            auto tuple_obj = PyTuple_New(t.parameters().size());
            for(unsigned i = 0; i < t.parameters().size(); ++i) {
                PyTuple_SET_ITEM(tuple_obj, i, encodePythonSchema(t.parameters()[i]));
            }
            return tuple_obj;
        }

        // composed types, use typing module for that
        throw std::runtime_error("unsupported type found");
    }


    python::Type decodePythonSchema(PyObject *schemaObject) {

        assert(schemaObject);

        // use string function to decode this...
        // ==> more error proof than the other method

        using namespace std;
        using namespace tuplex;
        unordered_map<string, python::Type> lookup{{"<class 'bool'>", python::Type::BOOLEAN},
                                                   {"bool", python::Type::BOOLEAN},
                                                   {"<class 'int'>", python::Type::I64},
                                                   {"int", python::Type::I64},
                                                   {"<class 'float'>", python::Type::F64},
                                                   {"float", python::Type::F64},
                                                   {"<class 'str'>", python::Type::STRING},
                                                   {"str", python::Type::STRING},
                                                   {"<class 'tuple'>", python::Type::GENERICTUPLE},
                                                   {"tuple", python::Type::GENERICTUPLE},
                                                   {"<class 'dict'>", python::Type::GENERICDICT},
                                                   {"dict", python::Type::GENERICDICT}};

        assert(schemaObject);

        // check first for primitives.
        if(schemaObject == Py_None)
            return python::Type::UNKNOWN;

        // check lookup dict, if match return.
        string typeStr = PyUnicode_AsUTF8(PyObject_Str(schemaObject));
        auto it = lookup.find(typeStr);
        if(it != lookup.end())
            return it->second;

        // i.e. typing modules exhibit form typing.Tuple[float, float] e.g.
        // check if a tuple or list is given (convenience for iterating values, will be tuple anyways in tuplex)
        if(PyTuple_Check(schemaObject) || PyList_Check(schemaObject)) {
            // iterate
            auto numElements = PySequence_Size(schemaObject);
            if(numElements < 0)
                throw std::runtime_error("sequence without length found in decodePythonSchema");
            if(0 == numElements)
                return python::Type::EMPTYTUPLE;

            // go over elements recursively
            vector<python::Type> types;
            for(int i = 0; i < numElements; ++i) {
                types.emplace_back(decodePythonSchema(PySequence_GetItem(schemaObject, i)));
            }
            return python::Type::makeTupleType(types);
        }

        // empty dict special case...?
        if(PyDict_Check(schemaObject) && PyDict_Size(schemaObject) == 0)
            return python::Type::EMPTYDICT;

        // typing module strings (decode them here)
        // ==> class should be _GenericAlias (typing module)
        if(strStartsWith(typeStr, "typing.")) {

            // shortcut: Any
            if(strStartsWith(typeStr, "typing.Any"))
                return python::Type::PYOBJECT;

            // so far support for Tuple[...] and Dict[...]
            // future: List[...]
            // decoding via _name and __args__
            PyObject* args = nullptr;

#if PY_VERSION_HEX < 0x03060000
            // fetching weird argnames
            // note: in Python 3.5. it's called __tuple_params__ for typing.Tuple

            if(strStartsWith(typeStr, "typing.Tuple"))
                args = PyObject_GetAttrString(schemaObject, "__tuple_params__");
            else if(strStartsWith(typeStr, "typing.Dict") || strStartsWith(typeStr, "typing.List"))
                args = PyObject_GetAttrString(schemaObject, "__args__");

#elif PY_VERSION_HEX < 0x03050000
#error "minimum of Python3.5 required to compile Tuplex"
#else
            args = PyObject_GetAttrString(schemaObject, "__args__");
#endif
            assert(args);
            assert(PyTuple_Check(args));
            // note: Need to do weird string start because typing changed so much between 3.5, 3.6, 3.7, ... -.-

            if(strStartsWith(typeStr, "typing.Tuple")) {
                // decode from args ==> is a tuple
                vector<python::Type> types;
                auto numElements = PyTuple_Size(args);
                for(int i = 0; i < numElements; ++i)
                    types.emplace_back(decodePythonSchema(PyTuple_GetItem(args, i)));
                return python::Type::makeTupleType(types);
            } else if(strStartsWith(typeStr, "typing.Dict")) {
                // tuple of two elements
                assert(PyTuple_Size(args) == 2);
                auto keyClass = PyTuple_GetItem(args, 0);
                auto valClass = PyTuple_GetItem(args, 1);
                assert(keyClass && valClass);
                auto keyType = decodePythonSchema(keyClass);
                auto valType = decodePythonSchema(valClass);

                return python::Type::makeDictionaryType(keyType, valType);
            } else if(strStartsWith(typeStr, "typing.List")) {
                python::Type elementType = decodePythonSchema(PyList_GetItem(args, 0));
                return python::Type::makeListType(elementType);
            } else if(strStartsWith(typeStr, "typing.Union") || strStartsWith(typeStr, "typing.Optional")) {
                if(PyTuple_Size(args) == 2) {
                    auto c1 = PyTuple_GetItem(args, 0);
                    auto c2 = PyTuple_GetItem(args, 1);
                    assert(c1 && c2);
                    auto t1 = decodePythonSchema(c1);
                    auto t2 = decodePythonSchema(c2);
                    auto s1 = PyUnicode_AsUTF8(PyObject_Str(c1));
                    auto s2 = PyUnicode_AsUTF8(PyObject_Str(c2));

                    std::string none_str = "<class 'NoneType'>";
                    if(s1 == none_str || s2 == none_str) {
                        auto non_null = s1 == none_str ? t2 : t1;
                        return python::Type::makeOptionType(non_null);
                    } else {
                        throw std::runtime_error(
                                "Tuplex can't understand typing module annotation " + typeStr + ": union of python::Type (" +
                                t1.desc() + ", " + t2.desc() + "), or string (" + s1 + ", " + s2 + ")");
                    }
                } else {
                    throw std::runtime_error("Tuplex can't understand typing module annotation " + typeStr +
                                             ": only Optional unions are understood right now");
                }
            } else {

                // whichever other typing annotations to decode...

                // Add them here...
            }

            // unknown typing module annotation
            throw std::runtime_error("Tuplex can't understand typing module annotation " + typeStr);
        }

        return python::Type::UNKNOWN;
    }


    PyObject *runAndGet(const std::string &code, const std::string &name) {

        if(code.empty())
            return nullptr;

        auto main_mod = getMainModule();
        auto main_dict = PyModule_GetDict(main_mod);

        PyRun_String(code.c_str(), Py_file_input, main_dict, main_dict);
        if(PyErr_Occurred()) {
            // fetch error, and throw runtime err
            handlePythonErrors();
#warning "better python error formatting needed!"
        }
        PyObject *obj = nullptr;
        if(!name.empty())
            obj = PyDict_GetItemString(main_dict, name.c_str());
        if(PyErr_Occurred()) {
            // fetch error, and throw runtime err
            handlePythonErrors();
#warning "better python error formatting needed!"
        }

        // important to incref!
        Py_XINCREF(obj);
        return obj;
    }

    std::string platformExtensionSuffix() {
        // basically execute following code:
        // import distutils.sysconfig
        // print(distutils.sysconfig.get_config_var('EXT_SUFFIX')
        if(Py_IsInitialized()) {
            auto res = runAndGet("import distutils.sysconfig; ext_suffix = distutils.sysconfig.get_config_var('EXT_SUFFIX')", "ext_suffix");
            if(!res) {
                std::cerr<<"no result, returning empty string. Internal error?"<<std::endl;
                return "";
            }
            return PyString_AsString(res);
        } else {
            std::cerr<<"calling platformExtensionSuffix - but interpreter is not running. Internal error?"<<std::endl;
        }
        return "";
    }

    // maj.min.patch
    static std::tuple<int, int, int> extract_version(const std::string& s) {
      using namespace std;
      vector<string> v;
      tuplex::splitString(s, '.', [&v](const std::string& part) { v.push_back(part); });
      assert(v.size() == 3);
      return make_tuple(std::stoi(v[0]), std::stoi(v[1]), std::stoi(v[2]));
    }

    bool cloudpickleCompatibility(std::ostream *os) {
      std::stringstream err;
      assert(python::holdsGIL());
      PyObject *mainModule = PyImport_AddModule("__main__");

#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 10)
      // should be 2.1.0+
      std::string desired_version = ">=2.1.0";
#else
      std::string desired_version = "<2.0.0";
#endif

      // check for error
      if(PyErr_Occurred()) {
        PyErr_Print();
        std::cout<<std::endl;
        return false;
      }

      // import cloudpickle for serialized functions
      PyObject *cloudpickleModule = PyImport_ImportModule("cloudpickle");
      if(!cloudpickleModule) {
          err<<"could not find cloudpickle module, please install via `pip3 install \""<<desired_version<<"\"`.";
          if(os)
            *os<<err.str();
          return false;
      }

      PyModule_AddObject(mainModule, "cloudpickle", cloudpickleModule);
      auto versionObj =  PyObject_GetAttr(cloudpickleModule, PyString_FromString("__version__"));
      if(!versionObj)
        return false;

      auto version_string = PyString_AsString(versionObj);

      auto version = extract_version(version_string);
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 10)
      // should be 2.1.0+
      if(std::get<0>(version) < 2 || std::get<1>(version) < 1) {
        err<<"minimum required cloudpickle version to use with "<<PY_VERSION<<" is 2.1.0";
        if(os)
          *os<<err.str();
        return false;
      }
#else
      // should be <2.0.0
      if(std::get<0>(version) >= 2) {
        err<<"cloudpickle version to use with "<<PY_VERSION<<" has to be less than 2.0.0";
        if(os)
          *os<<err.str();
        return false;
      }
#endif
      return true;
    }

    void runGC() {
        // import gc module to access debug garbage collector information
        //PyObject* gcModule = PyImport_AddModule("gc");
        // call gc.set_debug(gc.DEBUG_LEAK)
        // PyRun_SimpleString("import gc");
        // PyRun_SimpleString("gc.set_debug(gc.DEBUG_LEAK)");
        // PyRun_SimpleString("gc.disable()");

        PyObject* gcModule = PyImport_ImportModule("gc");
        assert(gcModule);
        auto gc_dict = PyModule_GetDict(gcModule);
        assert(gc_dict);
        auto gc_collect = PyDict_GetItemString(gc_dict, "collect");
        assert(gc_collect);
        auto arg = PyTuple_New(1);
        PyTuple_SET_ITEM(arg, 0, PyLong_FromLong(2)); // specify here collect level, 2 for full!
        PyObject_CallObject(gc_collect, arg);
    }
}
