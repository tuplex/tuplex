//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonWrappers.h>

namespace tuplex {

    boost::python::object PyObj_FromCJSONKey(const char* serializedKey) {
        assert(serializedKey);
        assert(strlen(serializedKey) >= 2);
        const char *keyString = serializedKey + 2;
        switch(serializedKey[0]) {
            case 's':
                return boost::python::object(std::string(keyString));
            case 'b':
                if(strcmp(keyString, "True") == 0)
                    return boost::python::object(true);
                if(strcmp(keyString, "False") == 0)
                    return boost::python::object(false);
                Logger::instance().defaultLogger().error("invalid boolean key: " + std::string(keyString) + ", returning Py_None");
                return boost::python::object();
            case 'i':
                return boost::python::object(strtoll(keyString, nullptr, 10));
            case 'f':
                return boost::python::object(strtod(keyString, nullptr));
            default:
                return boost::python::object();
        }
    }

    boost::python::object PyObj_FromCJSONVal(const cJSON* obj, const char type) {
        switch(type) {
            case 's':
                return boost::python::object(std::string(obj->valuestring));
            case 'b':
                return boost::python::object(static_cast<bool>(cJSON_IsTrue(obj)));
            case 'i':
                return boost::python::object(static_cast<int64_t>(obj->valuedouble));
            case 'f':
                return boost::python::object(obj->valuedouble);
            default:
                return boost::python::object();
        }
    }

    boost::python::object PyDict_FromCJSON(const cJSON* dict) {
        auto dictObj = boost::python::dict();
        cJSON* cur_item = dict->child;
        while(cur_item) {
            char *key = cur_item->string;
            auto keyObj = PyObj_FromCJSONKey(key);
            auto valObj = PyObj_FromCJSONVal(cur_item, key[1]);
            dictObj[keyObj] = valObj;
            cur_item = cur_item->next;
        }
        return dictObj;
    }

    boost::python::object fieldToPython(const tuplex::Field& f) {
        if(python::Type::BOOLEAN == f.getType())
            return boost::python::object(static_cast<bool>(f.getInt()));
        else if(python::Type::I64 == f.getType())
            return boost::python::object(f.getInt());
        else if(python::Type::F64 == f.getType())
            return boost::python::object(f.getDouble());
        else if(python::Type::STRING == f.getType())
            return boost::python::object(std::string((char*)f.getPtr()));
        else if(python::Type::GENERICDICT == f.getType() || f.getType().isDictionaryType()) {
            cJSON* dptr = cJSON_Parse((char*)f.getPtr());
            return PyDict_FromCJSON(dptr);
        } else if(f.getType().isListType()) {
            boost::python::list L;
            auto list = *((tuplex::List*)f.getPtr());
            for(int i=0; i<list.numElements(); ++i) {
                L.append(fieldToPython(list.getField(i)));
            }
            return L;
        }
        else {
            if(f.getType().isTupleType()) {
                boost::python::list L;
                auto tuple = *((tuplex::Tuple*)f.getPtr());
                for(int i = 0; i < tuple.numElements(); ++i) {
                    L.append(fieldToPython(tuple.getField(i)));
                }
                return boost::python::tuple(L);
            } else
                return boost::python::object();

        }
    }

    boost::python::object tupleToPython(const tuplex::Tuple& tuple) {
        boost::python::list L;
        for(int i = 0; i < tuple.numElements(); ++i) {
            L.append(fieldToPython(tuple.getField(i)));
        }
        return boost::python::tuple(L);
    }

    boost::python::object rowToPython(const tuplex::Row& row) {
        //return tuple with stuff in it (or if schema is simple, directly the elements...
        boost::python::list L;
        for(int col = 0; col < row.getNumColumns(); ++col) {
            if(python::Type::BOOLEAN == row.getType(col))
            L.append<bool>(row.getBoolean(col));
            else if(python::Type::I64 == row.getType(col))
                L.append<int64_t>(row.getInt(col));
            else if(python::Type::F64 == row.getType(col))
                L.append<double>(row.getDouble(col));
            else if(python::Type::STRING == row.getType(col))
                L.append<const char*>(row.getString(col).c_str());
            else {
                if(row.getType(col).isTupleType()) {
                    L.append(tupleToPython(row.getTuple(col)));
                } else
                 Logger::instance().logger("python").error("unknown type '" + row.getType(col).desc() + "' detected, could not convert to python object");
            }
        }

        // special case: zero or one element
        if(0 == row.getNumColumns())
            return boost::python::make_tuple();
        if(1 == row.getNumColumns())
            return L[0];

        // convert list to tuple
        return boost::python::tuple(L);
    }



    PyObject* fastResultSetToCPython(ResultSet* rs) {
        assert(rs);

        // fast conversion for simple types.
        size_t rowCount = rs->rowCount();


        auto rowType = rs->schema().getRowType();
        assert(rowType != python::Type::UNKNOWN);

        python::Type type = rowType;
        // if single type, reset by one
        assert(type.isTupleType());
        if(type.parameters().size() == 1)
            type = type.parameters().front();

        if(type == python::Type::F64) {



        }  else if(type.isTupleType() && type != python::Type::EMPTYTUPLE) {
            if(python::tupleElementsHaveSameType(type)) {

                assert(type.parameters().size() > 0);

                // we can speed up construction here by a lot
                auto elemType = type.parameters().front();

                if(elemType == python::Type::F64) {

                    size_t numTupleElements = type.parameters().size();

                    PyObject *listObj = PyList_New(rowCount);

                    Partition* partition = nullptr;
                    size_t pos = 0;
                    while(rs->hasNextPartition()) {
                        partition = rs->getNextPartition();

                        // add memory towards list object
                        auto ptr = partition->lockRaw();
                        auto end_ptr = ptr + partition->size();
                        size_t numRows = *((const int64_t*)ptr);
                        ptr += sizeof(int64_t);

                        double* dataptr = (double*)ptr;
                        for(unsigned i = 0; i < numRows; ++i) {

                            PyObject* tupleObj = PyTuple_New(numTupleElements);
                            for(unsigned j = 0; j < numTupleElements; ++j) {
                                PyTuple_SET_ITEM(tupleObj, j, PyFloat_FromDouble(*dataptr));
                                dataptr++;
                            }

                            // we know it is a double & there is a single output scheme only, so make this a fast convert!
                            PyList_SET_ITEM(listObj, pos++, tupleObj);
                        }

                        partition->unlock();

                        // free partition (pointer is managed by Executor)s
                        partition->invalidate();
                    }
                    return listObj;


                } else {
                    Logger::instance().defaultLogger().error("fastResult does not yet implement deserialization for multi tuple type " + elemType.desc());
                    exit(1);
                }
            }
        } else {
                Logger::instance().defaultLogger().error("fastResult does not yet implement deserialization for type " + rowType.desc());
                exit(1);
        }


        return nullptr;

    }

    ClosureEnvironment closureFromDict(PyObject* obj) {
        using namespace std;

        ClosureEnvironment ce;

        if(!obj || Py_None == obj)
            return ce; // empty

        if(!PyDict_Check(obj))
            throw std::runtime_error("object is not a dict, fail!");

        // // import json module
        // auto json_mod = PyImport_ImportModule("json");
        // if(!json_mod)
        //     throw std::runtime_error("could not find builtin json module!");
        // auto json_dict = PyModule_GetDict(json_mod);
        // auto json_dumps = PyDict_GetItemString(json_dict, "dumps");

        // iterate over dict
        // for k, v in d['__globals__'].items():
        //    if 'module' == str(type(v).__name__):
        //        print(k) # identifier
        //        print(v.__name__) # original identifier
        //        print(v.__file__) # location
        //        print(v.__package__) # package => empty for builtin?
        //        print('----------')
        //    else:
        //        print(k)
        //        print(type(v).__name__)
        //        print(v)
        PyObject *key = nullptr, *val = nullptr;
        Py_ssize_t pos = 0;  // must be initialized to 0 to start iteration, however internal iterator variable. Don't use semantically.
        while(PyDict_Next(obj, &pos, &key, &val)) {

            assert(PyUnicode_Check(key));
            auto identifier = python::PyString_AsString(key);

            auto& logger = Logger::instance().defaultLogger();
            // is val module?
            if(PyModule_CheckExact(val)) {
                // to extract module properties use module functions as described in https://docs.python.org/3/c-api/module.html

                ClosureEnvironment::Module m;
                m.identifier = identifier;
                m.original_identifier = PyModule_GetName(val);
                if(PyErr_Occurred()) {
                    cerr<<"failed to extract module name (__name__)"<<endl;
                    PyErr_Clear();
                }

                auto file_name_obj = PyModule_GetFilenameObject(val);
                if(!file_name_obj || PyErr_Occurred()) {
                    cerr<<"failed to extract module file (__file__)"<<endl;
                    PyErr_Clear();
                    m.location = "";
                } else {
                    m.location = python::PyString_AsString(file_name_obj);
                }

                auto module_dict = PyModule_GetDict(val);
                if(!module_dict || PyErr_Occurred()) {
                    PyErr_Clear();
                    cerr<<"could not retrieve __dict__ of module "<<m.original_identifier<<endl;
                }

                // get package
                if(module_dict) {
                    auto attr_package = PyObject_GetAttrString(val, "__package__");
                    if(!attr_package || PyErr_Occurred()) {
                        PyErr_Clear();
                        cerr<<"could not retrieve __package__ of module "<<m.original_identifier<<endl;
                    } else {
                        m.package = python::PyString_AsString(attr_package);
                    }
                }
                logger.debug("ClosureEnvironment: import " + m.original_identifier + " as " + m.identifier);
                ce.addModule(m);
            } else if(PyCallable_Check(val)) {
                ClosureEnvironment::Function f;
                f.identifier = identifier;

                auto pkg_obj = PyObject_GetAttrString(val, "__module__"); // i.e. the module
                auto qual_name_obj = PyObject_GetAttrString(val, "__qualname__");
                auto name_obj = PyObject_GetAttrString(val, "__name__");
                auto file_obj = PyObject_GetAttrString(val, "__globals__");
                if(file_obj)
                    file_obj = PyDict_GetItemString(file_obj, "file");

                if(qual_name_obj)
                    f.qualified_name = python::PyString_AsString(qual_name_obj);
                else if(name_obj)
                    f.qualified_name = python::PyString_AsString(name_obj);
                if(pkg_obj)
                    f.package = python::PyString_AsString(pkg_obj);
                if(file_obj)
                    f.location = python::PyString_AsString(file_obj);
                logger.debug("ClosureEnvironment: from " + f.package + " import " + f.qualified_name + " as " + f.identifier);
                ce.addFunction(f);
            } else {
                ClosureEnvironment::Constant c;
                c.identifier = identifier;
                c.type = python::mapPythonClassToTuplexType(val);

                c.value = python::pythonToField(val);
                // // call json.dumps
                // auto arg = PyTuple_New(1); PyTuple_SetItem(arg, 0, val);
                // auto resObj = PyObject_CallObject(json_dumps, arg);
                // if(!resObj || PyErr_Occurred()) {
                //     PyErr_Clear();
                //     throw std::runtime_error("could not convert value " + python::PyString_AsString(val) + " to json string");
                // }
                // c.json_value = python::PyString_AsString(resObj);

                // deprecated
                // c.pickled_value = python::pickleObject(python::getMainModule(), val);
                //auto string_value = python::PyString_AsString(python::deserializePickledObject(python::getMainModule(), c.json_value.c_str(), c.json_value.size()));

                auto string_value = c.value.desc();
                logger.debug("ClosureEnvironment: " + c.identifier + " = " + string_value);
                ce.addConstant(c);
            }
        }

        return ce;
    }
}