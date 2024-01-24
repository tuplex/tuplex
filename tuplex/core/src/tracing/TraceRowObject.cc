//
// Created by leonhards on 4/6/23.
//

#include <tracing/TraceRowObject.h>
#include <PythonHelpers.h>

namespace tuplex {
    Py_ssize_t TraceRowObject::length() {
        return items.size();
    }

    PyObject *TraceRowObject::getItem(PyObject *key) {
        assert(key);

        // what type is key? string or integer?
        if(PyUnicode_Check(key)) {
            auto str_key = python::PyString_AsString(key);

            // search in columns, else key error!
            auto it = std::find(columns.begin(), columns.end(), str_key);
            if(it != columns.end()) {
                auto offset = std::distance(columns.begin(), it);
                if(offset >= items.size()) {
                    PyErr_SetString(PyExc_KeyError, "invalid offset detected");
                    return nullptr;
                } else {
                    auto item = items[offset];
                    Py_XINCREF(item);
                    return item;
                }
            }

            PyErr_SetString(PyExc_KeyError, ("could not find key '" + str_key + "' in row object").c_str());
        } else if(PyLong_Check(key)) {
            auto offset = PyLong_AsLong(key);
            if(offset < 0 || offset >= items.size()) {
                PyErr_SetString(PyExc_KeyError, "invalid key");
                return nullptr;
            } else {
                auto item = items[offset];
                Py_XINCREF(item);
                return item;
            }

        } else {
            // key error
            PyErr_SetString(PyExc_KeyError, "could not find key in row object");
        }

        return nullptr;
    }

    PyObject *TraceRowObject::setDefault(PyObject *key, PyObject *value) {
        throw std::runtime_error("not yet implemetned");

        return nullptr;
    }

    int TraceRowObject::putItem(PyObject *key, PyObject *value) {
        throw std::runtime_error("not yet supported");

        return 0;
    }

    TraceRowObject *
    TraceRowObject::create(const std::vector<PyObject *> items, const std::vector<std::string> &columns) {
        // lazy init type
        if(TraceRowObjectType.tp_dict == nullptr) {
            if(PyType_Ready(&TraceRowObjectType) < 0)
                throw std::runtime_error("initializing internal trace row type failed");
            Py_INCREF(&TraceRowObjectType);
            assert(TraceRowObjectType.tp_dict);

            // should we register type as well with main module?
        }
        Py_INCREF(&TraceRowObjectType);
        auto o = (TraceRowObject*)PyType_GenericNew(&TraceRowObjectType, nullptr, nullptr);
        if(!o) {
            Py_DECREF(&TraceRowObjectType);
            return nullptr;
        }

        o->items = items;
        o->columns = columns;
        return o;
    }

    python::Type TraceRowObject::rowType(bool autoUpcast) const {
        // map each element!
        std::vector<python::Type> col_types;
        for(auto item : items)
            col_types.push_back(python::mapPythonClassToTuplexType(item, autoUpcast));
        return python::Type::makeTupleType(col_types);
    }
}
