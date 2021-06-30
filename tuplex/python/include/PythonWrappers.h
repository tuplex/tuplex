//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONWRAPPERS_H
#define TUPLEX_PYTHONWRAPPERS_H

#include <boost/python/list.hpp>
#include <Logger.h>
#include <boost/python/extract.hpp>
#include <TypeSystem.h>
#include <boost/python/tuple.hpp>
#include <ErrorDataSet.h>
#include <ClosureEnvironment.h>
#include "PythonException.h"

#include <PythonHelpers.h>
#include <PythonSerializer.h>

template <typename Container> boost::python::list STL_to_Python(const Container& vec) {
    typedef typename Container::value_type T;
    boost::python::list lst;
    std::for_each(vec.begin(), vec.end(), [&](const T& t) { lst.append(t); });
    return lst;
}

namespace tuplex {
    extern ClosureEnvironment closureFromDict(PyObject* obj);
}


/*!
 * helper function to extract columns from a string
 * @param listObj list of strings in python or None
 * @param object_name object name for which to display an error.
 * @return vector of strings. empty vector for none
 */
inline std::vector<std::string> extractFromListOfStrings(PyObject *listObj, const std::string& object_name="") {
    auto& logger = Logger::instance().logger("python");

    // get columns if they exist
    std::vector<std::string> v;
    if(listObj && listObj != Py_None) {
        // check what type colObj is (list of strings?)
        if(PyList_Check(listObj)) {
            auto num_cols = PyList_Size(listObj);
            // get columns
            for(unsigned i = 0; i < num_cols; ++i) {
                auto item = PyList_GetItem(listObj, i);
                Py_INCREF(item);

                if(!PyUnicode_Check(item)) {
                    Py_XDECREF(item);
                    throw std::runtime_error(object_name + " object must be None or list of strings");
                }

                std::string col = python::PyString_AsString(item);
                v.emplace_back(col);
            }
        } else {
            logger.warn(object_name + " object should be None or list.");
        }
    }
    return v;
}

/*!
 * extracts type hints from python objects from a dict either indexed by strings or integers.
 */
inline std::unordered_map<size_t, python::Type> extractIndexBasedTypeHints(PyObject * dictObj, const std::vector<std::string>& columns, const std::string& object_name = "") {
    assert(dictObj);
    auto& logger = Logger::instance().logger("python");

    std::unordered_map<size_t, python::Type> m;
    if(dictObj != Py_None) {
        if(PyDict_CheckExact(dictObj)) {
            PyObject *key = nullptr, *val = nullptr;
            Py_ssize_t pos = 0;
            while(PyDict_Next(dictObj, &pos, &key, &val)) {
                if(PyLong_CheckExact(key)) {
                    size_t keyVal = PyLong_AsSize_t(key);
                    python::Type valType = python::decodePythonSchema(val);
                    m[keyVal] = valType;
                } else if(PyUnicode_Check(key)) {
                    auto name = python::PyString_AsString(key);

                    // check if contained in columns, if not print warning.
                    size_t idx = 0;
                    for(idx = 0; idx < columns.size() && name != columns[idx]; ++idx);
                    if(columns.empty() || idx > columns.size()) {
                        logger.warn("received type hint for column '" + name + "', but could not find column. Ignoring hint.");
                    } else {
                        // encode as int
                        python::Type valType = python::decodePythonSchema(val);
                        m[idx] = valType;
                    }
                } else {
                    throw std::runtime_error("Invalid typing hints! non-integer key");
                }
            }
        } else {
            logger.warn(object_name + " object should be None or dictionary");
        }
    }
    return m;
}

inline std::unordered_map<std::string, python::Type> extractColumnBasedTypeHints(PyObject * dictObj,
                                                                        const std::vector<std::string> &columns,
                                                                        const std::string &object_name = "") {
    assert(dictObj);
    auto &logger = Logger::instance().logger("python");

    std::unordered_map<std::string, python::Type> m;
    if(dictObj != Py_None) {
        if(PyDict_CheckExact(dictObj)) {
        PyObject *key = nullptr, *val = nullptr;
        Py_ssize_t pos = 0;
        while(PyDict_Next(dictObj, &pos, &key, &val)) {
                if(PyLong_CheckExact(key)) {
                   // ignore...
                } else if(PyUnicode_Check(key)) {
                    auto name = python::PyString_AsString(key);

                    python::Type valType = python::decodePythonSchema(val);
                    m[name] = valType;
                } else {
                    throw std::runtime_error("Invalid typing hints! non-integer key");
                }
            }
        } else {
            logger.warn(object_name + " object should be None or dictionary");
        }
    }
    return m;
}

// helper functions
namespace tuplex {
    extern boost::python::object fieldToPython(const tuplex::Field& f);

    extern boost::python::object tupleToPython(const tuplex::Tuple& tuple);

    extern boost::python::object rowToPython(const tuplex::Row& row);


}

#endif //TUPLEX_PYTHONWRAPPERS_H