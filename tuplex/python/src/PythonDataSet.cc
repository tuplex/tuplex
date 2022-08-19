//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonDataSet.h>
#include <PythonWrappers.h>
#include <cstdio>
#include <CSVUtils.h>
#include <utils/Signals.h>
#include <limits>

#ifdef NDEBUG
#define LARGE_RESULT_SIZE 1000000ul
#else
#define LARGE_RESULT_SIZE 100000ul
#endif

namespace tuplex {
    py::object PythonDataSet::collect() {

        // make sure a dataset is wrapped
        assert(this->_dataset);

        // is callee error dataset? if so return list with error string
        if (this->_dataset->isError()) {
            ErrorDataSet *eds = static_cast<ErrorDataSet *>(this->_dataset);
            py::list L;
            L.append(eds->getError());
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            return L;
        } else {

            std::stringstream ss;
            // release GIL & hand over everything to Tuplex
            assert(PyGILState_Check()); // make sure this thread holds the GIL!
            python::unlockGIL();

            std::shared_ptr<ResultSet> rs;
            std::string err_message = "";
            try {
                rs = _dataset->collect(ss);
                if(!rs)
                    throw std::runtime_error("invalid result set");
                // if there are more than 1 million (100k in debug mode) elements print message...
                if (rs->rowCount() > LARGE_RESULT_SIZE)
                    Logger::instance().logger("python").info("transferring "
                                                             + std::to_string(rs->rowCount()) +
                                                             " elements back to Python. This might take a while...");
            } catch(const std::exception& e) {
                err_message = e.what();
                Logger::instance().defaultLogger().error(err_message);
            } catch(...) {
                err_message = "unknown C++ exception occurred, please change type.";
                Logger::instance().defaultLogger().error(err_message);
            }

            // ==> add noexcept statements!
            // reqacquire GIL
            python::lockGIL();

            // error? then return list of error string
            if(!rs || !err_message.empty()) {
                // Logger::instance().flushAll();
                Logger::instance().flushToPython();
                auto listObj = PyList_New(1);
                PyList_SetItem(listObj, 0, python::PyString_FromString(err_message.c_str()));
                return py::reinterpret_borrow<py::list>(listObj);
            }
            // collect results & transfer them back to python
            // new version, directly interact with the interpreter
            Timer timer;
            // build python list object from resultset
            auto listObj = resultSetToCPython(rs.get(), std::numeric_limits<size_t>::max());

#ifndef NDEBUG
            // check validity of each object
            assert(PyList_Check(listObj));
            auto num_elements = PyList_Size(listObj);
            for(int i = 0; i < num_elements; ++i) {
                // check element is valid!
                auto item = PyList_GET_ITEM(listObj, i);
                assert(item);
                assert(item->ob_refcnt > 0);
            }
            assert(listObj->ob_refcnt > 0);
#endif

            Logger::instance().logger("python").info("Data transfer back to Python took "
                                                     + std::to_string(timer.time()) + " seconds");

            auto list = py::reinterpret_borrow<py::list>(listObj);
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();

            // print errors
            if (ss.str().length() > 0)
                PySys_FormatStdout("%s", ss.str().c_str());

            return list;
        }
    }

    py::object PythonDataSet::take(const int64_t numRows) {
        // make sure a dataset is wrapped
        assert(this->_dataset);

        // is callee error dataset? if so return list with error string
        if (this->_dataset->isError()) {
            ErrorDataSet *eds = static_cast<ErrorDataSet *>(this->_dataset);
            py::list L;
            L.append(eds->getError());
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            return L;
        } else {

            std::stringstream ss;

            // release GIL & hand over everything to Tuplex
            assert(PyGILState_Check()); // make sure this thread holds the GIL!
            python::unlockGIL();

            std::shared_ptr<ResultSet> rs;
            std::string err_message = "";
            try {
                rs = _dataset->take(numRows, ss);
                if(!rs)
                    throw std::runtime_error("invalid result set");
                // if there are more than 1 million (100k in debug mode) elements print message...
                if (rs->rowCount() > LARGE_RESULT_SIZE)
                    Logger::instance().logger("python").info("transferring "
                                                             + std::to_string(rs->rowCount()) +
                                                             " elements back to Python. This might take a while...");
            } catch(const std::exception& e) {
                err_message = e.what();
                Logger::instance().defaultLogger().error(err_message);
            } catch(...) {
                err_message = "unknown C++ exception occurred, please change type.";
                Logger::instance().defaultLogger().error(err_message);
            }

            // reqacquire GIL
            python::lockGIL();

            // error? then return list of error string
            if(!rs || !err_message.empty()) {
                // Logger::instance().flushAll();
                Logger::instance().flushToPython();
                auto listObj = PyList_New(1);
                PyList_SetItem(listObj, 0, python::PyString_FromString(err_message.c_str()));
                return py::reinterpret_borrow<py::list>(listObj);
            }

            // collect results & transfer them back to python
            // new version, directly interact with the interpreter
            Timer timer;
            // build python list object from resultset
            auto listObj = resultSetToCPython(rs.get(), numRows);
            Logger::instance().logger("python").info("Data transfer back to python took "
                                                     + std::to_string(timer.time()) + " seconds");
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();

            // print errors
            if (ss.str().length() > 0)
                PySys_FormatStdout("%s", ss.str().c_str());

            return py::reinterpret_borrow<py::list>(listObj);
        }
    }

    PythonDataSet PythonDataSet::map(const std::string &lambda_code, const std::string &pickled_code, const py::object& closure) {

        auto& logger = Logger::instance().logger("python");
        logger.debug("entering map function");

        // make sure a dataset is wrapped
        assert(this->_dataset);
        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }
        auto closureObject = closure.ptr(); // nullptr if not set!
        // decode closure object
        auto ce = closureFromDict(closureObject);

        PythonDataSet pds;

        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->map(UDF(lambda_code, pickled_code, ce));
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::filter(const std::string &lambda_code, const std::string &pickled_code, const py::object& closure) {

        // make sure a dataset is wrapped
        assert(this->_dataset);
        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }
        auto closureObject = closure.ptr(); // nullptr if not set!
        auto ce = closureFromDict(closureObject);

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->filter(UDF(lambda_code, pickled_code, ce));
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::resolve(const int64_t exceptionCode, const std::string &lambda_code,
                                         const std::string &pickled_code, const py::object& closure) {
        assert(this->_dataset);
        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        auto closureObject = closure.ptr(); // nullptr if not set!
        auto ce = closureFromDict(closureObject);

        PythonDataSet pds;
        python::unlockGIL();

        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->resolve(i64ToEC(exceptionCode), UDF(lambda_code, pickled_code, ce));
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::mapColumn(const std::string &column, const std::string &lambda_code,
                                           const std::string &pickled_code, const py::object& closure) {
        assert(this->_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }
        auto closureObject = closure.ptr(); // nullptr if not set!
        auto ce = closureFromDict(closureObject);

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->mapColumn(column, UDF(lambda_code, pickled_code, ce));
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::aggregate(const std::string& comb, const std::string& comb_pickled,
                                           const std::string& agg, const std::string& agg_pickled,
                                           const std::string& initial_value_pickled,
                                           const py::object& comb_closure, const py::object& agg_closure) {
        using namespace std;

        // @TODO: warning if udfs are wrongly submitted
#warning "add warnings if UDFs are flipped!"
        assert(this->_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PyObject* combClosureObject = comb_closure.ptr();
        PyObject* aggClosureObject = agg_closure.ptr();
        auto combCE = closureFromDict(combClosureObject);
        auto aggCE = closureFromDict(aggClosureObject);

        // parse pickled initial_value
        if(initial_value_pickled.empty()) {
            PythonDataSet pds;
            pds.wrap(&_dataset->getContext()->makeError("pickled value is empty"));
            return pds;
        }
        auto py_initial_value = python::deserializePickledObject(python::getMainModule(),
                                                                 initial_value_pickled.c_str(),
                                                                 initial_value_pickled.size());
        // convert to row
        auto initial_value = python::pythonToRow(py_initial_value);

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->aggregate(UDF(comb, comb_pickled, combCE), UDF(agg, agg_pickled, aggCE), initial_value);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::aggregateByKey(const std::string& comb, const std::string& comb_pickled,
                                           const std::string& agg, const std::string& agg_pickled,
                                           const std::string& initial_value_pickled, py::list columns,
                                                const py::object& comb_closure,
                                                const py::object& agg_closure) {
        using namespace std;

        // @TODO: warning if udfs are wrongly submitted
#warning "add warnings if UDFs are flipped!"
        assert(this->_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PyObject* combClosureObject = comb_closure.ptr();
        PyObject* aggClosureObject = agg_closure.ptr();
        auto combCE = closureFromDict(combClosureObject);
        auto aggCE = closureFromDict(aggClosureObject);

        // parse pickled initial_value
        if(initial_value_pickled.empty()) {
            PythonDataSet pds;
            pds.wrap(&_dataset->getContext()->makeError("pickled value is empty"));
            return pds;
        }
        auto py_initial_value = python::deserializePickledObject(python::getMainModule(),
                                                                 initial_value_pickled.c_str(),
                                                                 initial_value_pickled.size());
        // convert to row
        auto initial_value = python::pythonToRow(py_initial_value);

        // TODO(rahuly): probably switch this to use integers later
        auto dataset_cols = _dataset->columns();
        PyObject * listObj = columns.ptr();
        std::vector<std::string> key_columns;
        for (unsigned i = 0; i < py::len(columns); ++i) {
            PyObject * obj = PyList_GetItem(listObj, i);
            Py_XINCREF(obj);
            if (PyObject_IsInstance(obj, reinterpret_cast<PyObject *>(&PyUnicode_Type))) {
                std::string col_name = python::PyString_AsString(obj);
                // search columns
                if (dataset_cols.empty())
                    throw std::runtime_error("trying to select column '" + col_name + "', but no column names defined");

                // search for column to convert to index
                int idx = indexInVector(col_name, dataset_cols);
                if (idx < 0) {
                    std::stringstream ss;
                    ss << "could not find column '" + col_name + "' in columns " << dataset_cols;
                    throw std::runtime_error(ss.str());
                }
                key_columns.push_back(col_name);
            } else {
                throw std::runtime_error("select columns only accepts integers or string!");
            }
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->aggregateByKey(UDF(comb, comb_pickled), UDF(agg, agg_pickled), initial_value, key_columns);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        //Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }


    PythonDataSet PythonDataSet::withColumn(const std::string &column, const std::string &lambda_code,
                                            const std::string &pickled_code, const py::object& closure) {
        assert(this->_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }
        auto closureObject = closure.ptr(); // nullptr if not set!
        auto ce = closureFromDict(closureObject);

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->withColumn(column, UDF(lambda_code, pickled_code, ce));
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::renameColumn(const std::string &oldName, const std::string &newName) {
        assert(_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->renameColumn(oldName, newName);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::renameColumnByPosition(int index, const std::string &newName) {
        assert(_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->renameColumn(index, newName);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::selectColumns(py::list L) {
        // check dataset is valid & perform error check.
        assert(this->_dataset);
        if (_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        // empty?
        if(_dataset->isEmpty()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        // there are two possible modes how list can be given:
        // 1.) selection using positive integers
        // 2.) via strings
        // ==> mixture also ok...
        auto rowType = _dataset->schema().getRowType();
        assert(rowType.isTupleType());
        auto num_cols = rowType.parameters().size();

        // go through list
        // ==> Tuplex supports list and integer indices which may be mixed up even!
        std::vector<std::size_t> columnIndices;
        auto columns = _dataset->columns();

        PyObject * listObj = L.ptr();
        for (unsigned i = 0; i < py::len(L); ++i) {
            PyObject * obj = PyList_GetItem(listObj, i);
            Py_XINCREF(obj);
            // check type:
            if (PyObject_IsInstance(obj, reinterpret_cast<PyObject *>(&PyLong_Type))) {
                int index = py::cast<int>(L[i]);

                // adjust negative index!
                int adjIndex = index < 0 ? index + num_cols : index;

                if (adjIndex < 0 || adjIndex >= num_cols) {
                    throw std::runtime_error("invalid index " + std::to_string(index)
                                             + " found, has to be -" + std::to_string(num_cols)
                                             + ",...0,...," + std::to_string(num_cols - 1));
                }
                assert(0 <= adjIndex && adjIndex < num_cols);

                columnIndices.push_back(static_cast<size_t>(adjIndex));
            } else if (PyObject_IsInstance(obj, reinterpret_cast<PyObject *>(&PyUnicode_Type))) {
                std::string col_name = python::PyString_AsString(obj);
                // search columns
                if (columns.empty())
                    throw std::runtime_error("trying to select column '" + col_name + "', but no column names defined");

                // search for column to convert to index
                int idx = indexInVector(col_name, columns);
                if (idx < 0) {
                    std::stringstream ss;
                    ss << "could not find column '" + col_name + "' in columns " << columns;
                    throw std::runtime_error(ss.str());
                }
                columnIndices.push_back(static_cast<size_t>(idx));
            } else {
                throw std::runtime_error("select columns only accepts integers or string!");
            }
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->selectColumns(columnIndices);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    void PythonDataSet::tocsv(const std::string &file_path, const std::string &lambda_code, const std::string &pickled_code,
                         size_t fileCount, size_t shardSize, size_t limit, const std::string &null_value,
                         py::object header) {
        // make sure a dataset is wrapped
        assert(this->_dataset);
        // ==> error handled below.

        std::unordered_map<std::string, std::string> outputOptions = defaultCSVOutputOptions();

        // is callee error dataset? if so return list with error string
        if (this->_dataset->isError()) {
            ErrorDataSet *eds = static_cast<ErrorDataSet *>(this->_dataset);
            py::list L;
            L.append(eds->getError());
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
        } else {
            // decode options
            outputOptions["null_value"] = null_value;

            // incref
            if(header.ptr())
                Py_XINCREF(header.ptr());

            // check what to do about the header
            assert(header.ptr());
            if(header.ptr() == Py_None) {
                // nothing todo, keep defaults...
            } else if(header.ptr() == Py_False || header.ptr() == Py_True) {
                // if false, no header
                outputOptions["header"] = header.ptr() == Py_False ? "false" : "true";
            } else {
                auto headerNames = extractFromListOfStrings(header.ptr(), "header");
                if (!headerNames.empty())
                    outputOptions["csvHeader"] = csvToHeader(headerNames) + "\n";
                outputOptions["header"] = "true";
            }

            // release GIL & hand over everything to Tuplex
            assert(PyGILState_Check()); // make sure this thread holds the GIL!
            python::unlockGIL();
            std::string err_message = "";
            try {
                // call tofile with all options provided via the wrapper...
                _dataset->tofile(FileFormat::OUTFMT_CSV,
                                 URI(file_path),
                                 UDF(lambda_code, pickled_code),
                                 fileCount,
                                 shardSize,
                                 outputOptions,
                                 limit);
            } catch(const std::exception& e) {
                err_message = e.what();
                Logger::instance().defaultLogger().error(err_message);
            } catch(...) {
                err_message = "unknown C++ exception occurred, please change type.";
                Logger::instance().defaultLogger().error(err_message);
            }

            python::lockGIL();

//            // nullptr? then error dataset!
//            if(!err_message.empty()) {
//                // Logger::instance().flushAll();
//                Logger::instance().flushToPython();
//                // TODO: roll back file system changes?
//            }
            Logger::instance().flushToPython();
        }
    }

    void PythonDataSet::toorc(const std::string &file_path, const std::string &lambda_code, const std::string &pickled_code,
                              size_t fileCount, size_t shardSize, size_t limit) {
        assert(this->_dataset);

        std::unordered_map<std::string, std::string> outputOptions = defaultORCOutputOptions();

        if (this->_dataset->isError()) {
            ErrorDataSet *eds = static_cast<ErrorDataSet *>(this->_dataset);
            py::list L;
            L.append(eds->getError());
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
        } else {
            assert(PyGILState_Check());

            outputOptions["columnNames"] = csvToHeader(_dataset->columns());

            python::unlockGIL();
            std::string err_message = "";
            try {
                _dataset->tofile(FileFormat::OUTFMT_ORC,
                                 URI(file_path),
                                 UDF(lambda_code, pickled_code),
                                 fileCount,
                                 shardSize,
                                 outputOptions,
                                 limit);
            } catch(const std::exception& e) {
                err_message = e.what();
                Logger::instance().defaultLogger().error(err_message);
            } catch(...) {
                err_message = "unknown C++ exception occurred, please change type.";
                Logger::instance().defaultLogger().error(err_message);
            }
            // Logger::instance().flushAll();
            python::lockGIL();
            Logger::instance().flushToPython();
        }
    }

    void PythonDataSet::show(const int64_t numRows) {

        // make sure a dataset is wrapped
        assert(this->_dataset);

        // release GIL & hand over everything to Tuplex
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        std::stringstream ss;
        std::string err_message = "";
        if (this->_dataset->isError()) {
            auto errset = dynamic_cast<ErrorDataSet *>(this->_dataset);
            assert(errset);
            ss << "Error: " << errset->getError();

        } else {
            try {
                this->_dataset->show(numRows, ss);
            } catch(const std::exception& e) {
                err_message = e.what();
                Logger::instance().defaultLogger().error(err_message);
            } catch(...) {
                err_message = "unknown C++ exception occurred, please change type.";
                Logger::instance().defaultLogger().error(err_message);
            }
        }
        // Logger::instance().flushAll();
        // reqacquire GIL
        python::lockGIL();
        Logger::instance().flushToPython();

        // python stdout
        if(!ss.str().empty() && err_message.empty())
            PySys_FormatStdout("%s", ss.str().c_str());
        else {
            PySys_FormatStdout("Error occurred: %s", err_message.c_str());
        }
    }

    PyObject* PythonDataSet::anyToCPythonWithPyObjects(ResultSet* rs, size_t maxRowCount) {
        assert(rs);

        // simply call the getnext row function from resultset
        PyObject * emptyListObj = PyList_New(0);
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
            return emptyListObj;
        }

        for(int i = 0; i < rowCount; ++i) {
            python::unlockGIL();
            auto row = rs->getNextRow();
            python::lockGIL();
            auto py_row = python::rowToPython(row, true);
            assert(py_row);
            PyList_SET_ITEM(listObj, i, py_row);
        }

        return listObj;
    }

    PyObject *PythonDataSet::anyToCPython(tuplex::ResultSet *rs, size_t maxRowCount) {
        assert(rs);

        PyObject * emptyListObj = PyList_New(0);
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);
        if (PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
            return emptyListObj;
        }

        // retrieve full partitions for speed
        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();
            auto schema = partition->schema();
            // single value? --> reset rowtype by one level
            auto type = schema.getRowType();
            assert(type.isTupleType());
            if (type.parameters().size() == 1)
                type = type.parameters().front();
            schema = Schema(schema.getMemoryLayout(), type);

            Deserializer ds(schema);

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t partitionRowCount = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            for (unsigned i = 0; i < partitionRowCount && pos < maxRowCount; ++i) {
                PyObject * rowObj = nullptr;

                // doesn't work. TODO: what does this mean?
                tuplex::cpython::fromSerializedMemory(ptr, end_ptr - ptr, schema, &rowObj,nullptr);
                assert(rowObj);

                // add to list object
                PyList_SetItem(listObj, pos++, rowObj);

                ptr += ds.inferLength(ptr);
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                return python::none();
            }
        }

        return listObj;
    }


    PyObject *PythonDataSet::f64ToCPython(tuplex::ResultSet *rs, size_t maxRowCount) {
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);

        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            double *dataptr = (double *) ptr;
            for (unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {
                // we know it is a double & there is a single output scheme only, so make this a fast convert!
                PyList_SET_ITEM(listObj, pos++, PyFloat_FromDouble(*dataptr));
                dataptr++;
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        return listObj;
    }

    PyObject *PythonDataSet::i64ToCPython(tuplex::ResultSet *rs, size_t maxRowCount) {
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);

        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            int64_t *dataptr = (int64_t *) ptr;
            for (unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {
                // we know it is a double & there is a single output scheme only, so make this a fast convert!
                PyList_SET_ITEM(listObj, pos++, PyLong_FromLongLong(*dataptr));
                dataptr++;
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        return listObj;
    }

    PyObject *PythonDataSet::boolToCPython(tuplex::ResultSet *rs, size_t maxRowCount) {
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);

        auto& logger = Logger::instance().defaultLogger();

        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            int64_t *dataptr = (int64_t *) ptr;
            for (unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {
                if (*dataptr > 0) {
                    Py_INCREF(Py_True); // list needs a ref, so inc ref count
                    PyList_SET_ITEM(listObj, pos++, Py_True);
                } else {
                    Py_INCREF(Py_False); // list needs a ref, so inc ref count
                    PyList_SET_ITEM(listObj, pos++, Py_False);
                }
                dataptr++;
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        return listObj;
    }

    PyObject *PythonDataSet::strToCPython(tuplex::ResultSet *rs, size_t maxRowCount) {
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);

        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            for (unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {


                // decode offset + size
                //int64_t info = varLenOffset | (varFieldSize << 32);
                int64_t info_field = *((int64_t *) ptr);
                int64_t fieldLength = info_field >> 32;
                int64_t offset = info_field & 0xffffffff;

                //assert(offset == 8);
                assert(fieldLength < partition->capacity());

                ptr += sizeof(int64_t);
                // ignore varlength field
                ptr += sizeof(int64_t);
                // here comes the str.
                assert(strlen((char *) ptr) <= partition->capacity());

                // Python string object
                PyList_SET_ITEM(listObj, pos++, python::PyString_FromString((char *) ptr));

                // inc ptr by string length
                ptr += fieldLength;
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        return listObj;
    }

    PyObject *PythonDataSet::f64TupleToCPython(tuplex::ResultSet *rs, size_t numTupleElements, size_t maxRowCount) {
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);

        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            double *dataptr = (double *) ptr;
            for (unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {

                PyObject * tupleObj = PyTuple_New(numTupleElements);
                for (unsigned j = 0; j < numTupleElements; ++j) {
                    PyTuple_SET_ITEM(tupleObj, j, PyFloat_FromDouble(*dataptr));
                    dataptr++;
                }

                // we know it is a double & there is a single output scheme only, so make this a fast convert!
                PyList_SET_ITEM(listObj, pos++, tupleObj);
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        return listObj;
    }

    PyObject *PythonDataSet::i64TupleToCPython(tuplex::ResultSet *rs, size_t numTupleElements, size_t maxRowCount) {
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject * listObj = PyList_New(rowCount);

        Partition *partition = nullptr;
        size_t pos = 0;
        while (rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t *) ptr);
            ptr += sizeof(int64_t);

            int64_t *dataptr = (int64_t *) ptr;
            for (unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {

                PyObject * tupleObj = PyTuple_New(numTupleElements);
                for (unsigned j = 0; j < numTupleElements; ++j) {
                    PyTuple_SET_ITEM(tupleObj, j, PyLong_FromLongLong(*dataptr));
                    dataptr++;
                }

                // we know it is a double & there is a single output scheme only, so make this a fast convert!
                PyList_SET_ITEM(listObj, pos++, tupleObj);
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        return listObj;
    }


    // Notes:
    // to make this even faster, use whatever code is available under https://github.com/python/cpython/blob/master/Objects/tupleobject.c
    // and https://github.com/python/cpython/blob/master/Include/tupleobject.h
    // i.e. get rid off some unnecessary if statements.
    PyObject* PythonDataSet::simpleTupleToCPython(tuplex::ResultSet *rs, const python::Type &type, size_t maxRowCount) {

        assert(type.isTupleType());
        assert(python::tupleElementsHaveSimpleTypes(type));

        auto numTupleElements = type.parameters().size();

        bool hasVarLenFields = !type.isFixedSizeType();

        // encode as type string
        char* typeStr = new char[numTupleElements];
        bool varLenField = makeTypeStr(type, typeStr);
        size_t rowCount = std::min(rs->rowCount(), maxRowCount);
        PyObject *listObj = PyList_New(rowCount);

        Partition* partition = nullptr;
        size_t pos = 0;
        while(rs->hasNextNormalPartition() && pos < maxRowCount) {
            partition = rs->getNextNormalPartition();

            // add memory towards list object
            auto ptr = partition->lockRaw();
            auto end_ptr = ptr + partition->size();
            size_t numRows = *((const int64_t*)ptr);
            ptr += sizeof(int64_t);

            for(unsigned i = 0; i < numRows && pos < maxRowCount; ++i) {

                PyObject* tupleObj = PyTuple_New(numTupleElements);
                for(unsigned j = 0; j < numTupleElements; ++j) {
                    switch(typeStr[j]) {
                        case 'b': {
                            // read int64t
                            int64_t b = *((int64_t*)ptr);
                            if(b > 0) {
                                Py_INCREF(Py_True); // list needs a ref, so inc ref count
                                PyTuple_SET_ITEM(tupleObj, j, Py_True);
                            } else {
                                Py_INCREF(Py_False); // list needs a ref, so inc ref count
                                PyTuple_SET_ITEM(tupleObj, j, Py_False);
                            }
                            ptr += sizeof(int64_t);
                            break;
                        }
                        case 'i': {
                            int64_t val = *((int64_t*)ptr);
                            ptr += sizeof(int64_t);
                            PyTuple_SET_ITEM(tupleObj, j, PyLong_FromLongLong(val));
                            break;
                        }
                        case 'f': {
                            double val = *((double*)ptr);
                            ptr += sizeof(int64_t);
                            PyTuple_SET_ITEM(tupleObj, j, PyFloat_FromDouble(val));
                            break;
                        }
                        case 's': {
                            // look up
                            int64_t info_field = *((int64_t*)ptr);
                            int64_t fieldLength = info_field >> 32;
                            int64_t offset = info_field & 0xffffffff;

                            //assert(offset == 8);
                            assert(fieldLength < partition->capacity());

                            char* strPtr = (char*)(ptr + offset);
                            // make sure it is zero-terminated!
                            assert(*(strPtr + fieldLength - 1) == '\0');

                            ptr += sizeof(int64_t);
                            // here comes the str.
                            assert(strlen((char*)ptr) <= partition->capacity());

                            // Python string object
                            PyTuple_SET_ITEM(tupleObj, j, python::PyString_FromString(strPtr));
                            break;
                        }
                        default:
                            throw std::runtime_error("weird type in toCPYTHON function");
                    }
                }

                // move ptr by varlenfieldsize
                if(hasVarLenFields) {
                    int64_t varlensize = *((int64_t*)ptr);
                    ptr += varlensize + sizeof(int64_t);
                }

                // we know it is a double & there is a single output scheme only, so make this a fast convert!
                PyList_SET_ITEM(listObj, pos++, tupleObj);
            }

            partition->unlock();

            // free partition (pointer is managed by Executor)s
            partition->invalidate();

            // check for signals, i.e. abort loop if exception is raised
            if(check_and_forward_signals(true)) {
                rs->clear();
                Py_XDECREF(listObj);
                Py_XINCREF(Py_None);
                return Py_None;
            }
        }
        delete [] typeStr;

        return listObj;
    }

    PyObject *PythonDataSet::resultSetToCPython(tuplex::ResultSet *rs, size_t maxRowCount) {
        // b.c. merging of arbitrary python objects is not implemented yet, whenever they're present, use general
        // version
        // @TODO: this could be optimized!
        if(rs->fallbackRowCount() != 0)
            return anyToCPythonWithPyObjects(rs, maxRowCount);

        auto type = rs->schema().getRowType();
        // if single type, reset by one
        assert(type.isTupleType());
        if (type.parameters().size() == 1)
            type = type.parameters().front();

        if (python::Type::BOOLEAN == type) {
            return boolToCPython(rs, maxRowCount);
        } else if (python::Type::I64 == type) {
            return i64ToCPython(rs, maxRowCount);
        } else if (python::Type::F64 == type) {
            return f64ToCPython(rs, maxRowCount);
        } else if (python::Type::STRING == type) {
            return strToCPython(rs, maxRowCount);
        } else {
            // special case: fast convert of simple tuples...
            if (type.isTupleType()) {
                if (python::tupleElementsHaveSameType(type) && type.parameters().size() > 0) {
                    python::Type eType = type.parameters().front();

                    // check what type it is
                    if (python::Type::I64 == eType)
                        return i64TupleToCPython(rs, type.parameters().size(), maxRowCount);
                    if (python::Type::F64 == eType)
                        return f64TupleToCPython(rs, type.parameters().size(), maxRowCount);
                }

                // check if primitive type
                if(python::tupleElementsHaveSimpleTypes(type) && type.parameters().size() > 0)
                    return simpleTupleToCPython(rs, type, maxRowCount);
            }

            // use fallback for any type (slow)
            return anyToCPython(rs, maxRowCount);
        }
    }

    PythonDataSet PythonDataSet::ignore(const int64_t exceptionCode) {
        assert(this->_dataset);

        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->ignore(i64ToEC(exceptionCode));
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::cache(bool storeSpecialized) {
        assert(this->_dataset);

        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->cache(storeSpecialized);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    py::list PythonDataSet::columns() {
        // check if error dataset?
        assert(_dataset);

        std::vector<std::string> cols;

        std::string err_message = "";
        try {
            // is error dataset? if so return it directly!
            if (!_dataset->isError())
                cols = _dataset->columns();
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        return STL_to_Python(cols);
    }

    PythonDataSet
    PythonDataSet::join(const PythonDataSet &right, const std::string &leftKeyColumn, const std::string &rightKeyColumn,
                        const std::string &leftPrefix, const std::string &leftSuffix, const std::string &rightPrefix,
                        const std::string &rightSuffix) {

        assert(_dataset);
        assert(right._dataset);

        // error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        if(right._dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(right._dataset);
            return pds;
        }

        // create
        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->join(*right._dataset, leftKeyColumn, rightKeyColumn, leftPrefix, leftSuffix,
                                 rightPrefix, rightSuffix);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    //@TODO: refactor, this is copy & paste code from join
    PythonDataSet
    PythonDataSet::leftJoin(const PythonDataSet &right, const std::string &leftKeyColumn, const std::string &rightKeyColumn,
                        const std::string &leftPrefix, const std::string &leftSuffix, const std::string &rightPrefix,
                        const std::string &rightSuffix) {

        assert(_dataset);
        assert(right._dataset);

        // error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        if(right._dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(right._dataset);
            return pds;
        }

        // create
        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->leftJoin(*right._dataset, leftKeyColumn, rightKeyColumn, leftPrefix, leftSuffix,
                                     rightPrefix, rightSuffix);
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    PythonDataSet PythonDataSet::unique() {
        assert(this->_dataset);

        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            PythonDataSet pds;
            pds.wrap(this->_dataset);
            return pds;
        }

        PythonDataSet pds;
        // GIL release & reacquire
        assert(PyGILState_Check()); // make sure this thread holds the GIL!
        python::unlockGIL();
        DataSet *ds = nullptr;
        std::string err_message = "";
        try {
            ds = &_dataset->unique();
        } catch(const std::exception& e) {
            err_message = e.what();
            Logger::instance().defaultLogger().error(err_message);
        } catch(...) {
            err_message = "unknown C++ exception occurred, please change type.";
            Logger::instance().defaultLogger().error(err_message);
        }

        python::lockGIL();

        // nullptr? then error dataset!
        if(!ds || !err_message.empty()) {
            // Logger::instance().flushAll();
            Logger::instance().flushToPython();
            assert(_dataset->getContext());
            ds = &_dataset->getContext()->makeError(err_message);
        }
        pds.wrap(ds);
        // Logger::instance().flushAll();
        Logger::instance().flushToPython();
        return pds;
    }

    py::object PythonDataSet::types() {
        // is error dataset? if so return it directly!
        if (this->_dataset->isError()) {
            return py::none(); // none
        }

        auto row_type = _dataset->schema().getRowType();
        assert(row_type.isTupleType());


        assert(row_type.isTupleType());
        // return as list (always!)
        PyObject* listObj = PyList_New(row_type.parameters().size());
        for(unsigned i = 0; i < row_type.parameters().size(); ++i) {
            auto typeobj = python::encodePythonSchema(row_type.parameters()[i]);
            PyList_SetItem(listObj, i, typeobj);
        }
        return py::reinterpret_borrow<py::list>(listObj);
    }

    py::object PythonDataSet::exception_counts() {
        if(this->_dataset->isError())
            return py::none(); // none

        // return dict object
        auto dict = PyDict_New();

        // fetch from dataset corresponding metrics
        auto counts = _dataset->getContext()->metrics().getOperatorExceptionCounts(this->_dataset->getOperator()->getID());
        for(const auto& keyval : counts) {
            PyDict_SetItemString(dict, keyval.first.c_str(), PyLong_FromLongLong(keyval.second));
        }

        return py::reinterpret_borrow<py::dict>(dict);
    }
}