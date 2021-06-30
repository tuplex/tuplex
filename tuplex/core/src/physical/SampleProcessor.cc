//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/SampleProcessor.h>
#include <logical/UDFOperator.h>
#include <logical/MapColumnOperator.h>
#include <logical/WithColumnOperator.h>
#include <logical/ResolveOperator.h>
#include <logical/FileInputOperator.h>
#include <Utils.h>
#include <vector>
#include <string>
#include <stdexcept>



namespace tuplex {
    void SampleProcessor::releasePythonObjects() {

        assert(python::isInterpreterRunning());
        python::lockGIL();

        // release UDFs
        for(auto keyval : _TUPLEXs) {
            Py_XDECREF(keyval.second);
        }

        python::unlockGIL();

        _TUPLEXs.clear();
    }

    void SampleProcessor::cachePythonUDFs() {
        // lazy deserialize
        if(!_udfsCached) {

            assert(python::isInterpreterRunning());


            for(auto op : _operators) {
                if(hasUDF(op)) {
                    UDFOperator *udfop = dynamic_cast<UDFOperator*>(op);
                    assert(udfop);

                    auto pickled_code = udfop->getUDF().getPickledCode(); // this has internal python locks

                    python::lockGIL();
                    auto pFunc = python::deserializePickledFunction(python::getMainModule(),
                                                                    pickled_code.c_str(), pickled_code.size());
                    python::unlockGIL();

                    _TUPLEXs[op->getID()] = pFunc;
                }
            }

            _udfsCached = true;
        }
    }

    std::string formatTraceback(const std::string& functionName,
                                const std::string& exceptionClass,
                                const std::string& message,
                                long lineNo) {
        std::stringstream ss;

        ss<<"line "<<lineNo<<" in "<<functionName<<":"<<"\n    ---> "<<exceptionClass<<": "<<message;

        return ss.str();
    }


    // get dictmode from fas166
    // AND get
    python::PythonCallResult SampleProcessor::applyOperator(LogicalOperator *op, PyObject *pyRow) {
        python::PythonCallResult pcr;

//        // check type of input row (either tuple OR dict)
//        if(PyTuple_Check(pyRow)) {
//            // tuple as input
//        } else
//            // dict as input (not yet supported!)
//            throw std::runtime_error("dict as input not yet supported!!!");
//        }

        // extract UDF relevant information
        PyObject *TUPLEX = nullptr;
        bool dictMode = false;
        std::vector<std::string> columns;
        if(hasUDF(op)) {
            TUPLEX = _TUPLEXs.at(op->getID());
            UDFOperator* udfop = dynamic_cast<UDFOperator*>(op);
            assert(udfop);

            dictMode = udfop->getUDF().dictMode();
            columns = udfop->columns();
        }

        // apply operation
        switch(op->type()) {

            // @Todo: solve for dict mode OR tuple mode...
            // --> makes this a little more complicated...
            // --> check for each function (using AST visitor), whether it is in dict mode or not


            case LogicalOperatorType::MAP: {
                Py_XINCREF(pyRow); // +1, for function to consume

                // call function depending on mode
                // Note: if result is not taken, decrease ref!
                pcr = dictMode ? python::callFunctionWithDictEx(TUPLEX, pyRow, op->parent()->columns()) :
                                  python::callFunctionEx(TUPLEX, pyRow);


                break;
            }

            case LogicalOperatorType::FILTER: {
                // return bool res here
                Py_XINCREF(pyRow); // +1, for function to consume

                // call function depending on mode
                pcr = dictMode ? python::callFunctionWithDictEx(TUPLEX, pyRow, op->parent()->columns()) :
                                  python::callFunctionEx(TUPLEX, pyRow);
                break;
            }

            case LogicalOperatorType::WITHCOLUMN: {
                PyObject* pyRes = nullptr;

                // get cached UDF
                auto wop = ((WithColumnOperator*)op);
                // apply UDF and check for errors...
                auto idx = wop->getColumnIndex();
                auto num_columns = columns.size();
                Py_XINCREF(pyRow); // count +1, because call Function consumes 1

                pcr = dictMode ? python::callFunctionWithDictEx(TUPLEX, pyRow, op->parent()->columns()) :
                                python::callFunctionEx(TUPLEX, pyRow);

                auto pyColRes = pcr.res;
                if(pcr.exceptionCode == ExceptionCode::SUCCESS) {

                    assert(pyColRes);

                    pyRes = PyTuple_New(num_columns);
                    for(unsigned i = 0; i < num_columns; i++) {
                        if(i != idx) {
                            assert(i < PyTuple_Size(pyRow));
                            PyTuple_SET_ITEM(pyRes, i, PyTuple_GET_ITEM(pyRow, i));
                        }
                        else
                            PyTuple_SET_ITEM(pyRes, i, pyColRes);
                    }
                }

                pcr.res = pyRes;

                break;
            }

            // @Todo: should compute graph for all samples too and flag which ones would get thrown out by filter...
            case LogicalOperatorType::MAPCOLUMN: {
                PyObject* pyRes = nullptr;

                auto idx = ((MapColumnOperator*)op)->getColumnIndex();
                PyObject *pyElement = PyTuple_GetItem(pyRow, idx);
                PyObject *pyArg = PyTuple_New(1);
                PyTuple_SET_ITEM(pyArg, 0, pyElement);

                // only in tuple mode!
                pcr = python::callFunctionEx(TUPLEX, pyArg);
                auto pyColRes = pcr.res;
                if(pcr.exceptionCode == ExceptionCode::SUCCESS) {
                    pyRes = PyTuple_New(PyTuple_Size(pyRow));
                    for(unsigned i = 0; i < PyTuple_Size(pyRow); ++i) {
                        if(i != idx)
                            PyTuple_SET_ITEM(pyRes, i, PyTuple_GET_ITEM(pyRow, i));
                        else
                            PyTuple_SET_ITEM(pyRes, i, pyColRes);
                    }
                }

                // output
                pcr.res = pyRes;

                break;
            }

            default:
                throw "unknown operator " + op->name() + " seen in sampling procedure";
        }

        return pcr;
    }


    python::PythonCallResult SampleProcessor::applyMap(bool dictMode, PyObject *TUPLEX, PyObject *pyRow,
                                                       const std::vector<std::string> &columns) {

        // debug: check & assert refcounts
#ifndef NDEBUG
        auto oldRowRefCnt = pyRow->ob_refcnt;
        auto oldUDFRefCnt = TUPLEX->ob_refcnt;
#endif

        Py_XINCREF(pyRow);

        // call function depending on mode
        // Note: if result is not taken, decrease ref!
        auto pcr = dictMode ? python::callFunctionWithDictEx(TUPLEX, pyRow, columns) :
              python::callFunctionEx(TUPLEX, pyRow);

        // debug assert refcnts, should not be cleared
#ifndef NDEBUG
        assert(pyRow->ob_refcnt >= oldRowRefCnt);
        assert(TUPLEX->ob_refcnt >= oldUDFRefCnt);
#endif

        return pcr;
    }

    python::PythonCallResult SampleProcessor::applyMapColumn(bool dictMode, PyObject *TUPLEX, PyObject *pyRow,
                                                             int idx) {

        assert(!dictMode); // no dict mode allowed in mapColumn!

        // debug: check & assert refcounts
#ifndef NDEBUG
        auto oldRowRefCnt = pyRow->ob_refcnt;
        auto oldUDFRefCnt = TUPLEX->ob_refcnt;
#endif
        Py_XINCREF(pyRow);

        PyObject* pyRes = nullptr;
        PyObject *pyElement = PyTuple_GetItem(pyRow, idx);
        PyObject *pyArg = PyTuple_New(1);
        PyTuple_SET_ITEM(pyArg, 0, pyElement);

        // only in tuple mode!
        auto pcr = python::callFunctionEx(TUPLEX, pyArg);
        auto pyColRes = pcr.res;
        if(pcr.exceptionCode == ExceptionCode::SUCCESS) {
            pyRes = PyTuple_New(PyTuple_Size(pyRow));
            for(unsigned i = 0; i < PyTuple_Size(pyRow); ++i) {
                if(i != idx)
                    PyTuple_SET_ITEM(pyRes, i, PyTuple_GET_ITEM(pyRow, i));
                else
                    PyTuple_SET_ITEM(pyRes, i, pyColRes);
            }
        }

        // output
        pcr.res = pyRes;

        // debug assert refcnts, should not be cleared
#ifndef NDEBUG
        assert(pyRow->ob_refcnt >= oldRowRefCnt);
        assert(TUPLEX->ob_refcnt >= oldUDFRefCnt);
#endif

        return pcr;
    }

    python::PythonCallResult SampleProcessor::applyWithColumn(bool dictMode, PyObject *TUPLEX, PyObject *pyRow,
                                                              const std::vector<std::string> &columns, int idx) {

        // debug: check & assert refcounts
#ifndef NDEBUG
        auto oldRowRefCnt = pyRow->ob_refcnt;
        auto oldUDFRefCnt = TUPLEX->ob_refcnt;
#endif
        PyObject* pyRes = nullptr;
        auto num_columns = columns.size();

        Py_XINCREF(pyRow); // required because of the consumption below.

        // call function depending on mode
        // Note: if result is not taken, decrease ref!
        auto pcr = dictMode ? python::callFunctionWithDictEx(TUPLEX, pyRow, columns) :
                   python::callFunctionEx(TUPLEX, pyRow);

        auto pyColRes = pcr.res;
        if(pcr.exceptionCode == ExceptionCode::SUCCESS) {
            assert(pyColRes);

            pyRes = PyTuple_New(num_columns);
            for(unsigned i = 0; i < num_columns; i++) {
                if(i != idx) {
                    assert(i < PyTuple_Size(pyRow));
                    auto item = PyTuple_GET_ITEM(pyRow, i);
                    PyTuple_SET_ITEM(pyRes, i, item);
                }
                else
                    PyTuple_SET_ITEM(pyRes, i, pyColRes);
            }
        }

        pcr.res = pyRes;

        // debug assert refcnts, should not be cleared
#ifndef NDEBUG
        assert(pyRow->ob_refcnt >= oldRowRefCnt);
        assert(TUPLEX->ob_refcnt >= oldUDFRefCnt);
#endif

        return pcr;

    }


    // trace row w/o resolvers/ignore applied
    SampleProcessor::TraceResult SampleProcessor::traceRow(const tuplex::Row &row) {
        TraceResult tr;

        // input row is received, apply all operators in this processor to it
        python::lockGIL();

        PyObject* rowObj = python::rowToPython(row);
        for(auto op : _operators) {

            // if UDFOperator, decode whether it's dict mode or not
            // extract UDF relevant information
            PyObject *TUPLEX = nullptr;
            bool dictMode = false;
            std::vector<std::string> columns;
            if(hasUDF(op)) {
                TUPLEX = _TUPLEXs.at(op->getID());
                UDFOperator* udfop = dynamic_cast<UDFOperator*>(op);
                assert(udfop);

                dictMode = udfop->getUDF().dictMode();
                columns = udfop->parent()->columns(); // get the parents (output) columns, they're the current operators input columns.
            }

            switch(op->type()) {
                case LogicalOperatorType::FILEINPUT:
                case LogicalOperatorType::PARALLELIZE:
                case LogicalOperatorType::TAKE:
                case LogicalOperatorType::FILEOUTPUT:
                    break; // ignore

                case LogicalOperatorType::MAP: {
                    // there's always output for map, i.e. apply
                    auto pcr = applyMap(dictMode, TUPLEX, rowObj, columns);

                    // check what result is
                    if(ExceptionCode::SUCCESS == pcr.exceptionCode)
                        rowObj = pcr.res;
                    else {
                        tr.exceptionTraceback = formatTraceback(pcr.functionName,
                                                         pcr.exceptionClass,
                                                         pcr.exceptionMessage,
                                                         pcr.exceptionLineNo);
                        tr.outputRow = python::pythonToRow(rowObj);
                        tr.ec = pcr.exceptionCode;
                        tr.lastOperatorID = op->getID();
                        python::unlockGIL();
                        return tr;
                    }

                    break;
                }

                case LogicalOperatorType::MAPCOLUMN: {

                    auto idx = dynamic_cast<MapColumnOperator*>(op)->getColumnIndex();
                    auto pcr = applyMapColumn(dictMode, TUPLEX, rowObj, idx);

                    // check what result is
                    if(ExceptionCode::SUCCESS == pcr.exceptionCode)
                        rowObj = pcr.res;
                    else {
                        tr.exceptionTraceback = formatTraceback(pcr.functionName,
                                                                pcr.exceptionClass,
                                                                pcr.exceptionMessage,
                                                                pcr.exceptionLineNo);
                        tr.outputRow = python::pythonToRow(rowObj);
                        tr.ec = pcr.exceptionCode;
                        tr.lastOperatorID = op->getID();
                        python::unlockGIL();
                        return tr;
                    }

                    break;
                }

                case LogicalOperatorType::WITHCOLUMN: {
                    auto idx = dynamic_cast<WithColumnOperator*>(op)->getColumnIndex();
                    auto pcr = applyWithColumn(dictMode, TUPLEX, rowObj, columns, idx);

                    // check what result is
                    if(ExceptionCode::SUCCESS == pcr.exceptionCode)
                        rowObj = pcr.res;
                    else {
                        tr.exceptionTraceback = formatTraceback(pcr.functionName,
                                                                pcr.exceptionClass,
                                                                pcr.exceptionMessage,
                                                                pcr.exceptionLineNo);
                        tr.outputRow = python::pythonToRow(rowObj);
                        tr.ec = pcr.exceptionCode;
                        tr.lastOperatorID = op->getID();
                        python::unlockGIL();
                        return tr;
                    }

                    break;
                }

                case LogicalOperatorType::FILTER: {
                    // special case: reuse map for this
                    auto pcr = applyMap(dictMode, TUPLEX, rowObj, columns);

                    // check what result is
                    if(ExceptionCode::SUCCESS == pcr.exceptionCode) {
                        // check what the result is
                        auto res = python::pythonToRow(pcr.res);
                        assert(res.getNumColumns() == 1);

                        // if false, then filtered out. I.e. stop & return trace result!
                        if(!res.getBoolean(0)) {
                            tr.outputRow = Row();
                            tr.ec = ExceptionCode::SUCCESS;
                            tr.lastOperatorID = op->getID();
                            python::unlockGIL();
                            return tr;
                        }
                        // else nothing todo, continue going through the pipeline :)
                    }
                    else {
                        tr.exceptionTraceback = formatTraceback(pcr.functionName,
                                                                pcr.exceptionClass,
                                                                pcr.exceptionMessage,
                                                                pcr.exceptionLineNo);
                        tr.outputRow = python::pythonToRow(rowObj);
                        tr.ec = pcr.exceptionCode;
                        tr.lastOperatorID = op->getID();
                        python::unlockGIL();
                        return tr;
                    }

                    break;
                }
                case LogicalOperatorType::UNKNOWN:
                case LogicalOperatorType::RESOLVE:
                case LogicalOperatorType::IGNORE:
                case LogicalOperatorType::JOIN:
                case LogicalOperatorType::AGGREGATE:
                case LogicalOperatorType::CACHE:
                default: {
                    break;
                }
            }
        }

        python::unlockGIL();
        return tr;
    }

    // @TODO: rewrite this function to use apply operator AND work for functions with dictionaries...
    // ==> i.e. the extractSqft example
    ExceptionSample SampleProcessor::generateExceptionSample(const Row& row, bool excludeAvailableResolvers) noexcept {

        using namespace std;
        ExceptionSample es;

        // for some reason GILState blocks here, use restore thread thus...
        auto tr = traceRow(row); // always trace without accounting for the resolver

        // check if the given operator is actually a resolver and whether exception was thrown in res
        // ==> try to apply res
        if(!excludeAvailableResolvers && tr.ec != ExceptionCode::SUCCESS) {
            // apply resolvers if necessary!
            Row work_row = tr.outputRow;

            // get last operator ID
            int index = 0;
            while(index < _operators.size() && _operators[index]->getID() != tr.lastOperatorID)
                index++;
            assert(index < _operators.size());
            assert(_operators[index]->getID() == tr.lastOperatorID);

            // now check if resolver is present, if so try to resolve or find the one which causes the exception!
            while(index + 1 < _operators.size() && _operators[index + 1]->type() == LogicalOperatorType::RESOLVE) {
                auto op = dynamic_cast<ResolveOperator*>(_operators[index + 1]);
                assert(op && op->type() == LogicalOperatorType::RESOLVE);

#warning " for some reason in the resolve t"

                // get resolver UDF
                auto TUPLEX = _TUPLEXs.at(op->getID());
                assert(TUPLEX);
                auto dictMode = op->getUDF().dictMode();
                auto columns = op->getNormalParent()->columns(); // get the parents (output) columns, they're the current operators input columns.

                // check whether for this exception code a resolver exists, if not => continue!
                if(op->ecCode() == tr.ec) {

                    python::lockGIL();

                    // just apply function, enough for the traceback...
                    // ==> function is not traced through resolvers!
                    auto pyRow = python::rowToPython(work_row);

                    auto pcr = dictMode ? python::callFunctionWithDictEx(TUPLEX, pyRow, columns) :
                                          python::callFunctionEx(TUPLEX, pyRow);

                    // trace result!
                    tr.exceptionTraceback = formatTraceback(pcr.functionName,
                                                            pcr.exceptionClass,
                                                            pcr.exceptionMessage,
                                                            pcr.exceptionLineNo);
                    tr.outputRow = work_row;
                    tr.ec = pcr.exceptionCode;
                    tr.lastOperatorID = op->getID();

                    python::unlockGIL();
                }

                index++;
            }



        }


        es.rows.push_back(tr.outputRow);
        es.first_row_traceback = tr.exceptionTraceback;

//        // compute over rows
//        assert(!inRows.empty());
//
//        vector<python::PythonCallResult> results;
//        for(int j = 0; j < inRows.size(); ++j) {
//            python::PythonCallResult pcr;
//            auto work_row = inRows[j];
//            for(int i = 0; i < max_idx + 1; ++i) {
//                // only apply map/resolve, i.e. the trafos
//                LogicalOperator* op = _operators[i];
//
//                // skip parallelize and csv
//                if(op->type() == LogicalOperatorType::CSV || op->type() == LogicalOperatorType::PARALLELIZE) {
//                    assert(operatorID != op->getID()); // make sure THEY ARE NOT THE CAUSE OF THE ERROR
//                    continue;
//                }
//
//                pcr = applyOperator(op, python::rowToPython(work_row));
//                if(pcr.exceptionCode == ExceptionCode::SUCCESS)
//                    work_row = python::pythonToRow(pcr.res);
//                else {
//                    // make sure op is the exception throwing operator!
//                    assert(op->getID() == operatorID);
//                    break;
//                }
//            }
//
//            if(0 == j)
//                es.first_row_traceback = formatTraceback(pcr.functionName,
//                                                         pcr.exceptionClass,
//                                                         pcr.exceptionMessage,
//                                                         pcr.exceptionLineNo);
//
//            // push back to sample
//            es.rows.emplace_back(work_row);
//        }




//        bool first_row = true;
//        for(auto row : inRows) {
//            Row work_row = row;
//            ExceptionCode ec;
//            PyObject *pyRes = nullptr;
//            // go with row through pipeline
//            for(int i = 0; i < max_idx + 1; ++i) {
//                // only apply map/resolve, i.e. the trafos
//                LogicalOperator* op = _operators[i];
//
//
//
//                if(op->type() == LogicalOperatorType::MAP ||
//                   op->type() == LogicalOperatorType::RESOLVE) {
//                    pyRes = python::callFunction(_TUPLEXs.at(op->getID()), python::rowToPython(work_row), ec);
//
//
//#warning "there might also an issue here when it comes to resolvers because the first error-free resolver is applied!!!"
//
//                    // only overwrite if successful, i.e. resolvers may also throw exceptions...
//                    // note though, that resolvers apply only to the operator before, not the resolver before!
//                    if(ec == ExceptionCode::SUCCESS)
//                        work_row = python::pythonToRow(pyRes);
//                }
//
//                // apply MAPCOLUMN
//                if(op->type() == LogicalOperatorType::MAPCOLUMN) {
//                    auto idx = dynamic_cast<MapColumnOperator*>(op)->getColumnIndex();
//                    PyObject* pyRow = python::rowToPython(work_row);
//                    PyObject *pyElement = PyTuple_GetItem(pyRow, idx);
//                    PyObject *pyArg = PyTuple_New(1);
//                    PyTuple_SET_ITEM(pyArg, 0, pyElement);
//
//                    pyRes = python::callFunction(_TUPLEXs.at(op->getID()), pyArg, ec);
//                    if(ec == ExceptionCode::SUCCESS) {
//                        auto pyRowRes = PyTuple_New(PyTuple_Size(pyRow));
//                        for(unsigned i = 0; i < PyTuple_Size(pyRow); ++i) {
//                            if(i != idx)
//                                PyTuple_SET_ITEM(pyRowRes, i, PyTuple_GET_ITEM(pyRow, i));
//                            else
//                                PyTuple_SET_ITEM(pyRowRes, i, pyRes);
//                        }
//                        work_row = python::pythonToRow(pyRowRes); // transformed row
//                    }
//
//                    Py_XDECREF(pyRow);
//                }
//
//                // apply WITHCOLUMN
//                if(op->type() == LogicalOperatorType::WITHCOLUMN) {
//
//                    auto wop = dynamic_cast<WithColumnOperator*>(op);
//                    auto idx = wop->getColumnIndex();
//                    auto num_columns = wop->getColumns().size();
//                    PyObject* pyRow = python::rowToPython(work_row);
//                    Py_XINCREF(pyRow); // count +1, because call Function consumes 1
//                    pyRes = python::callFunction(_TUPLEXs.at(op->getID()), pyRow, ec);
//
//                    if(ec == ExceptionCode::SUCCESS) {
//                        auto pyRowRes = PyTuple_New(num_columns);
//                        for(unsigned i = 0; i < num_columns; i++) {
//                            if(i != idx) {
//                                assert(i < PyTuple_Size(pyRow));
//                                PyTuple_SET_ITEM(pyRowRes, i, PyTuple_GET_ITEM(pyRow, i));
//                            }
//                            else
//                                PyTuple_SET_ITEM(pyRowRes, i, pyRes);
//                        }
//
//                        work_row = python::pythonToRow(pyRowRes); // transformed row
//                    }
//
//                    // decref row & res (though they should be at 0)
//                    Py_XDECREF(pyRow);
//                    Py_XDECREF(pyRes);
//                }
//
//
//                // in debug mode check that filter is valid...
//                // i.e. we know that the tuple went through the pipeline till operator with operatorID
//#ifndef NDEBUG
//                if(op->type() == LogicalOperatorType::FILTER) {
//                    pyRes = python::callFunction(_TUPLEXs.at(op->getID()), python::rowToPython(work_row), ec);
//                    assert(ec == ExceptionCode::SUCCESS);
//                    assert(pyRes == Py_True || pyRes == Py_False);
//                }
//#endif
//            }
//
//            es.rows.emplace_back(work_row);
//
//            // now generate traceback + exceptions...
//            if(first_row) {
//                // apply max_idx operator & capture exception for first row
//                // for other rows, simply save result before
//                auto op = _operators[max_idx];
//                python::PythonCallResult pcr;
//
//                // what signature do operators take?
//                // special case resolve, check on parent type
//                LogicalOperatorType optype = op->type();
//
//                // check what type of function signature it is
//                bool singleColumnArg = false;
//                int singleColumnIndex = -1;
//                if(op->type() == LogicalOperatorType::MAPCOLUMN) {
//                    singleColumnArg = true;
//                    singleColumnIndex = dynamic_cast<MapColumnOperator*>(op)->getColumnIndex();
//                }
//
//                // check if resolve operator, then check if non-resolve parent is mapColumn
//                if(op->type() == LogicalOperatorType::RESOLVE) {
//                    auto rop = dynamic_cast<ResolveOperator*>(op);
//                    auto parent = rop->getNormalParent();
//                    if(parent->type() == LogicalOperatorType::MAPCOLUMN) {
//                        singleColumnArg = true;
//                        singleColumnIndex = dynamic_cast<MapColumnOperator*>(parent)->getColumnIndex();
//                    }
//                }
//
//                if(singleColumnArg) {
//                    assert(singleColumnIndex >= 0);
//                    // extract element
//                    PyObject *pyRow = python::rowToPython(work_row);
//                    assert(PyTuple_Check(pyRow));
//
//                    // get element
//                    assert(singleColumnIndex < PyTuple_Size(pyRow));
//                    PyObject *pyElement = PyTuple_GetItem(pyRow, singleColumnIndex); // steals reference from Row
//                    PyObject *pySingleArg = PyTuple_New(1);
//                    PyTuple_SET_ITEM(pySingleArg, 0, pyElement); // steals reference from pyElement
//                    pcr = python::callFunctionEx(_TUPLEXs.at(op->getID()), pySingleArg);
//
//                    // element & arg steal reference from row. Therefore sufficient to release row only
//                    Py_XDECREF(pyRow);
//
//                } else {
//                    // the whole row is the argument, i.e. call on it!
//                    pcr = python::callFunctionEx(_TUPLEXs.at(op->getID()), python::rowToPython(work_row));
//                }
//
//                es.first_row_traceback = formatTraceback(pcr.functionName,
//                                                         pcr.exceptionClass,
//                                                         pcr.exceptionMessage,
//                                                         pcr.exceptionLineNo);
//
//                // in debug mode, validate result @Todo
//                first_row = false;
//            }
//        }

        return es;
    }


    std::vector<std::string> SampleProcessor::getColumnNames(int64_t operatorID) {
        // find operator & return column names
        auto it = std::find_if(_operators.begin(), _operators.end(), [operatorID](LogicalOperator* op) {
            return op->getID() == operatorID;
        });

        if(it != _operators.end())
            return (*it)->getDataSet()->columns();

        // warn?
        Logger::instance().defaultLogger().warn("accesing unknown operator " + std::to_string(operatorID) + " in sample processor");

        return std::vector<std::string>();
    }

    LogicalOperator* SampleProcessor::getOperator(int64_t operatorID) {
        // find operator & return column names
        auto it = std::find_if(_operators.begin(), _operators.end(), [operatorID](LogicalOperator* op) {
            return op->getID() == operatorID;
        });

        if(it != _operators.end())
            return *it;

        Logger::instance().defaultLogger().warn("accesing unknonw operator " + std::to_string(operatorID) + " in sample processor");

        return nullptr;
    }

    int SampleProcessor::getOperatorIndex(int64_t operatorID) {
        // find operator & return column names
        auto it = std::find_if(_operators.begin(), _operators.end(), [operatorID](LogicalOperator* op) {
            return op->getID() == operatorID;
        });

        if(it != _operators.end())
            return it - _operators.begin();

        Logger::instance().defaultLogger().warn("accesing unknonw operator " + std::to_string(operatorID) + " in sample processor");

        return -1;
    }
}