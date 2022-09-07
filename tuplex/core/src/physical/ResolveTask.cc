//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <algorithm> // for binary search

#include <physical/ResolveTask.h>
#include <RuntimeInterface.h>
#include <bucket.h>
#include <TypeAnnotatorVisitor.h>
#include <CSVUtils.h>

#define BUF_FORMAT_COMPILED_RESOLVE 0
#define BUF_FORMAT_NORMAL_OUTPUT 1
#define BUF_FORMAT_GENERAL_OUTPUT 2

// to enable debug tracing of resolution, use TRACE_EXCEPTIONS
// #define TRACE_EXCEPTIONS

extern "C" {
    static int64_t rRowCallback(tuplex::ResolveTask *task, uint8_t* buf, int64_t bufSize) {
        assert(task);
        return task->mergeRow(buf, bufSize, BUF_FORMAT_COMPILED_RESOLVE);
    }

    static int64_t rExceptCallback(tuplex::ResolveTask *task, const int64_t ecCode, const int64_t opID, const int64_t row, const uint8_t *buf, const size_t bufSize) {
        assert(task);

        // Logger::instance().logger("resolve task").debug("writing exception for row #" + std::to_string(row));

        // @TODO: avoid double recording with BOTH fallback path and interpreter path...
        // i.e. better call the callback somewhere else...
        // --> get rid of callback?
        // --> just record which ecCode, opID, row, buf?
        // what about loops?
        task->exceptionCallback(ecCode, opID, row, buf, bufSize);
        return (int64_t)tuplex::ExceptionCode::SUCCESS;
    }

    static void
    rStrHashCallback(tuplex::ResolveTask *task, char *strkey, size_t key_size, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::ResolveTask*>(task));
        task->writeRowToHashTable(strkey, key_size, bucketize, buf, buf_size);
    }
    static void
    rI64HashCallback(tuplex::ResolveTask *task, int64_t intkey, bool intkeynull, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::ResolveTask*>(task));
        auto key = static_cast<uint64_t>(intkey);
        task->writeRowToHashTable(key, intkeynull, bucketize, buf, buf_size);
    }

    static void
    rStrHashAggCallback(tuplex::ResolveTask *task, char *strkey, size_t key_size, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::ResolveTask*>(task));
        task->writeRowToHashTableAggregate(strkey, key_size, bucketize, buf, buf_size);
    }
    static void
    rI64HashAggCallback(tuplex::ResolveTask *task, int64_t intkey, bool intkeynull, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::ResolveTask*>(task));
        auto key = static_cast<uint64_t>(intkey);
        task->writeRowToHashTableAggregate(key, intkeynull, bucketize, buf, buf_size);
    }
}

namespace tuplex {
    codegen::write_row_f ResolveTask::mergeRowCallback() {
        return reinterpret_cast<codegen::write_row_f >(rRowCallback);
    }

    codegen::exception_handler_f ResolveTask::exceptionCallback() {
        return reinterpret_cast<codegen::exception_handler_f >(rExceptCallback);
    }
    codegen::str_hash_row_f ResolveTask::writeStringHashTableCallback() {
        return reinterpret_cast<codegen::str_hash_row_f>(rStrHashCallback);
    }
    codegen::i64_hash_row_f  ResolveTask::writeInt64HashTableCallback() {
        return reinterpret_cast<codegen::i64_hash_row_f>(rI64HashCallback);
    }

    codegen::str_hash_row_f ResolveTask::writeStringHashTableAggregateCallback() {
        return reinterpret_cast<codegen::str_hash_row_f>(rStrHashAggCallback);
    }
    codegen::i64_hash_row_f  ResolveTask::writeInt64HashTableAggregateCallback() {
        return reinterpret_cast<codegen::i64_hash_row_f>(rI64HashAggCallback);
    }

    void ResolveTask::writeRowToHashTable(char *key, size_t key_size, bool bucketize, char *buf, size_t buf_size) {
        // from TransformTask
        // @TODO: refactor more nicely using traits?
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable->hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // put into hashmap or null bucket
        if(key != nullptr && key_size > 0) {
            // put into hashmap!
            uint8_t *bucket = nullptr;
            if(bucketize) {
                hashmap_get(_htable->hm, key, key_size, (void **) (&bucket));
                // update or new entry
                bucket = extend_bucket(bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
            }
            hashmap_put(_htable->hm, key, key_size, bucket);
        } else {
            // goes into null bucket, no hash
            _htable->null_bucket = extend_bucket(_htable->null_bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
        }
    }

    void ResolveTask::writeRowToHashTableAggregate(char *key, size_t key_len, bool bucketize, char *buf, size_t buf_size) {
        // from TransformTask
        // @TODO: refactor more nicely using traits?
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable->hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // get the bucket
        uint8_t *bucket = nullptr;
        if(key != nullptr && key_len > 0) {
            // get current bucket
            hashmap_get(_htable->hm, key, key_len, (void **) (&bucket));
        } else {
            // goes into null bucket, no hash
            bucket = _htable->null_bucket;
        }

        // aggregate in the new value
        aggregateValues(&bucket, buf, buf_size);

        // write back the bucket
        if(key != nullptr && key_len > 0) {
            hashmap_put(_htable->hm, key, key_len, bucket);
        } else {
            _htable->null_bucket = bucket;
        }
    }

    void ResolveTask::writeRowToHashTable(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size) {
        // from TransformTask
        // @TODO: refactor more nicely using traits?
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable->hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // put into hashmap or null bucket
        if(!key_null) {
            // put into hashmap!
            uint8_t *bucket = nullptr;
            if(bucketize) {
                int64_hashmap_get(_htable->hm, key, (void **) (&bucket));
                // update or new entry
                bucket = extend_bucket(bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
            }
            int64_hashmap_put(_htable->hm, key, bucket);
        } else {
            // goes into null bucket, no hash
            _htable->null_bucket = extend_bucket(_htable->null_bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
        }
    }

    void ResolveTask::writeRowToHashTableAggregate(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size) {
        // from TransformTask
        // @TODO: refactor more nicely using traits?
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable->hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // get the bucket
        uint8_t *bucket = nullptr;
        if(!key_null) {
            // get current bucket
            int64_hashmap_get(_htable->hm, key, (void **) (&bucket));
        } else {
            bucket = _htable->null_bucket;
        }
        // aggregate in the new value
        aggregateValues(&bucket, buf, buf_size);
        if(!key_null) {
            // get current bucket
            int64_hashmap_put(_htable->hm, key, bucket);
        } else {
            // goes into null bucket, no hash
            _htable->null_bucket = bucket;
        }
    }


    PyObject * ResolveTask::tupleFromParseException(const uint8_t* ebuf, size_t esize) {
        // cf.  char* serializeParseException(int64_t numCells,
        //            char **cells,
        //            int64_t* sizes,
        //            size_t *buffer_size,
        //            std::vector<bool> colsToSerialize,
        //            decltype(malloc) allocator)
        int64_t num_cells = *(int64_t*)ebuf; ebuf += sizeof(int64_t);
        PyObject* tuple = PyTuple_New(num_cells);
        for(unsigned j = 0; j < num_cells; ++j) {
            auto info = *(int64_t*)ebuf;
            auto offset = info & 0xFFFFFFFF;
            const char* cell = reinterpret_cast<const char *>(ebuf + offset);

            // @TODO: quicker conversion from str cell?
            PyTuple_SET_ITEM(tuple, j, python::PyString_FromString(cell));

            auto cell_size = info >> 32u;
            ebuf += sizeof(int64_t);
        }
        return tuple;
    }

    void ResolveTask::writePythonObjectToFallbackSink(PyObject *out_row) {
        assert(out_row);

        // similar to merge row, need to write other rows first!
        // -> this updates the rowNumber counter
        if(_mergeRows)
            emitNormalRows();

        // needs to be put into separate list of python objects...
        // save index as well to merge back in order.
        assert(_currentRowNumber >= _numUnresolved);
        auto pickledObject = python::pickleObject(python::getMainModule(), out_row);
        auto pyObjectSize = pickledObject.size();
        auto bufSize = 4 * sizeof(int64_t) + pyObjectSize;

        uint8_t *buf = new uint8_t[bufSize];
        auto ptr = buf;
        *((int64_t*)ptr) = _currentRowNumber - _numUnresolved; ptr += sizeof(int64_t);
        *((int64_t*)ptr) = ecToI64(ExceptionCode::PYTHON_PARALLELIZE); ptr += sizeof(int64_t);
        *((int64_t*)ptr) = -1; ptr += sizeof(int64_t);
        *((int64_t*)ptr) = pyObjectSize; ptr += sizeof(int64_t);
        memcpy(ptr, pickledObject.c_str(), pyObjectSize);
        rowToMemorySink(owner(), _fallbackSink, Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING})),
                        0, contextID(), buf, bufSize);
        delete[] buf;
    }

    int64_t ResolveTask::mergeNormalRow(const uint8_t *buf, int64_t bufSize) {
        using namespace std;

        // if merging was disabled, simply write rows out
        if(!_mergeRows) {
            writeRow(buf, bufSize);
            return 0;
        }

        // emit all the normals rows now to perform the merge operation
        emitNormalRows();

        // copy exceptional row to partition
        writeRow(buf, bufSize);
#ifdef TRACE_EXCEPTIONS
        std::cout<<"resolved row: "<<_rowNumber<<std::endl;
#endif
        return 0;
    }

    int64_t ResolveTask::mergeRow(const uint8_t *buf, int64_t bufSize, int bufFormat) {
        using namespace std;

        // what format is it in?
        switch(bufFormat) {
            case BUF_FORMAT_NORMAL_OUTPUT:
                return mergeNormalRow(buf, bufSize);
            case BUF_FORMAT_COMPILED_RESOLVE:
            case BUF_FORMAT_GENERAL_OUTPUT: {
                // is resolveOutput different from normal case?
                if(_resolverOutputSchema.getRowType().hash() == _targetOutputSchema.getRowType().hash()) {
                    return mergeNormalRow(buf, bufSize);
                } else {
                    // ok, resolverSchema == targetGeneralCaseOutputSchema?
                    if(_resolverOutputSchema.getRowType().hash() == commonCaseOutputSchema().getRowType().hash()) {
                        // store in general case sink
                        // make normal case violation
                        size_t except_size = 0; // 4 8-byte fields (isn't that a bit overkill?)
                        // exceptionCode, exceptionOperatorID, rowNumber, size
                        int64_t ecCode = ecToI64(ExceptionCode::NORMALCASEVIOLATION);
                        int64_t ecOpID = 0; // dummy
                        int64_t rowNumber = _currentRowNumber - _numUnresolved;
                        uint8_t* except_buf = serializeExceptionToMemory(ecCode, ecOpID, rowNumber, buf, bufSize, &except_size);

                        // sink row to type violation exceptions with commonCaseOutputSchema
                        rowToMemorySink(owner(), _generalCaseSink, commonCaseOutputSchema(),
                                        0, contextID(), except_buf, except_size);
                        if(except_buf)
                            free(except_buf);
                        return 0;
                    } else {
                        // need to cast from resolve output schema to general case output schema.
                        // if this doesn't work, then store as python object
                        throw std::runtime_error("not yet implemented, only supports case where resolve output matches target general case");
                    }
                }
                break;
            }
#ifndef NDEBUG
default:
    throw std::runtime_error("unknown buffer format in resolve task");
#endif
        }
        return 0;
    }

    void ResolveTask::emitNormalRows() {

        auto& logger = Logger::instance().logger("resolve task");
        // logger.debug("emitting " + std::to_string(_currentRowNumber - _rowNumber) + " normal rows before current resolved row");

        // copy as many rows until current row number is reached...
        // EDIT: OR normal rows are exhausted??
        // ==> probably there is an issue with normal/exceptional rows.
        // tip: create mini viable example with Zillow data...
        if(_currentNormalPartitionIdx < _partitions.size()) {
            while(_rowNumber != _currentRowNumber) {
                assert(_normalNumRows > 0);
                assert(_rowNumber <= _currentRowNumber);
                assert(_normalRowNumber < _normalNumRows); // has to be true


                // when using file, error here because str format is used!!!
                // => change decoding here!!!
                size_t size = readOutputRowSize(_normalPtr, _normalPtrBytesRemaining);

                // copy normal row to merged partitions

                // make sure not all normal rows have been exhausted yet:
                if(_normalRowNumber < _normalNumRows) {
                    writeRow(_normalPtr, size);
                    _normalPtr += size;
                    _normalPtrBytesRemaining -= size;
#ifdef TRACE_EXCEPTIONS
                    std::cout<<"normal row: "<<_rowNumber<<std::endl;
#endif
                }

                // inc row numbers and fetch potentially next normal partition for the merge
                _normalRowNumber++;
                _rowNumber++;

                // next normal partition?
                if(_normalRowNumber == _normalNumRows) {
                    // check if there is a partition left
                    if(_currentNormalPartitionIdx + 1 < _partitions.size()) {
                        _partitions[_currentNormalPartitionIdx]->unlock();
                        _currentNormalPartitionIdx++;

                        _normalPtr = _partitions[_currentNormalPartitionIdx]->lockRaw();
                        _normalNumRows = *((int64_t*)_normalPtr); _normalPtr += sizeof(int64_t);
                        _normalPtrBytesRemaining = _partitions[_currentNormalPartitionIdx]->bytesWritten();
                        _normalRowNumber = 0;
                    } else {
#ifdef TRACE_EXCEPTIONS
                        // all normal rows exhausted!
                        std::cout<<"all normal rows exhausted!"<<std::endl;
#endif
                    }
                }
            }
        } else {

            // nothing to do.
            // Note: rows match only if no filter was involved
            //assert(_currentRowNumber == _rowNumber);
        }
    }

    static bool requiresInterpreterReprocessing(const ExceptionCode& ec) {
        switch(ec) {
            case ExceptionCode::BADPARSE_STRING_INPUT:
            case ExceptionCode::NORMALCASEVIOLATION:
            case ExceptionCode::PYTHON_PARALLELIZE:
                return true;
            default:
                return false;
        }
    }

    static  int print_hm_key(void* userData, hashmap_element* entry) {
        auto data = (uint8_t*)entry->data; // bucket data. First is always an int64_t holding how many rows there are.

        using namespace std;
        if(entry->in_use) {
            // cout<<"key: "<<entry->key;

            if(data) {
                // other bucket count
                auto num_entries =  (*(uint64_t*)data >> 32ul);
                auto x = *(int64_t*)(data + sizeof(int64_t));
                // cout<<" "<<pluralize(num_entries, "value");
                if(userData)
                    *(int64_t*)userData += x;
            }
//
//            cout<<endl;
        }

        return MAP_OK;
    }

    void ResolveTask::processExceptionRow(int64_t& ecCode, int64_t operatorID, const uint8_t* ebuf, size_t eSize) {
        // inc counter here, hence only count exception rows!
        _numInputRowsRead++;

#ifndef NDEBUG
        {
             // // uncomment to check in debugger easier what rows are used.
             // // use this code to get exception info:
             // auto row = i64ToEC(ecCode) != ExceptionCode::BADPARSE_STRING_INPUT ?
             //         Row::fromMemory(getExceptionSchema(), ebuf, eSize) :
             //         Row("bad parse string input");
             //
             // if(i64ToEC(ecCode) != ExceptionCode::BADPARSE_STRING_INPUT) {
             //     std::cerr<<"NVO: "<<std::endl;
             // }
             //
             // auto row_str = row.toPythonString();
             //
             //
             // std::cout<<"Row "<<_currentRowNumber
             // <<" (OperatorID="<<operatorID<<", ecCode="
             // <<exceptionCodeToPythonClass(i64ToEC(ecCode))<<"):\n"<<row_str<<std::endl;

        }
#endif

        // To super verify everything, skip this quick escape path.
        // However, this leads to great speed improvement...
        // not all codes qualify for reprocessing => only internals should get reprocessed!
        // => other error codes are "true" exceptions
        // => if it's a true exception, simply save it again as exception.
        bool potentiallyHasResolverOnSlowPath = !_operatorIDsAffectedByResolvers.empty() &&
                                                std::binary_search(_operatorIDsAffectedByResolvers.begin(),
                                                                   _operatorIDsAffectedByResolvers.end(), operatorID);
        if(!requiresInterpreterReprocessing(i64ToEC(ecCode)) && !potentiallyHasResolverOnSlowPath) {
            // TODO: check with resolvers!
            // i.e., we can directly save this as exception IF code is not an interpreter code
            // and true exception, i.e. no resolvers available.
            // => need a list of for which opIds/codes resolvers are available...
            ///....
            _numUnresolved++;
            exceptionCallback(ecCode, operatorID, _currentRowNumber, ebuf, eSize);
            return;
        }

        // fallback 1: slow, compiled code path
        int resCode = -1;
        if(_functor && ecCode != ecToI32(ExceptionCode::PYTHON_PARALLELIZE)) {
            resCode = _functor(this, _rowNumber, ecCode, ebuf, eSize);
            // uncomment to print out details on demand
            // if(resCode != 0) {
            //     std::cout<<"functor delivered resCode "<<resCode<<std::endl;
            // }

            // normal-case violation too? -> backup via interpreter!
            if(resCode == ecToI32(ExceptionCode::NORMALCASEVIOLATION)) {
                if(!_interpreterFunctor) {
#ifndef NDEBUG
                    std::cerr<<"normal case violation encountered, but no interpreter backup?"<<std::endl;
#endif
                }
                resCode = -1;
                // exception occured that is not a schema violation so row will not be present in output
            } else if (resCode != 0) {
                _numUnresolved++;
            }
        }

        // fallback 2: interpreter path
        // --> only go there if a non-true exception was recorded. Else, it will be dealt with above
        if(resCode == -1 && _interpreterFunctor) {

            // acquire GIL
            python::lockGIL();
            PyCallable_Check(_interpreterFunctor);

            // holds the pythonized data
            PyObject* tuple = nullptr;

            bool parse_cells = false;

            // there are different data reps for certain error codes.
            // => decode the correct object from memory & then feed it into the pipeline...
            if(ecCode == ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)) {
                // it's a string!
                tuple = tupleFromParseException(ebuf, eSize);
                parse_cells = true; // need to parse cells in python mode.
            } else if(ecCode == ecToI64(ExceptionCode::NORMALCASEVIOLATION)) {
                // changed, why are these names so random here? makes no sense...
                auto row = Row::fromMemory(exceptionsInputSchema(), ebuf, eSize);

                tuple = python::rowToPython(row, true);
                parse_cells = false;
                // called below...
            } else if (ecCode == ecToI64(ExceptionCode::PYTHON_PARALLELIZE)) {
                auto pyObj = python::deserializePickledObject(python::getMainModule(), (char *) ebuf, eSize);
                tuple = pyObj;
                parse_cells = false;
            } else {
                // normal case, i.e. an exception occurred somewhere.
                // --> this means if pipeline is using string as input, we should convert
                auto row = Row::fromMemory(exceptionsInputSchema(), ebuf, eSize);

                // cell source automatically takes input, i.e. no need to convert. simply get tuple from row object
                tuple = python::rowToPython(row, true);

#ifndef NDEBUG
                if(PyTuple_Check(tuple)) {
                    // make sure tuple is valid...
                    for(unsigned i = 0; i < PyTuple_Size(tuple); ++i) {
                        auto elemObj = PyTuple_GET_ITEM(tuple, i);
                        assert(elemObj);
                    }
                }
#endif
                parse_cells = false;
            }

            // compute
            // @TODO: we need to encode the hashmaps as these hybrid objects!
            // ==> for more efficiency we prob should store one per executor!
            //     the same goes for any hashmap...

            assert(tuple);
#ifndef NDEBUG
            if(!tuple) {
                owner()->error("bad decode, using () as dummy...");
                tuple = PyTuple_New(0); // empty tuple.
            }
#endif

            // note: current python pipeline always expects a tuple arg. hence pack current element.
            if(PyTuple_Check(tuple) && PyTuple_Size(tuple) > 1) {
                // nothing todo...
            } else {
                auto tmp_tuple = PyTuple_New(1);
                PyTuple_SET_ITEM(tmp_tuple, 0, tuple);
                tuple = tmp_tuple;
            }

#ifndef NDEBUG
            // // to print python object
            // Py_XINCREF(tuple);
            // PyObject_Print(tuple, stdout, 0);
            // std::cout<<std::endl;
#endif

            // call pipFunctor
            size_t num_python_args = 1 + _py_intermediates.size() + hasHashTableSink();

            // special case unique, no arg required (done via output)
            if(hasHashTableSink() && _hash_agg_type == AggregateType::AGG_UNIQUE)
                num_python_args -= 1;

            PyObject* args = PyTuple_New(num_python_args);
            PyTuple_SET_ITEM(args, 0, tuple);
            for(unsigned i = 0; i < _py_intermediates.size(); ++i) {
                Py_XINCREF(_py_intermediates[i]);
                PyTuple_SET_ITEM(args, i + 1, _py_intermediates[i]);
            }
            // set hash table sink
            if(hasHashTableSink() && _hash_agg_type != AggregateType::AGG_UNIQUE) { // special case: unique -> note: unify handling this with the other cases...
                assert(_htable->hybrid_hm);
                Py_XINCREF(_htable->hybrid_hm);
                PyTuple_SET_ITEM(args, num_python_args - 1, _htable->hybrid_hm);
            }

#ifndef NDEBUG
            if(hasHashTableSink()) {
                using namespace std;
                cout<<"hash sink result is: "<<endl;
                auto hsink = _htable;
                assert(hsink);
                if(hsink->hm) {
                    cout<<"hashmap contains following keys: "<<endl;
                    int64_t test = 0;
                    hashmap_iterate(hsink->hm, print_hm_key, &test);
                    cout<<"total rows: "<<test<<endl;
                }
            }

#endif


            auto kwargs = PyDict_New(); PyDict_SetItemString(kwargs, "parse_cells", python::boolean(parse_cells));
            auto pcr = python::callFunctionEx(_interpreterFunctor, args, kwargs);

            if(pcr.exceptionCode != ExceptionCode::SUCCESS) {
                // this should not happen, bad internal error. codegen'ed python should capture everything.
                owner()->error("bad internal python error: " + pcr.exceptionMessage);
                python::unlockGIL();
                return;
            } else {
                // all good, row is fine. exception occurred?
                assert(pcr.res);

                // type check: save to regular rows OR save to python row collection
                if(!pcr.res) {
                    owner()->error("bad internal python error, NULL object returned");
                } else {

#ifndef NDEBUG
                    // // uncomment to print res obj
                    // Py_XINCREF(pcr.res);
                    // PyObject_Print(pcr.res, stdout, 0);
                    // std::cout<<std::endl;
#endif
                    auto exceptionObject = PyDict_GetItemString(pcr.res, "exception");
                    if(exceptionObject) {

                        // overwrite operatorID which is throwing.
                        auto exceptionOperatorID = PyDict_GetItemString(pcr.res, "exceptionOperatorID");
                        operatorID = PyLong_AsLong(exceptionOperatorID);
                        auto exceptionType = PyObject_Type(exceptionObject);
                        // can ignore input row.
                        ecCode = ecToI64(python::translatePythonExceptionType(exceptionType));

#ifndef NDEBUG
                        // // debug printing of exception and what the reason is...
                        // // print res obj
                        // Py_XINCREF(pcr.res);
                        // std::cout<<"exception occurred while processing using python: "<<std::endl;
                        // PyObject_Print(pcr.res, stdout, 0);
                        // std::cout<<std::endl;
#endif

                        // the callback exceptionCallback(ecCode, opID, _rowNumber, ebuf, eSize) gets called below...!
                        resCode = -1;
                    } else {
                        // normal, check type and either merge to normal set back OR onto python set together with row number?
                        auto resultRows = PyDict_GetItemString(pcr.res, "outputRows");

                        // no output rows? continue.
                        if(!resultRows) {
                            python::unlockGIL();
                            return;
                        }

                        assert(PyList_Check(resultRows));

                        auto listSize = PyList_Size(resultRows);
                        // No rows were created, meaning the row was filtered out
                        if (0 == listSize) {
                            _numUnresolved++;
                        }


                        {
#ifndef NDEBUG
                            // // debug
                            // Py_XINCREF(resultRows);
                            // PyObject_Print(resultRows, stdout, 0); std::cout<<std::endl;
#endif
                        }

                        for(int i = 0; i < listSize; ++i) {
                            // type check w. output schema
                            // cf. https://pythonextensionpatterns.readthedocs.io/en/latest/refcount.html
                            auto rowObj = PyList_GetItem(resultRows, i);
                            Py_XINCREF(rowObj);


                            // because we have the logic to separate types etc. in the hashtable, for hash table output we can use
                            // simplified output schema here!
                            if(hasHashTableSink()) {
                                auto key = PyDict_GetItemString(pcr.res, "key");
                                sinkRowToHashTable(rowObj, key);
                                continue;
                            }

                            auto rowType = python::mapPythonClassToTuplexType(rowObj, false);

                            // special case output schema is str (fileoutput!)
                            if(rowType == python::Type::STRING) {
                                // write to file, no further type check necessary b.c.
                                // if it was the object string it would be within a tuple!
                                auto cptr = PyUnicode_AsUTF8(rowObj);
                                Py_XDECREF(rowObj);
                                mergeRow(reinterpret_cast<const uint8_t *>(cptr), strlen(cptr), BUF_FORMAT_NORMAL_OUTPUT); // don't write '\0'!
                            } else {

                                // there are three options where to store the result now

                                // 1. fits targetOutputSchema (i.e. row becomes normalcase row)
                                bool outputAsNormalRow = python::Type::UNKNOWN != unifyTypes(rowType, _targetOutputSchema.getRowType(), _allowNumericTypeUnification)
                                                         && canUpcastToRowType(rowType, _targetOutputSchema.getRowType());
                                // 2. fits generalCaseOutputSchema (i.e. row becomes generalcase row)
                                bool outputAsGeneralRow = python::Type::UNKNOWN != unifyTypes(rowType,
                                                                                              commonCaseOutputSchema().getRowType(), _allowNumericTypeUnification)
                                                          && canUpcastToRowType(rowType, commonCaseOutputSchema().getRowType());

                                // 3. doesn't fit, store as python object. => we should use block storage for this as well. Then data can be shared.

                                // can upcast? => note that the && is necessary because of cases where outputSchema is
                                // i64 but the given row type f64. We can cast up i64 to f64 but not the other way round.
                                if(outputAsNormalRow) {
                                    Row resRow = python::pythonToRow(rowObj).upcastedRow(_targetOutputSchema.getRowType());
                                    assert(resRow.getRowType() == _targetOutputSchema.getRowType());

                                    // write to buffer & perform callback
                                    auto buf_size = 2 * resRow.serializedLength();
                                    uint8_t *buf = new uint8_t[buf_size];
                                    memset(buf, 0, buf_size);
                                    auto serialized_length = resRow.serializeToMemory(buf, buf_size);
                                    // call row func!
                                    // --> merge row distinguishes between those two cases. Distinction has to be done there
                                    //     because of compiled functor who calls mergeRow in the write function...
                                    mergeRow(buf, serialized_length, BUF_FORMAT_NORMAL_OUTPUT);
                                    delete [] buf;
                                } else if(outputAsGeneralRow) {
                                    Row resRow = python::pythonToRow(rowObj).upcastedRow(commonCaseOutputSchema().getRowType());
                                    assert(resRow.getRowType() == commonCaseOutputSchema().getRowType());

                                    // write to buffer & perform callback
                                    auto buf_size = 2 * resRow.serializedLength();
                                    uint8_t *buf = new uint8_t[buf_size];
                                    memset(buf, 0, buf_size);
                                    auto serialized_length = resRow.serializeToMemory(buf, buf_size);
                                    // call row func!
                                    // --> merge row distinguishes between those two cases. Distinction has to be done there
                                    //     because of compiled functor who calls mergeRow in the write function...
                                    mergeRow(buf, serialized_length, BUF_FORMAT_GENERAL_OUTPUT);
                                    delete [] buf;
                                } else {
                                    // Unwrap single element tuples before writing them to the fallback sink
                                    if(PyTuple_Check(rowObj) && PyTuple_Size(rowObj) == 1) {
                                        writePythonObjectToFallbackSink(PyTuple_GetItem(rowObj, 0));
                                    } else {
                                        writePythonObjectToFallbackSink(rowObj);
                                    }
                                }
                                // Py_XDECREF(rowObj);
                            }
                        }

#ifndef NDEBUG
                        if(PyErr_Occurred()) {
                            // print out the otber objects...
                            std::cout<<__FILE__<<":"<<__LINE__<<" python error not cleared properly!"<<std::endl;
                            PyErr_Print();
                            std::cout<<std::endl;
                            PyErr_Clear();
                        }
#endif

                        // everything was successful, change resCode to 0!
                        resCode = 0;
                    }
                }
            }

            python::unlockGIL();
        }

        // fallback 3: still exception? save...
        if(resCode == -1) {
            _numUnresolved++;
            exceptionCallback(ecCode, operatorID, _currentRowNumber, ebuf, eSize);
        }
    }

    void ResolveTask::execute() {

        // Note: if output is hash-table then order doesn't really matter
        // --> can process things independently from each other.

        using namespace std;

        Timer timer;

        _numInputRowsRead = 0;

        // alloc hashmap if required
        if(hasHashTableSink()) {
            if(!_htable->hm)
                _htable->hm = hashmap_new();

            python::lockGIL();
            // init hybrid
            auto adjusted_key_type = _hash_element_type.isTupleType() && _hash_element_type.parameters().size() == 1 ?
                                     _hash_element_type.parameters().front() : _hash_element_type;

            // null bucket will receive NULLs always.
            if(adjusted_key_type.isOptionType())
                adjusted_key_type = adjusted_key_type.elementType();

            // unique adjustment, unknown bucket type -> None
            if(_hash_bucket_type == python::Type::UNKNOWN && _hash_agg_type == AggregateType::AGG_UNIQUE)
                _hash_bucket_type = python::Type::NULLVALUE;

#ifndef NDEBUG
            assert(owner());
            owner()->info("initializing hybrid hash table with keytype=" + adjusted_key_type.desc() + ", valuetype=" + _hash_bucket_type.desc());
#endif
            // value mode
            LookupStorageMode valueMode;
            if(_hash_agg_type == AggregateType::AGG_BYKEY || _hash_agg_type == AggregateType::AGG_GENERAL) {
                valueMode = LookupStorageMode::VALUE;
            } else {
                // list of values (i.e. for a join)
                valueMode = LookupStorageMode::LISTOFVALUES;
            }
            auto hybrid = CreatePythonHashMapWrapper(*_htable, adjusted_key_type,
                                                                                        _hash_bucket_type, valueMode);
            assert(_htable->hybrid_hm);
            assert(reinterpret_cast<uintptr_t>(hybrid) == reinterpret_cast<uintptr_t>(_htable->hybrid_hm)); // objects are the same pointer!
            python::unlockGIL();
        }

        // abort if no exceptions!
        if(_exceptionPartitions.empty() && _generalPartitions.empty() && _fallbackPartitions.empty())
            return;

        // special case: no functor & no python pipeline functor given
        // => everything becomes an exception!
        if(!_functor && !_interpreterFunctor) {
            // _normalCasePartitions stay the same

#ifndef NDEBUG
            cout<<"DESIGN WARNING: should check code s.t. that no resolve tasks are produced in this case here."<<endl;
#endif

            // copy _generalCasePartitions over to base class
            IExceptionableTask::setExceptions(_generalPartitions);

            // clear exceptions, because they have been resolved (or put to new exceptions!)
            // if task produced exceptions, they are stored in the IExceptionableTask class!
            // => no need to overwrite them, getter for iexceptionabletask has all info!
            _generalPartitions.clear();
            _wallTime = timer.time();

            return;
        }

        // merging rows can be disabled, which makes the exception resolution faster
        // exec only over exception partitions
        if(!_mergeRows) {
            // resolve partitions
            // merge exceptions with normal rows after calling slow code over them...
            // basic idea is go over all exception partitions, execute row wise the resolution function
            // and merge the result back to the partitions
            for (const auto &partition : _generalPartitions) {
                const uint8_t *ptr = partition->lock();
                auto numRows = partition->getNumRows();
                for (int i = 0; i < numRows; ++i) {
                    const uint8_t *ebuf = nullptr;
                    int64_t ecCode = -1, operatorID = -1;
                    size_t eSize = 0;
                    auto delta = deserializeExceptionFromMemory(ptr, &ecCode, &operatorID, &_currentRowNumber, &ebuf,
                                                                &eSize);

                    processExceptionRow(ecCode, operatorID, ebuf, eSize);

                    ptr += delta;
                    _rowNumber++;
                }
                partition->unlock();
                partition->invalidate();
            }

            for (const auto &partition : _fallbackPartitions) {
                const uint8_t *ptr = partition->lock();
                auto numRows = partition->getNumRows();
                for (int i = 0; i < numRows; ++i) {
                    const uint8_t *ebuf = nullptr;
                    int64_t ecCode = -1, operatorID = -1;
                    size_t eSize = 0;
                    auto delta = deserializeExceptionFromMemory(ptr, &ecCode, &operatorID, &_currentRowNumber, &ebuf,
                                                                &eSize);

                    processExceptionRow(ecCode, operatorID, ebuf, eSize);

                    ptr += delta;
                    _rowNumber++;
                }
                partition->unlock();
                partition->invalidate();
            }

            for (const auto &partition : _exceptionPartitions) {
                const uint8_t *ptr = partition->lock();
                auto numRows = partition->getNumRows();
                for (int i = 0; i < numRows; ++i) {
                    const uint8_t *ebuf = nullptr;
                    int64_t ecCode = -1, operatorID = -1;
                    size_t eSize = 0;
                    auto delta = deserializeExceptionFromMemory(ptr, &ecCode, &operatorID, &_currentRowNumber, &ebuf,
                                                                &eSize);

                    processExceptionRow(ecCode, operatorID, ebuf, eSize);

                    ptr += delta;
                    _rowNumber++;
                }
                partition->unlock();
                partition->invalidate();
            }

            // merging is done, unlock the last partition & copy the others over.
            unlockAll();

            // all exceptions have been resolved. merge partition arrays together
            vector<Partition*> mergedPartitions;
            for(auto p : _partitions)
                mergedPartitions.push_back(p);
            for(auto p : _mergedRowsSink.partitions)
                mergedPartitions.push_back(p);

            // overwrite merged partitions (& in future also exceptions!!!)
            _partitions = mergedPartitions;

            // clear exceptions, because they have been resolved (or put to new exceptions!)
            // if task produced exceptions, they are stored in the IExceptionableTask class!
            // => no need to overwrite them, getter for iexceptionabletask has all info!
            _exceptionPartitions.clear();
            _generalPartitions.clear();
            _fallbackPartitions.clear();
        } else {
            executeInOrder();
        }

        _wallTime = timer.time();

        // print out status
        std::stringstream ss;
        ss<<"[Task Finished] Resolve "<<"in "
          <<std::to_string(wallTime())<<"s";
        // @TODO: include exception info & Co
        owner()->info(ss.str());

        // send status update, i.e. if exceptions were resolved then it's time to reflect this!
        // ==> i.e. send here delta of resolved rows!
        // ==> if resolvers throw exceptions, send here too what happened (in terms of delta)
        // + a traceback
        sendStatusToHistoryServer();
    }

    void ResolveTask::executeInOrder() {
        auto& logger = Logger::instance().logger("resolve task");

        // Determine if normal partitions exist
        if(!_partitions.empty()) {
            // merge normal partitions and resolved ones (incl. lookup)

            // extract number of fixed columns to decode normal row size
            //!! when optimizing later this will fail !!

            // ready normal partition for merge
            _currentNormalPartitionIdx = 0;
            _normalPtr = _partitions[_currentNormalPartitionIdx]->lockRaw();
            _normalNumRows = *((int64_t *) _normalPtr);
            _normalPtr += sizeof(int64_t);
            _normalPtrBytesRemaining = _partitions[_currentNormalPartitionIdx]->bytesWritten();
            _normalRowNumber = 0;
            _rowNumber = 0;
        } else {
            _currentNormalPartitionIdx = 0;
            _normalPtr = nullptr;
            _normalPtrBytesRemaining = 0;
            _normalNumRows = 0;
            _normalRowNumber = 0;
            _rowNumber = 0;
        }

        size_t curExceptionInd = 0;
        size_t exceptionsRemaining = 0;
        const uint8_t *expPtr = nullptr;
        size_t exceptionNumRows = 0;
        for (int i = 0; i < _exceptionPartitions.size(); ++i) {
            auto numRows = _exceptionPartitions[i]->getNumRows();
            exceptionNumRows += numRows;
            if (i == 0) {
                expPtr = _exceptionPartitions[i]->lock();
                exceptionsRemaining = numRows;
            }
        }

        size_t curGeneralInd = 0;
        size_t generalRemaining = 0;
        const uint8_t *generalPtr = nullptr;
        size_t generalNumRows = 0;
        for (int i = 0; i < _generalPartitions.size(); ++i) {
            auto numRows = _generalPartitions[i]->getNumRows();
            generalNumRows += numRows;
            if (i == 0) {
                generalPtr = _generalPartitions[i]->lock();
                generalRemaining = numRows;
            }
        }

        size_t curFallbackInd = 0;
        size_t fallbackRemaining = 0;
        const uint8_t *fallPtr = nullptr;
        size_t fallbackNumRows = 0;
        for (int i = 0; i < _fallbackPartitions.size(); ++i) {
            auto numRows = _fallbackPartitions[i]->getNumRows();
            fallbackNumRows += numRows;
            if (i == 0) {
                fallPtr = _fallbackPartitions[i]->lock();
                fallbackRemaining = numRows;
            }
        }

        // Merge input and runtime exceptions in order. To do so, we can compare the row indices of the
        // current runtime and input exception and process the one that occurs first. The saved row indices of
        // runtime exceptions do not account for the existence of input exceptions, so we need to add the previous
        // input exceptions to compare the true row number
        while (_exceptionCounter < exceptionNumRows && _generalCounter < generalNumRows && _fallbackCounter < fallbackNumRows) {
            auto expRowInd = *((int64_t *) expPtr) + _fallbackCounter + _generalCounter;
            auto generalRowInd = *((int64_t *) generalPtr) + _fallbackCounter;
            auto fallbackRowInd = *((int64_t *) fallPtr);

            const uint8_t *buf = nullptr;
            int64_t ecCode = 0, operatorID = -1;
            size_t eSize = 0;
            if (fallbackRowInd <= expRowInd && fallbackRowInd <= generalRowInd) {
                fallbackRemaining--;
                _fallbackCounter++;

                auto delta = deserializeExceptionFromMemory(fallPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                fallPtr += delta;
            } else if (generalRowInd <= expRowInd && generalRowInd <= fallbackRowInd) {
                generalRemaining--;
                _generalCounter++;

                auto delta = deserializeExceptionFromMemory(generalPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                _currentRowNumber += _fallbackCounter;
                generalPtr += delta;
            } else {
                exceptionsRemaining--;
                _exceptionCounter++;

                auto delta = deserializeExceptionFromMemory(expPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                _currentRowNumber += _fallbackCounter + _generalCounter;
                expPtr += delta;
            }

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            if (exceptionsRemaining == 0) {
                _exceptionPartitions[curExceptionInd]->unlock();
                _exceptionPartitions[curExceptionInd]->invalidate();
                curExceptionInd++;
                if (curExceptionInd < _exceptionPartitions.size()) {
                    exceptionsRemaining = _exceptionPartitions[curExceptionInd]->getNumRows();
                    expPtr = _exceptionPartitions[curExceptionInd]->lock();
                }
            }

            if (generalRemaining == 0) {
                _generalPartitions[curGeneralInd]->unlock();
                _generalPartitions[curGeneralInd]->invalidate();
                curGeneralInd++;
                if (curGeneralInd < _generalPartitions.size()) {
                    generalRemaining = _generalPartitions[curGeneralInd]->getNumRows();
                    generalPtr = _generalPartitions[curGeneralInd]->lock();
                }
            }

            if (fallbackRemaining == 0) {
                _fallbackPartitions[curFallbackInd]->unlock();
                _fallbackPartitions[curFallbackInd]->invalidate();
                curFallbackInd++;
                if (curFallbackInd < _fallbackPartitions.size()) {
                    fallbackRemaining = _fallbackPartitions[curFallbackInd]->getNumRows();
                    fallPtr = _fallbackPartitions[curFallbackInd]->lock();
                }
            }
        }

        while (_exceptionCounter < exceptionNumRows && _generalCounter < generalNumRows) {
            auto expRowInd = *((int64_t *) expPtr) + _fallbackCounter + _generalCounter;
            auto generalRowInd = *((int64_t *) generalPtr) + _generalCounter;

            const uint8_t *buf = nullptr;
            int64_t ecCode = 0, operatorID = -1;
            size_t eSize = 0;
            if (generalRowInd <= expRowInd) {
                generalRemaining--;
                _generalCounter++;

                auto delta = deserializeExceptionFromMemory(generalPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                _currentRowNumber += _fallbackCounter;
                generalPtr += delta;
            } else {
                exceptionsRemaining--;
                _exceptionCounter++;

                auto delta = deserializeExceptionFromMemory(expPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                _currentRowNumber += _fallbackCounter + _generalCounter;
                expPtr += delta;
            }

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            if (exceptionsRemaining == 0) {
                _exceptionPartitions[curExceptionInd]->unlock();
                _exceptionPartitions[curExceptionInd]->invalidate();
                curExceptionInd++;
                if (curExceptionInd < _exceptionPartitions.size()) {
                    exceptionsRemaining = _exceptionPartitions[curExceptionInd]->getNumRows();
                    expPtr = _exceptionPartitions[curExceptionInd]->lock();
                }
            }

            if (generalRemaining == 0) {
                _generalPartitions[curGeneralInd]->unlock();
                _generalPartitions[curGeneralInd]->invalidate();
                curGeneralInd++;
                if (curGeneralInd < _generalPartitions.size()) {
                    generalRemaining = _generalPartitions[curGeneralInd]->getNumRows();
                    generalPtr = _generalPartitions[curGeneralInd]->lock();
                }
            }
        }

        while (_generalCounter < generalNumRows && _fallbackCounter < fallbackNumRows) {
            auto generalRowInd = *((int64_t *) generalPtr) + _fallbackCounter;
            auto fallbackRowInd = *((int64_t *) fallPtr);

            const uint8_t *buf = nullptr;
            int64_t ecCode = 0, operatorID = -1;
            size_t eSize = 0;
            if (fallbackRowInd <= generalRowInd) {
                fallbackRemaining--;
                _fallbackCounter++;

                auto delta = deserializeExceptionFromMemory(fallPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                fallPtr += delta;
            } else {
                generalRemaining--;
                _generalCounter++;

                auto delta = deserializeExceptionFromMemory(generalPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                _currentRowNumber += _fallbackCounter;
                generalPtr += delta;
            }

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            if (generalRemaining == 0) {
                _generalPartitions[curGeneralInd]->unlock();
                _generalPartitions[curGeneralInd]->invalidate();
                curGeneralInd++;
                if (curGeneralInd < _generalPartitions.size()) {
                    generalRemaining = _generalPartitions[curGeneralInd]->getNumRows();
                    generalPtr = _generalPartitions[curGeneralInd]->lock();
                }
            }

            if (fallbackRemaining == 0) {
                _fallbackPartitions[curFallbackInd]->unlock();
                _fallbackPartitions[curFallbackInd]->invalidate();
                curFallbackInd++;
                if (curFallbackInd < _fallbackPartitions.size()) {
                    fallbackRemaining = _fallbackPartitions[curFallbackInd]->getNumRows();
                    fallPtr = _fallbackPartitions[curFallbackInd]->lock();
                }
            }
        }

        while (_exceptionCounter < exceptionNumRows && _fallbackCounter < fallbackNumRows) {
            auto expRowInd = *((int64_t *) expPtr) + _fallbackCounter + _generalCounter;
            auto fallbackRowInd = *((int64_t *) fallPtr);

            const uint8_t *buf = nullptr;
            int64_t ecCode = 0, operatorID = -1;
            size_t eSize = 0;
            if (fallbackRowInd <= expRowInd) {
                fallbackRemaining--;
                _fallbackCounter++;

                auto delta = deserializeExceptionFromMemory(fallPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                fallPtr += delta;
            } else {
                exceptionsRemaining--;
                _exceptionCounter++;

                auto delta = deserializeExceptionFromMemory(expPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
                _currentRowNumber += _fallbackCounter + _generalCounter;
                expPtr += delta;
            }

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            if (exceptionsRemaining == 0) {
                _exceptionPartitions[curExceptionInd]->unlock();
                _exceptionPartitions[curExceptionInd]->invalidate();
                curExceptionInd++;
                if (curExceptionInd < _exceptionPartitions.size()) {
                    exceptionsRemaining = _exceptionPartitions[curExceptionInd]->getNumRows();
                    expPtr = _exceptionPartitions[curExceptionInd]->lock();
                }
            }

            if (fallbackRemaining == 0) {
                _fallbackPartitions[curFallbackInd]->unlock();
                _fallbackPartitions[curFallbackInd]->invalidate();
                curFallbackInd++;
                if (curFallbackInd < _fallbackPartitions.size()) {
                    fallbackRemaining = _fallbackPartitions[curFallbackInd]->getNumRows();
                    fallPtr = _fallbackPartitions[curFallbackInd]->lock();
                }
            }
        }

        while (_exceptionCounter < exceptionNumRows) {
            const uint8_t *buf = nullptr;
            int64_t ecCode = -1, operatorID = -1;
            size_t eSize = 0;
            auto delta = deserializeExceptionFromMemory(expPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
            _currentRowNumber += _generalCounter + _fallbackCounter;
            expPtr += delta;

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            exceptionsRemaining--;
            _exceptionCounter++;

            if (exceptionsRemaining == 0) {
                _exceptionPartitions[curExceptionInd]->unlock();
                _exceptionPartitions[curExceptionInd]->invalidate();
                curExceptionInd++;
                if (curExceptionInd < _exceptionPartitions.size()) {
                    exceptionsRemaining = _exceptionPartitions[curExceptionInd]->getNumRows();
                    expPtr = _exceptionPartitions[curExceptionInd]->lock();
                }
            }
        }

        while (_generalCounter < generalNumRows) {
            const uint8_t *buf = nullptr;
            int64_t ecCode = -1, operatorID = -1;
            size_t eSize = 0;
            auto delta = deserializeExceptionFromMemory(generalPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
            _currentRowNumber += _fallbackCounter;

            generalPtr += delta;

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            generalRemaining--;
            _generalCounter++;

            if (generalRemaining == 0) {
                _generalPartitions[curGeneralInd]->unlock();
                _generalPartitions[curGeneralInd]->invalidate();
                curGeneralInd++;
                if (curGeneralInd < _generalPartitions.size()) {
                    generalRemaining = _generalPartitions[curGeneralInd]->getNumRows();
                    generalPtr = _generalPartitions[curGeneralInd]->lock();
                }
            }
        }

        while (_fallbackCounter < fallbackNumRows) {
            const uint8_t *buf = nullptr;
            int64_t ecCode = -1, operatorID = -1;
            size_t eSize = 0;
            auto delta = deserializeExceptionFromMemory(fallPtr, &ecCode, &operatorID, &_currentRowNumber, &buf, &eSize);
            fallPtr += delta;

            processExceptionRow(ecCode, operatorID, buf, eSize);
            _rowNumber++;

            fallbackRemaining--;
            _fallbackCounter++;

            if (fallbackRemaining == 0) {
                _fallbackPartitions[curFallbackInd]->unlock();
                _fallbackPartitions[curFallbackInd]->invalidate();
                curFallbackInd++;
                if (curFallbackInd < _fallbackPartitions.size()) {
                    fallbackRemaining = _fallbackPartitions[curFallbackInd]->getNumRows();
                    fallPtr = _fallbackPartitions[curFallbackInd]->lock();
                }
            }
        }

        // add remaining normal rows & partitions to merged partitions
        while(_normalRowNumber < _normalNumRows) {
            // trick: to get row size, you know number of normal elements + variable length!
            // ==> can be used for quick merging!
            size_t size = readOutputRowSize(_normalPtr, _normalPtrBytesRemaining);

            writeRow(_normalPtr, size);
            _normalPtr += size;
            _normalPtrBytesRemaining -= size;
            _normalRowNumber++;
        }

        if (!_partitions.empty())
            _partitions[_currentNormalPartitionIdx]->unlock();

        // merging is done, unlock the last partition & copy the others over.
        unlockAll();

        for(int i = _currentNormalPartitionIdx + 1; i < _partitions.size(); ++i) {
            _mergedRowsSink.unlock();
            _mergedRowsSink.partitions.push_back(_partitions[i]);
        }

        // overwrite merged partitions (& in future also exceptions!!!)
        _partitions = _mergedRowsSink.partitions;

        // clear exceptions, because they have been resolved (or put to new exceptions!)
        // if task produced exceptions, they are stored in the IExceptionableTask class!
        // => no need to overwrite them, getter for iexceptionabletask has all info!
        _exceptionPartitions.clear();
        _generalPartitions.clear();
        _fallbackPartitions.clear();
    }

    void ResolveTask::sendStatusToHistoryServer() {

        // check first if history server exists
        // note important to save in variable here. Multi threads may change this...
        auto hs = owner()->historyServer();
        if(!hs)
            return;

        hs->sendTrafoTask(_stageID, 0, 0, this->exceptionCounts(), IExceptionableTask::getExceptions(), false);
    }

    void ResolveTask::unlockAll() {
        _mergedRowsSink.unlock();
        _generalCaseSink.unlock();
        _fallbackSink.unlock();

        // unlock exceptionable task
        IExceptionableTask::unlockAll();
    }

    size_t ResolveTask::readOutputRowSize(const uint8_t *buf, size_t bufSize) {
        assert(buf);

        // read row size depending on format
        switch(_outputFormat) {

            case FileFormat::OUTFMT_CSV: {
                // parse CSV row to get size (greedily parse all delimiters!!!)
                // => need to store output specification here for delimiter & co...
                char delimiter = _csvDelimiter;
                char quotechar = _csvQuotechar;

                return csvOffsetToNextLine(reinterpret_cast<const char*>(_normalPtr), _normalPtrBytesRemaining, delimiter, quotechar);
            }
            case FileFormat::OUTFMT_TUPLEX:
            case FileFormat::OUTFMT_ORC: {
                // tuplex in memory format
                assert(_deserializerNormalOutputCase);
                return std::min(_deserializerNormalOutputCase->inferLength(buf), bufSize);
            }
            default: {
                throw std::runtime_error("unsupported output format in resolve task!");
            }
        }

        return 0;
    }

    void ResolveTask::writeRow(const uint8_t *buf, size_t bufSize) {

        // when hash table is activated, output here has to go to a hash table!
        assert(!hasHashTableSink());

        rowToMemorySink(owner(), _mergedRowsSink, commonCaseOutputSchema(), 0, contextID(), buf, bufSize);
    }

    PyObject* unwrapTuple(PyObject* o) {
        if(PyTuple_Check(o) && PyTuple_Size(o) == 1) {
            auto item = PyTuple_GetItem(o, 0);
            Py_XINCREF(item);
            return item;
        }
        return o;
    }

    void ResolveTask::sinkRowToHashTable(PyObject *rowObject, PyObject* key) {
        using namespace std;

        // sink rowObject to hash table
        assert(rowObject);

        switch(_hash_agg_type) {
            case AggregateType::AGG_UNIQUE: {
                auto rowType = python::mapPythonClassToTuplexType(rowObject, false);

                // special case: Is it single element column? => this is the only supported right now for unique...
                if(rowType.parameters().size() == 1) {
                    // -> unwrap!
                    rowObject = PyTuple_GetItem(rowObject, 0);
                }

                // lazy create table
                if(!_htable->hybrid_hm) {

                    // adjust element type for single objects
                    // @TODO: this properly has to be thought through again...
                    auto adjusted_key_type = _hash_element_type.isTupleType() && _hash_element_type.parameters().size() == 1 ?
                            _hash_element_type.parameters().front() : _hash_element_type;

                    _htable->hybrid_hm = reinterpret_cast<PyObject *>(CreatePythonHashMapWrapper(*_htable,
                                                                                                adjusted_key_type,
                                                                                                _hash_bucket_type,
                                                                                                LookupStorageMode::VALUE));
                }

                assert(_htable->hybrid_hm);
                int rc =((HybridLookupTable*)_htable->hybrid_hm)->putItem(rowObject, nullptr);
                // could also invoke via PyObject_SetItem(_htable->hybrid_hm, rowObject, python::none());
                if(PyErr_Occurred()) {
                    PyErr_Print();
                    cout<<endl;
                    PyErr_Clear();
                }
                break;
            }

            case AggregateType::AGG_BYKEY: {
                // get key from result.
                assert(key);

                // unwrap tuple if necessary to store original value.
                rowObject = unwrapTuple(rowObject);

                // debug print
                {
#ifndef NDEBUG
                    Py_XINCREF(rowObject);
                    Py_XINCREF(key);

                    std::stringstream ss;
                    ss<<"sinking following to hash endpoint:\n";
                    auto skey = python::PyString_AsString(key);
                    if(skey == "For Hire Vehicle Complaint") {
                        std::cout<<"found to debug"<<std::endl;
                    }
                    ss<<"key: "<<python::PyString_AsString(key)<<"\n";
                    ss<<"object: "<<python::PyString_AsString(rowObject)<<"\n";
                    std::cout<<ss.str()<<std::endl;
#endif
                }


                // lazy create table
                if(!_htable->hybrid_hm) {
                    auto adjusted_key_type = _hash_element_type.isTupleType() && _hash_element_type.parameters().size() == 1 ?
                                             _hash_element_type.parameters().front() : _hash_element_type;

                    _htable->hybrid_hm = reinterpret_cast<PyObject *>(CreatePythonHashMapWrapper(*_htable,
                                                                                                adjusted_key_type,
                                                                                                _hash_bucket_type,
                                                                                                LookupStorageMode::LISTOFVALUES));
                    assert(_htable->hm && _htable->hybrid_hm);
                }

                assert(_htable->hybrid_hm);
                int rc =((HybridLookupTable*)_htable->hybrid_hm)->putItem(key, rowObject);
                if(PyErr_Occurred()) {
                    PyErr_Print();
                    cout<<endl;
                    PyErr_Clear();
                }
                break;
            }

            default: {
                string agg_mode_str = "unknown";
                if(_hash_agg_type == AggregateType::AGG_GENERAL)
                    agg_mode_str = "general";
                if(_hash_agg_type == AggregateType::AGG_BYKEY)
                    agg_mode_str = "bykey";
                string err_msg = "unsupported aggregate fallback for mode=" + agg_mode_str + " encountered, key type: " + _hash_element_type.desc() + ", bucket type: " + _hash_element_type.desc();
                owner()->error(err_msg);
                break;
            }
        }

        // two options: 1.) simple hash table
        // 2.) keyed hashtable -> i.e. extract key col, then put into hash table
        // 3.) potentially look up function to manipulate hash table (aggByKey?)
        // -> 3 functions in python: a.) init aggregate, b.) update aggregate c.) later: combine aggregates (this will be done last)
        // @TODO.
    }
}