//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_RESOLVETASK_H
#define TUPLEX_RESOLVETASK_H

#include <physical/execution/HybridHashTable.h>
#include <logical/AggregateOperator.h>
#include "physical/codegen/CodeDefs.h"
#include "physical/codegen/BlockBasedTaskBuilder.h"
#include "ee/local/Executor.h"
#include "physical/execution/IExceptionableTask.h"
#include "physical/execution/TransformTask.h"

// @TODO: invalidate partitions...
namespace tuplex {

    // @TODO: NOTE: the exception writing is WAY TO complex.
    // keep it simpler, just write in exception partition
    // row, eccode, load
    // ==> makes resolution easier...
    // if necessary, just count the rows once (or count on insert)...
    // --> makes also merging later easier... (because they arrive in order!)
    class ResolveTask : public IExceptionableTask {
    public:
        ResolveTask() = delete;

        /*!
         * create a new resolve task
         * @param stageID to which task belongs to
         * @param contextID to which context belongs to
         * @param partitions input rows with normal case
         * @param runtimeExceptions input rows for exceptions, in exception format
         * @param inputExceptions schema violations that occur during data loading
         * @param inputExceptionInfo values to map input partitions to their input exceptions
         * @param operatorIDsAffectedByResolvers operators that are followed by resolvers in the pipeline
         * @param inputSchema input schema of exception rows
         * @param outputSchema output schema which resolution must adhere to
         * @param mergeRows whether to merge rows in order (makes only sense when no hashjoin is involved)
         * @param functor compiled slow path row-based functor
         * @param interpreterFunctor python object representing the interpreter pipeline
         */

        // Resolve Task schemas:
        // => partitions have some sort of schema: I.e. the common case schema
        // => exceptions have different schemata, depending on the exception type
        // => 1. COMMONCASEVIOLATION => i.e. exception with data in general case, because data violated the common case
        // => 2  COMMONCASEVIOLATION_PYTHON => i.e. exception with data as pickled python object (tuple of objects), because data does not even adhere to the general case

        // now algorithm goes like this:
        // a general (desired) targetNormalCaseOutputSchema is given.
        // => if data does not adhere to the targetNormalCaseOutputSchema, it should be redone as commoncase violation/python violation exception with the LAST operator ID
        // need to define the schema what the resolve functor returns...
        ResolveTask(int64_t stageID,
                    int64_t contextID,
                    const std::vector<Partition*>& partitions,
                    const std::vector<Partition*>& exceptionPartitions,
                    const std::vector<Partition*>& generalPartitions,
                    const std::vector<Partition*>& fallbackPartitions,
                    const std::vector<int64_t>& operatorIDsAffectedByResolvers, //! used to identify which exceptions DO require reprocessing because there might be a resolver in the slow path for them.
                    Schema exceptionInputSchema, //! schema of the input rows in which both user exceptions and normal-case violations are stored in. This is also the schema in which rows which on the slow path produce again an exception will be stored in.
                    Schema resolverOutputSchema, //! schema of rows that the resolve function outputs if it doesn't rethrow exceptions
                    Schema targetNormalCaseOutputSchema, //! the schema what the resolver should produce, might require upcasting!
                    Schema targetGeneralCaseOutputSchema, //! the schema of rows which do not fit the normal case, but should be serialized as normal-case violations should be in. Used e.g., for caching.
                    bool mergeRows, //! whether to merge rows in order or not
                    bool allowNumericTypeUnification, //! whether to auto upcast numeric types, i.e. bool -> int -> float
                    FileFormat outputFormat, //! output format of normal rows. Required for merging
                    char csvDelimiter,
                    char csvQuotechar,
                    codegen::resolve_f functor=nullptr,
                    PyObject* interpreterFunctor=nullptr) : IExceptionableTask::IExceptionableTask(exceptionInputSchema, contextID),
                                                            _stageID(stageID),
                                                            _partitions(partitions),
                                                            _exceptionPartitions(exceptionPartitions),
                                                            _generalPartitions(generalPartitions),
                                                            _fallbackPartitions(fallbackPartitions),
                                                            _exceptionCounter(0),
                                                            _generalCounter(0),
                                                            _fallbackCounter(0),
                                                            _resolverOutputSchema(resolverOutputSchema),
                                                            _targetOutputSchema(targetNormalCaseOutputSchema),
                                                            _mergeRows(mergeRows),
                                                            _allowNumericTypeUnification(allowNumericTypeUnification),
                                                            _operatorIDsAffectedByResolvers(operatorIDsAffectedByResolvers),
                                                            _outputFormat(outputFormat),
                                                            _csvDelimiter(csvDelimiter),
                                                            _csvQuotechar(csvQuotechar),
                                                            _functor(functor),
                                                            _deserializerGeneralCaseOutput(new Deserializer(targetGeneralCaseOutputSchema)),
                                                            _deserializerNormalOutputCase(new Deserializer(_targetOutputSchema)),
                                                            _interpreterFunctor(interpreterFunctor),
                                                            _htable(nullptr),
                                                            _htableFormat(HashTableFormat::UNKNOWN),
                                                            _outputRowNumber(0),
                                                            _wallTime(0.0),
                                                            _numInputRowsRead(0),
                                                            _numUnresolved(0) {
            // copy the IDs and sort them so binary search can be used.
            std::sort(_operatorIDsAffectedByResolvers.begin(), _operatorIDsAffectedByResolvers.end());
            _normalPtrBytesRemaining = 0;
        }

        // @TODO: destructor, destroy list!

        static codegen::write_row_f mergeRowCallback();
        static codegen::exception_handler_f exceptionCallback();
        static codegen::str_hash_row_f writeStringHashTableCallback();
        static codegen::i64_hash_row_f writeInt64HashTableCallback();
        static codegen::str_hash_row_f writeStringHashTableAggregateCallback();
        static codegen::i64_hash_row_f writeInt64HashTableAggregateCallback();

        /*!
         * this function merges the following buffer into output.
         * schema of the row has to be resolverOutputSchema!!! DO NOT HOLD THE GIL WHEN CALLING THIS FUNCTION!
         * @param buf
         * @param bufSize
         * @param bufFormat in which format the row is. 0=resolver, 1=normal case, 2=general case
         * @return how many bytes were written to normal output partitions. Can be 0 when e.g. row goes to general case output exceptions.
         */
        int64_t mergeRow(const uint8_t* buf, int64_t bufSize, int bufFormat) noexcept;

        /*!
        * this function merges the following buffer into output.
        * schema of the row has to be resolverOutputSchema!!!
        * @param buf
        * @param bufSize
        * @param bufFormat in which format the row is. 0=resolver, 1=normal case, 2=general case
        * @return how many bytes were written to normal output partitions. Can be 0 when e.g. row goes to general case output exceptions.
        */
        int64_t mergeNormalRow(const uint8_t* buf, int64_t bufSize);

        inline void exceptionCallback(const int64_t ecCode, const int64_t opID, const int64_t row, const uint8_t *buf, const size_t bufSize) {

            // when using compiled fallback, only serialize non-normal case violation exceptions.
            // these are true, UDF produced exceptions.
            // Else use interpreter functor
            if(_interpreterFunctor && ecCode == ecToI64(ExceptionCode::NORMALCASEVIOLATION)) {
                // ... do nothing => will get resolved using fallback!
            } else {
                serializeException(ecCode, opID, row, buf, bufSize);
            }
        }
        void writeRowToHashTable(char *key, size_t key_size, bool bucketize, char *buf, size_t buf_size);
        void writeRowToHashTable(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size);
        void writeRowToHashTableAggregate(char *key, size_t key_size, bool bucketize, char *buf, size_t buf_size);
        void writeRowToHashTableAggregate(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size);

        /*!
         * sink output to hashtable
         * @param fmt format of the hashtable (i.e. grouped? globally grouped?)
         * @param hashKeyType the type of the key to hash for
         * @param hashBucketType the type of the rows to store in the table
         */
        void sinkOutputToHashTable(HashTableFormat fmt,
                                   const AggregateType& aggType,
                                   const python::Type& hashKeyType,
                                   const python::Type& hashBucketType, map_t hm=nullptr,
                                   uint8_t* null_bucket=nullptr) {
            _htableFormat = fmt;
            _hash_element_type = hashKeyType;
            _hash_bucket_type = hashBucketType;
            _hash_agg_type = aggType;

            // init sink if data is given
            assert(!_htable);
            _htable = new HashTableSink();
            _htable->hm = hm;
            _htable->null_bucket = null_bucket;

            // null bucket can stay nullptr, however hashmap needs to be initialized
            if(!_htable->hm)
                throw std::runtime_error("internal error: need to have valid hashmap assigned.");

            python::lockGIL();
            auto valueMode = aggType == AggregateType::AGG_BYKEY ? LookupStorageMode::LISTOFVALUES : LookupStorageMode::VALUE;
            auto hybrid = CreatePythonHashMapWrapper(*_htable, hashKeyType.withoutOption(), hashBucketType, valueMode);
            assert(reinterpret_cast<uintptr_t>(hybrid) == reinterpret_cast<uintptr_t>(_htable->hybrid_hm)); // same object
            python::unlockGIL();
        }

        HashTableSink* hashTableSink() const { return _htable; } // needs to be freed manually!
        HashTableSink* moveHashSink() { auto ptr = _htable; _htable = nullptr; return ptr; }
        bool hasHashTableSink() const { return _htableFormat != HashTableFormat::UNKNOWN; }

        void execute() override;

        TaskType type() const override { return TaskType::RESOLVE; }

        CodePathStat codePathStats() const { return _pathStats; }

        std::vector<Partition*> getOutputPartitions() const override { return _partitions; }

        std::vector<Partition*> getOutputFallbackPartitions() const { return _fallbackSink.partitions; }

        /// very important to override this because of the special two exceptions fields of ResolveTask
        /// i.e. _generalCasePartitions store what exceptions to resolve, IExceptionableTask::_generalCasePartitions exceptions that occurred
        /// during resolution.
        std::vector<Partition*> getExceptions() const override {

            // TODO: override here which exceptions to return
            // i.e. IExceptionableTask stores exceptions where rows produced errors on slow path as well

            // second set should be for exceptions which could be resolved using slower path or python, but do not adhere to the output schema!
            // => i.e. when caching data, throw away the IExceptionable exceptions, because they do not really matter...

            // @TODO: how does the cached exception work? i.e. need special op cacheExceptions() perhaps which tells to preserve exceptions for
            // future similar pipeline executions to iteratively resolve data...

            // when allowing things like map(...).resolve(...).cache().ignore(...).resolve(...) then
            // ONLY exceptions which are happening incl. map and after need to be serialized and returned.
            // all the others simply remain unresolved at this point and can only be resolved by introducing additional
            // pipeline logic.

            return IExceptionableTask::getExceptions();
        }

        /*!
         * return partitions of rows which do not adhere to the common case. I.e. to be reused by the except case.
         * @return
         */
        std::vector<Partition*> exceptionsFromTargetSchema() const { return _generalCaseSink.partitions; }

        void setHybridIntermediateHashTables(size_t numIntermediates, PyObject** intermediates) {
            _py_intermediates.clear();
            for(unsigned i = 0; i < numIntermediates; ++i) {
                assert(intermediates[i]);
                _py_intermediates.emplace_back(intermediates[i]);
            }
        }

        double wallTime() const override { return _wallTime; }
        size_t getNumInputRows() const override { return _numInputRowsRead; }

    private:
        int64_t                 _stageID; /// to which stage does this task belong to.
        std::vector<Partition*> _partitions;
        std::vector<Partition*> _exceptionPartitions;
        std::vector<Partition*> _generalPartitions;
        std::vector<Partition*> _fallbackPartitions;

        size_t _exceptionCounter;
        size_t _generalCounter;
        size_t _fallbackCounter;

        inline Schema commonCaseInputSchema() const { return _deserializerGeneralCaseOutput->getSchema(); }
        Schema                  _resolverOutputSchema; //! what the resolve functor produces
        Schema                  _targetOutputSchema; //! which schema the final rows should be in...
        codegen::resolve_f      _functor;            //! holds slow code path with all resolvers inlined.
        PyObject*               _interpreterFunctor;            //! fallback function for interpreter, i.e. in order to process pipeline from start to end...
        bool                    _mergeRows;
        bool                    _allowNumericTypeUnification;
        std::vector<int64_t>    _operatorIDsAffectedByResolvers;

        FileFormat _outputFormat; //! output format of regular rows, required when merging rows in order...
        char _csvDelimiter;
        char _csvQuotechar;

        size_t _numUnresolved;

        int64_t                 _currentRowNumber;
        // std::vector<Partition*> _mergedPartitions;

        int _currentNormalPartitionIdx;
        const uint8_t* _normalPtr;
        size_t _normalPtrBytesRemaining;
        int64_t _normalNumRows;
        int64_t _normalRowNumber;
        int64_t _rowNumber; // merged, running row number
        std::unique_ptr<Deserializer> _deserializerGeneralCaseOutput; // used to infer length of a general case row

        std::unique_ptr<Deserializer> _deserializerNormalOutputCase; //! deserializer object for the target output schema for the normal case

        // output point writer
        // uint8_t* _outPtr;
        // uint8_t* _outStartPtr;

        // sink for merged, resolved rows...
        MemorySink _mergedRowsSink;

        // sink for type violation rows
        MemorySink _generalCaseSink;

        // sink for fallback rows that violate normal and general case
        MemorySink _fallbackSink;

        // hash table sink
        // -> hash to be a hybrid because sometimes incompatible python objects have to be hashed here.
        HashTableSink* _htable;
        HashTableFormat _htableFormat;
        python::Type _hash_element_type;
        python::Type _hash_bucket_type;
        AggregateType _hash_agg_type;

        // hybrid inputs (i.e. when having a long stage the hash-tables of a join)
        std::vector<PyObject*> _py_intermediates;

        CodePathStat _pathStats;

        /*!
       * certain exception codes are internal and require resolution via a pure python pipeline. I.e. malformed input...
       * @param ec
       * @return
       */
        inline bool resolveRequiresInterpreter(const ExceptionCode& ec) {
            // when no functor is specified, resolve
            if(!_functor)
                return true;

            switch(ec) {
                case ExceptionCode::BADPARSE_STRING_INPUT:
                    return true;
                default:
                    return false;
            }
        }

        /*!
         * Serialize and write python row to the fallback row sink
         * @param out_row
         */
        void writePythonObjectToFallbackSink(PyObject* out_row);

        int64_t _outputRowNumber;

        double _wallTime;
        size_t _numInputRowsRead;

        // the different row schemas to use
        inline Schema commonCaseOutputSchema() const {
            return _deserializerGeneralCaseOutput->getSchema();
        }

        inline Schema normalCaseOutputSchema() const {
            assert(_targetOutputSchema.getRowType() == _deserializerNormalOutputCase->getSchema().getRowType());
            return _targetOutputSchema;
        }

        // the input schema of exceptions
        inline Schema exceptionsInputSchema() const {
            return IExceptionableTask::getExceptionSchema();
        }

        void unlockAll() override;

        /*!
         *  write row to merged partitions
         * @param buf
         * @param bufSize
         */
        void writeRow(const uint8_t* buf, size_t bufSize);

        void emitNormalRows();

        size_t readOutputRowSize(const uint8_t* buf, size_t bufSize);

        void sendStatusToHistoryServer();

        /*!
         * execute resolve and merge rows together in order
         */
        void executeInOrder();

        /*!
         * call the different code paths per single row.
         * @param ecCode
         * @param ebuf
         * @param eSize
         */
        void processExceptionRow(int64_t& ecCode, int64_t operatorID, const uint8_t* ebuf, size_t eSize);

        inline uint64_t bytesWritten() const {
            throw std::runtime_error("should not be used!");
            return 0;
        }

        void sinkRowToHashTable(PyObject *rowObject, PyObject* key=nullptr);

        void releaseAllLocks() override;
    };

    /*!
     * create tuple for parse exception (BADPARSESTRINGINPUT)
     * requires GIL to be held
     * @param ebuf buffer holding the exception
     * @param esize size in bytes of the buffer
     * @return pyobject
     */
    extern PyObject* tupleFromParseException(const uint8_t* ebuf, size_t esize);

    extern std::string pythonStringFromParseException(const uint8_t* ebuf, size_t esize);

#ifndef NDEBUG
    // helper functions to quickly build up an exception reservoir
    extern void resetExceptionReservoir(size_t reservoirLimit);
    extern void putExceptionSample(uint32_t ecCode, uint32_t opID, PyObject* input_row, bool acquireGIL);

    // debug helper to display exception input rows.
    // helpful when debugging internal failures...
    extern void displayExceptions(std::ostream& os, bool acquireGIL);
#endif

    inline std::tuple<bool, PyObject*> decodeFallbackRow(const ExceptionCode& ecCode,
                                                         const uint8_t* ebuf,
                                                         size_t eSize,
                                                         const Schema& normal_case_schema,
                                                         const Schema& general_case_schema) {
        assert(ecCode != ExceptionCode::SUCCESS);
        assert(ebuf);

        PyObject* tuple = nullptr;
        bool parse_cells = false;

        // there are different data reps for certain error codes.
        // => decode the correct object from memory & then feed it into the pipeline...
        switch(ecCode) {
            case ExceptionCode::BADPARSE_STRING_INPUT: {
                // it's a string!
                tuple = tupleFromParseException(ebuf, eSize);
                parse_cells = true; // need to parse cells in python mode.
                break;
            }
            case ExceptionCode::NORMALCASEVIOLATION: {
                // changed, why are these names so random here? makes no sense...
                // std::cout<<"except schema: "<<exceptionsInputSchema().getRowType().desc()<<std::endl;
                auto row = Row::fromMemory(normal_case_schema, ebuf, eSize);

                tuple = python::rowToPython(row, true);
                parse_cells = false;
                // called below...
                break;
            }
            case ExceptionCode::GENERALCASEVIOLATION: {
                // changed, why are these names so random here? makes no sense...
                // std::cout<<"except schema: "<<exceptionsInputSchema().getRowType().desc()<<std::endl;
                auto row = Row::fromMemory(general_case_schema, ebuf, eSize);

                tuple = python::rowToPython(row, true);
                parse_cells = false;
                // called below...
                break;
            }
            case ExceptionCode::PYTHON_PARALLELIZE: {
                auto pyObj = python::deserializePickledObject(python::getMainModule(), (char *) ebuf, eSize);
                tuple = pyObj;
                parse_cells = false;
                break;
            }
            default: {
// normal case, i.e. an exception occurred somewhere.
                // --> this means if pipeline is using string as input, we should convert
                auto row = Row::fromMemory(normal_case_schema, ebuf, eSize);

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
                break;
            }
        }

        assert(tuple);

        // note: current python pipeline always expects a tuple arg. hence pack current element.
        if(PyTuple_Check(tuple) && PyTuple_Size(tuple) > 1) {
            // nothing todo...
        } else if(!parse_cells) {
            auto tmp_tuple = PyTuple_New(1);
            PyTuple_SET_ITEM(tmp_tuple, 0, tuple);
            tuple = tmp_tuple;
        }

        return std::make_tuple(parse_cells, tuple);
    }

}

#endif //TUPLEX_RESOLVETASK_H