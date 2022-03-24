//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TRANSFORMTASK_H
#define TUPLEX_TRANSFORMTASK_H

#include "IExceptionableTask.h"
#include "physical/codegen/CodeDefs.h"
#include "physical/execution/FileInputReader.h"
#include <hashmap.h>

namespace tuplex {

    // more complicated is ResolveTask, because takes 4 parameters: source, dest, exceptionSource, exceptionDest.
    struct MemorySink {
        std::vector<Partition*> partitions;
        Partition *currentPartition;
        size_t bytesWritten;
        uint8_t* outputPtr;

        MemorySink() : currentPartition(nullptr), bytesWritten(0), outputPtr(nullptr)   {}

        inline void unlock() {
            if(currentPartition) {
                currentPartition->setBytesWritten(bytesWritten);
                currentPartition->unlockWrite();
            }
            currentPartition = nullptr;
            outputPtr = nullptr;
        }

        inline void reset() {
            partitions.clear();
            currentPartition = nullptr;
            bytesWritten = 0;
            outputPtr = nullptr;
        }
    };

    inline int64_t rowToMemorySink(Executor *owner, MemorySink& sink,
                                   const Schema& outputSchema,
                                   int64_t outputDataSetID, int64_t contextID,
                                   const uint8_t *buf, int64_t size) {
        // @TODO: make sure outputDataSetID works... for now ignored
        assert(outputDataSetID >= 0 && outputSchema != Schema::UNKNOWN);

        assert(owner);

        auto minRequiredSize =  (size_t)size + sizeof(int64_t); // Make sure allocate at least 8 bytes for the row counter
        assert(minRequiredSize > 0);

        // write to partition OR somewhere else...
        // empty partition? ==> allocate!
        if(!sink.currentPartition) {
            assert(owner);
            sink.currentPartition = owner->allocWritablePartition(minRequiredSize, outputSchema, outputDataSetID, contextID);
            sink.bytesWritten = 0;
            sink.outputPtr = sink.currentPartition->lockWriteRaw();
            *((int64_t*)sink.outputPtr) = 0; // 0 rows written yet
            sink.partitions.emplace_back(sink.currentPartition);
        }

        assert(sink.outputPtr);
        // check if enough capacity is left if not allocate new partition
        if(sink.bytesWritten + size > sink.currentPartition->capacity()) {
            sink.currentPartition->unlockWrite();
            sink.currentPartition->setBytesWritten(sink.bytesWritten); // set bytes written to partition
            sink.currentPartition = owner->allocWritablePartition(minRequiredSize, outputSchema, outputDataSetID, contextID);
            sink.bytesWritten = 0;
            sink.outputPtr = sink.currentPartition->lockWriteRaw();
            *((int64_t*)sink.outputPtr) = 0; // 0 rows written yet
            sink.partitions.emplace_back(sink.currentPartition);
        }

        // check that row fits into buffer
        assert(sizeof(int64_t) + sink.bytesWritten + size <= sink.currentPartition->size());

        // copy to output partition & inc row number
        // notice the offset of the numRow field and the bytes written
        memcpy(sink.outputPtr + sizeof(int64_t) + sink.bytesWritten, buf, size);
        sink.bytesWritten += size;
        *((int64_t*)sink.outputPtr) = *((int64_t*)sink.outputPtr) + 1;

        return ecToI32(ExceptionCode::SUCCESS);
    }

    // new, sink everything to hashtable
    enum class HashTableFormat {
        UNKNOWN, //internal do not use
        BYTES, // i.e. for strings, tuples etc. => key is a malloced bytes region!
        UINT64 // i.e. for integers, floats, bools, ... => key is a uint64_t!
    };

    struct HashTableSink { // should be 128bytes wide...
        map_t hm; //! pointer to hashmap, union with other structs...
        uint8_t* null_bucket; //! null bucket to store hashed NULL values.
        PyObject* hybrid_hm; //! optional pointer to hybrid hashmap including hm --> this will be only used in resolve task, because compiled paths have defined schema.

        HashTableSink() : hm(nullptr), null_bucket(nullptr), hybrid_hm(nullptr) {}
    };

    // one Trafo task which can be configured somehow
    class TransformTask : public IExecutorTask {
    public:
        TransformTask() : _outPrefix(1024),
                          _functor(nullptr),
                          _stageID(-1),
                          _htableFormat(HashTableFormat::UNKNOWN),
                          _wallTime(0.0),
                          _updateInputExceptions(false) {
            resetSinks();
            resetSources();
        }

        TransformTask(const TransformTask& other) = delete;

        void sinkExceptionsToMemory(const Schema& inputSchema) { _inputSchema = inputSchema; }
        void sinkExceptionsToFile(const Schema& inputSchema, const URI& exceptionOutputURI) { throw std::runtime_error("not yet supported"); }

        void setFunctor(codegen::read_block_exp_f functor) {
            _functor = (void*)functor;
        }

        void setFunctor(codegen::read_block_f functor) {
            _functor = (void*)functor;
           // @TODO: update other vars too...
        }

        void setFunctor(codegen::cells_row_f functor) {
            _functor = (void*)functor;
            // @TODO: update other vars too...
        }

        void setStageID(int stageID) { _stageID = stageID; };

        void setInputMemorySource(Partition* partition, bool invalidateAfterUse=true);
        void setInputMemorySources(const std::vector<Partition*>& partitions, bool invalidateAfterUse=true);

        /*!
         * add a FileInputReader to the task
         * @param inputFile path to input file
         * @param makeParseExceptionsInternal csv option
         * @param operatorID operator ID
         * @param rowType schema
         * @param header csv option
         * @param cellBasedFunctor code generation
         * @param numColumns number of columns in row
         * @param rangeStart start byte
         * @param rangeSize number of bytes to read
         * @param delimiter csv option
         * @param quotechar csv option
         * @param colsToKeep columns that will be used
         * @param partitionSize size to create partitions
         * @param fmt file format
         */
        void setInputFileSource(const URI& inputFile,
                                bool makeParseExceptionsInternal,
                                int64_t operatorID,
                                const python::Type& rowType,
                                const std::vector<std::string>& header,
                                bool cellBasedFunctor,
                                size_t numColumns,
                                size_t rangeStart,
                                size_t rangeSize,
                                char delimiter,
                                char quotechar,
                                const std::vector<bool>& colsToKeep,
                                size_t partitionSize,
                                FileFormat fmt);

        void sinkOutputToMemory(const Schema& outputSchema, int64_t outputDataSetID, int64_t contextID);
        void sinkOutputToFile(const URI& uri, const std::unordered_map<std::string, std::string>& options);
        void setOutputPrefix(const char* buf, size_t bufSize); // extra prefix to write first to output.

        void sinkOutputToHashTable(HashTableFormat fmt, int64_t outputDataSetID);
        HashTableSink hashTableSink() const { return _htable; } // needs to be freed manually!

        void setOutputLimit(size_t limit) { _outLimit = limit; resetOutputLimitCounter(); }
        void setOutputSkip(size_t numRowsToSkip) { _outSkipRows = numRowsToSkip; }
        void execute() override;

        bool hasFileSink() const { return _outputFilePath != URI::INVALID; }
        bool hasFileSource() const { return _inputFilePath != URI::INVALID; }
        bool hasMemorySink() const { return _outputSchema != Schema::UNKNOWN; }
        bool hasMemorySource() const { return !_inputPartitions.empty(); }
        bool hasHashTableSink() const { return _htableFormat != HashTableFormat::UNKNOWN; }
        HashTableFormat hashTableFormat() const { return _htableFormat; }

        /*!
         * get the function which to use for writing output rows.
         * @param globalOutputLimit if true, then task uses global aggregate limit
         * @param fileOutput whether to use fileoutput or not
         * @return writerow function address
         */
        static codegen::write_row_f writeRowCallback(bool globalOutputLimit, bool fileOutput=false);
        static codegen::exception_handler_f exceptionCallback(bool fileOutput=false);
        static codegen::str_hash_row_f writeStringHashTableCallback();
        static codegen::i64_hash_row_f writeInt64HashTableCallback();
        static codegen::str_hash_row_f writeStringHashTableAggregateCallback();
        static codegen::i64_hash_row_f writeInt64HashTableAggregateCallback();
        static codegen::write_row_f aggCombineCallback();

        static void resetOutputLimitCounter();

        // most be public because of C++ issues -.-
        int64_t writeRowToMemory(uint8_t* buf, int64_t bufSize);
        int64_t writeRowToFile(uint8_t* buf, int64_t bufSize);
        void writeExceptionToMemory(const int64_t ecCode, const int64_t opID, const int64_t row, const uint8_t *buf, const size_t bufSize);
        void writeExceptionToFile(const int64_t ecCode, const int64_t opID, const int64_t row, const uint8_t *buf, const size_t bufSize);
        void writeRowToHashTable(char *key, size_t key_len, bool bucketize, char *buf, size_t buf_size);
        void writeRowToHashTable(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size);
        void writeRowToHashTableAggregate(char *key, size_t key_len, bool bucketize, char *buf, size_t buf_size);
        void writeRowToHashTableAggregate(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size);

        // prob remove getOutputPartitions from task...
        std::vector<Partition*> getOutputPartitions() const override { return _output.partitions; }
        std::vector<Partition*> getExceptionPartitions() const { return _exceptions.partitions; }

        size_t getNumExceptions() const;

        TaskType type() const override { return TaskType::UDFTRAFOTASK; }

        int64_t getStageID() const { return _stageID; }

        Schema inputSchema() const { return _inputSchema; }

        size_t getNumOutputRows() const override;

        /*!
        * returns the number of exceptions in this task, hashed after operatorID and exception code
        * @return
        */
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> exceptionCounts() const { return _exceptionCounts; }

        ExceptionInfo inputExceptionInfo() { return _inputExceptionInfo; }
        std::vector<Partition*> inputExceptions() { return _inputExceptions; }

        void setInputExceptionInfo(ExceptionInfo info) { _inputExceptionInfo = info; }
        void setInputExceptions(const std::vector<Partition*>& inputExceptions) { _inputExceptions = inputExceptions; }
        void setUpdateInputExceptions(bool updateInputExceptions) { _updateInputExceptions = updateInputExceptions; }

        double wallTime() const override { return _wallTime; }

        size_t output_rows_written() const { return _numOutputRowsWritten; }
        size_t output_limit() const { return _outLimit; }
    private:
        void resetSinks();
        void resetSources();

        int _stageID; // id to identify the stage this tasks belongs to.

        bool _invalidateSourceAfterUse;

        bool hasFilePrefix() const { return _outPrefix.size() != 0; }

        void* _functor; // compiled function
        //codegen::agg_combine_f  _combiner; // thread-local combiner to be executed, optional.

        size_t _numInputRowsRead;
        size_t _numOutputRowsWritten;

        // file source variables
        URI _inputFilePath;
        std::unique_ptr<FileInputReader> _reader;

        // file sink variables
        URI _outputFilePath;
        std::unique_ptr<VirtualFile> _outFile;
        Buffer _outPrefix;
        std::unordered_map<std::string, std::string> _outOptions;

        size_t _outLimit; // limits how many rows to write at max
        size_t _outSkipRows; // how many rows at start to skip

        // memory source variables
        std::vector<Partition*> _inputPartitions;

        // memory sink variables
        Schema _outputSchema; //! schema of the final output rows.
        int64_t _outputDataSetID; //! datasetID of the final operator.
        int64_t _contextID; //! contextID where output partitions belong to
        MemorySink _output;

        // exception partitions (sunk to memory)
        MemorySink _exceptions;
        Schema _inputSchema;

        ExceptionInfo _inputExceptionInfo;
        std::vector<Partition*> _inputExceptions;
        bool _updateInputExceptions;

        // hash table sink
        HashTableSink _htable;
        HashTableFormat _htableFormat;

        // NEW: row counter here for correct exception handling...
        int64_t _outputRowCounter;

        double _wallTime;

        inline int64_t contextID() const { return _contextID; }

        inline void unlockAllMemorySinks() {  // output partition existing? if so unlock
           _output.unlock();
           _exceptions.unlock();
        }

        void processMemorySourceWithExp();
        void processMemorySource();
        void processFileSource();

        // exceptions
        // exception counts (required for sampling etc. later)
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _exceptionCounts;

        void incExceptionCounts(int64_t ecCode, int64_t opID);


        void sendStatusToHistoryServer();

        size_t getNumInputRows() const override {
            return _numInputRowsRead;
        }


    };

    // thread-local helper funcs for aggregates!
    extern bool initThreadLocalAggregates(size_t num_slots, codegen::agg_init_f init_func, codegen::agg_combine_f combine_func);
    extern bool initThreadLocalAggregateByKey(codegen::agg_init_f init_func, codegen::agg_combine_f combine_func, codegen::agg_agg_f aggregate_func);
    extern bool fetchAggregate(uint8_t** out, int64_t* out_size);

    extern uint8_t* combineBuckets(uint8_t *bucketA, uint8_t* bucketB);
    extern void aggregateValues(uint8_t** bucket, char *buf, size_t buf_size);
}

#endif //TUPLEX_TRANSFORMTASK_H