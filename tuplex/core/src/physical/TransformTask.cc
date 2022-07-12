//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/TransformTask.h>
#include <RuntimeInterface.h>
#include <CSVUtils.h>
#include <physical/JITCompiledCSVReader.h>
#include <physical/CSVReader.h>
#include <physical/TextReader.h>
#include <physical/OrcReader.h>
#include <bucket.h>

namespace tuplex {
    // this is a logic to stop the execution once it has reached the topLimit and bottomLimit
    // here, we assume that task order starts with zero and count up by 1, e.g. 0, 1, 2, ..., n
    // To implement limit, we maintain a mapping from the task order to the number of rows done in that task
    // (rows done are either 0 or #output rows after processing)
    // we can then find out how many top rows are done by looking at g_rowsDone[0], g_rowsDone[1], ...
    // until we reach some segment that's 0
    // likewise, we can find the bottom rows done by looking at g_rowsDone[g_maxOrder], g_rowsDone[g_maxOrder - 1], ...

    // mapping from order number -> row count if the task is finished
    static std::mutex g_rowsDoneMutex;
    static std::unordered_map<size_t, size_t> g_rowsDone;
    static std::atomic_size_t g_maxOrder;

    void TransformTask::setMaxOrderAndResetLimits(size_t maxOrder) {
        g_rowsDone.clear();
        g_maxOrder = maxOrder;
    }
}

extern "C" {
    static int64_t w2mCallback(tuplex::TransformTask* task, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        return task->writeRowToMemory(buf, bufSize);
    }

    static int64_t w2fCallback(tuplex::TransformTask* task, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        return task->writeRowToFile(buf, bufSize);
    }

    static int64_t limited_w2mCallback(tuplex::TransformTask* task, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        return task->writeRowToMemory(buf, bufSize);
    }

    static int64_t limited_w2fCallback(tuplex::TransformTask* task, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        return task->writeRowToFile(buf, bufSize);
    }

    static void e2mCallback(tuplex::TransformTask *task, int64_t ecCode, int64_t opID, int64_t row, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        task->writeExceptionToMemory(ecCode, opID, row, buf, bufSize);
    }

    static void e2fCallback(tuplex::TransformTask *task, int64_t ecCode, int64_t opID, int64_t row, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        task->writeExceptionToFile(ecCode, opID, row, buf, bufSize);
    }

    static void strw2hCallback(tuplex::TransformTask *task, char *strkey, size_t key_size, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        assert(dynamic_cast<tuplex::TransformTask*>(task)->hashTableFormat() == tuplex::HashTableFormat::BYTES);
        task->writeRowToHashTable(strkey, key_size, bucketize, buf, buf_size);
    }

    static void i64w2hCallback(tuplex::TransformTask *task, int64_t intkey, bool intkeynull, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        assert(dynamic_cast<tuplex::TransformTask*>(task)->hashTableFormat() == tuplex::HashTableFormat::UINT64);
        auto key = static_cast<uint64_t>(intkey);
        // TODO: do we need to do something more sophisticated here? e.g. with a union
        // key = ((union { int64_t i; uint64_t u; }){ .i = intkey }).u;
        task->writeRowToHashTable(key, intkeynull, bucketize, buf, buf_size);
    }

    static void strw2hAggCallback(tuplex::TransformTask *task, char *strkey, size_t key_size, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        assert(dynamic_cast<tuplex::TransformTask*>(task)->hashTableFormat() == tuplex::HashTableFormat::BYTES);
        task->writeRowToHashTableAggregate(strkey, key_size, bucketize, buf, buf_size);
    }

    static void i64w2hAggCallback(tuplex::TransformTask *task, int64_t intkey, bool intkeynull, bool bucketize, char *buf, size_t buf_size) {
        assert(task);
        assert(dynamic_cast<tuplex::TransformTask*>(task));
        assert(dynamic_cast<tuplex::TransformTask*>(task)->hashTableFormat() == tuplex::HashTableFormat::UINT64);
        auto key = static_cast<uint64_t>(intkey);
        // TODO: do we need to do something more sophisticated here? e.g. with a union
        // key = ((union { int64_t i; uint64_t u; }){ .i = intkey }).u;
        task->writeRowToHashTableAggregate(key, intkeynull, bucketize, buf, buf_size);
    }
}

namespace tuplex {

    // hashmap helper
    // call this func in iterate
    static int hash_count_helper(std::tuple<size_t, size_t>* counters, hashmap_element* entry) {
        auto data = (uint8_t*)entry->data; // bucket data. First is always an int64_t holding how many rows there are.

        if(entry->in_use)
            std::get<1>(*counters)++;

        if(data) {
            // other bucket count
            std::get<0>(*counters) += (*(uint64_t*)data >> 32ul);
        }

        return MAP_OK;
    }

    static void hashmap_info(map_t hm, size_t* out_row_count, size_t* out_bucket_count) {
        std::tuple<size_t, size_t> counters = std::make_tuple(0, 0);
        hashmap_iterate(hm, reinterpret_cast<PFany>(hash_count_helper), (void*)&counters);

        // output
        if(out_row_count)
            *out_row_count = std::get<0>(counters);
        if(out_bucket_count)
            *out_bucket_count = std::get<1>(counters);
    }

    static int int64_hash_count_helper(std::tuple<size_t, size_t>* counters, int64_hashmap_element* entry) {
        auto data = (uint8_t*)entry->data; // bucket data. First is always an int64_t holding how many rows there are.

        if(entry->in_use)
            std::get<1>(*counters)++;

        if(data) {
            // other bucket count
            std::get<0>(*counters) += (*(uint64_t*)data >> 32ul);
        }

        return MAP_OK;
    }

    static void int64_hashmap_info(map_t hm, size_t* out_row_count, size_t* out_bucket_count) {
        std::tuple<size_t, size_t> counters = std::make_tuple(0, 0);
        int64_hashmap_iterate(hm, reinterpret_cast<PFintany>(int64_hash_count_helper), (void *) &counters);

        // output
        if(out_row_count)
            *out_row_count = std::get<0>(counters);
        if(out_bucket_count)
            *out_bucket_count = std::get<1>(counters);
    }



    // callbacks
    codegen::write_row_f TransformTask::writeRowCallback(bool globalOutputLimit, bool fileOutput) {
        if(globalOutputLimit) {
            if(fileOutput)
                return reinterpret_cast<codegen::write_row_f>(limited_w2fCallback);
            else
                return reinterpret_cast<codegen::write_row_f>(limited_w2mCallback);
        } else {
            if(fileOutput)
                return reinterpret_cast<codegen::write_row_f>(w2fCallback);
            else
                return reinterpret_cast<codegen::write_row_f>(w2mCallback);
        }
    }
    codegen::exception_handler_f TransformTask::exceptionCallback(bool fileOutput) {
        if(fileOutput)
            return reinterpret_cast<codegen::exception_handler_f>(e2fCallback);
        else
            return reinterpret_cast<codegen::exception_handler_f>(e2mCallback);
    }

    codegen::str_hash_row_f TransformTask::writeStringHashTableCallback() {
        return reinterpret_cast<codegen::str_hash_row_f>(strw2hCallback);
    }

    codegen::i64_hash_row_f TransformTask::writeInt64HashTableCallback() {
        return reinterpret_cast<codegen::i64_hash_row_f>(i64w2hCallback);
    }

    codegen::str_hash_row_f TransformTask::writeStringHashTableAggregateCallback() {
        return reinterpret_cast<codegen::str_hash_row_f>(strw2hAggCallback);
    }

    codegen::i64_hash_row_f TransformTask::writeInt64HashTableAggregateCallback() {
        return reinterpret_cast<codegen::i64_hash_row_f>(i64w2hAggCallback);
    }


    // aggregate: We do this here smarter using thread_locals!
    // @TODO: avoid thread_local, use array and give each task, the executor ID/thread number!
    static uint8_t** tl_aggregate = nullptr; // <-- lazy init within task!
    static int64_t* tl_aggregate_size = nullptr; // <-- lazy init as well!
    static size_t tl_num_slots = 0;
    static codegen::agg_combine_f agg_combine_functor = nullptr; // <-- check
    static codegen::agg_init_f agg_init_functor = nullptr;
    static codegen::agg_agg_f agg_aggregate_functor = nullptr;

    // do not go separate way, simply add to thread-local aggregate!
    extern "C" int64_t combineAggregate(TransformTask* task, uint8_t* buf, int64_t buf_size) {
        assert(agg_combine_functor);
        assert(task);
#ifndef NDEBUG
        assert(buf);
        assert(tl_aggregate); // if this fails, init was not performed...
#endif

        auto threadNum = task->threadNumber();
        assert(threadNum < tl_num_slots);
        agg_combine_functor(&tl_aggregate[threadNum], &tl_aggregate_size[threadNum], buf, buf_size);
        return 0;
    }

    static void freeAggregates() {
        if (tl_aggregate) {
            for(unsigned i = 0; i < tl_num_slots; ++i)
                free(tl_aggregate[i]); // these should be C-malloced!
            delete [] tl_aggregate;
        }

        if (tl_aggregate_size) {
            delete [] tl_aggregate_size;
        }

        tl_aggregate = nullptr;
        tl_aggregate_size = nullptr;
        tl_num_slots = 0;
        agg_combine_functor = nullptr;
        agg_init_functor = nullptr;
        agg_aggregate_functor = nullptr;
    }

    bool initThreadLocalAggregates(size_t num_slots, codegen::agg_init_f init_func, codegen::agg_combine_f combine_func) {
        assert(num_slots > 0 && num_slots < 65536); // make sure not too many slots.

        freeAggregates();

        agg_combine_functor = combine_func;

        tl_num_slots = num_slots;
        tl_aggregate = new uint8_t*[num_slots];
        tl_aggregate_size = new int64_t[num_slots];

        for(unsigned i = 0; i < num_slots; ++i) {
            init_func(&tl_aggregate[i], &tl_aggregate_size[i]);
        }

        return true;
    }

    bool initThreadLocalAggregateByKey(codegen::agg_init_f init_func, codegen::agg_combine_f combine_func, codegen::agg_agg_f aggregate_func) {
        freeAggregates();

        agg_combine_functor = combine_func;
        agg_init_functor = init_func;
        agg_aggregate_functor = aggregate_func;
        return true;
    }

    bool fetchAggregate(uint8_t** out, int64_t* out_size) {
        if(!tl_aggregate || !tl_aggregate_size)
            return false;

        assert(tl_num_slots > 0);

        // combine all the thread-local aggregates
        uint8_t* agg = tl_aggregate[0];
        int64_t agg_size = tl_aggregate_size[0];
        for(unsigned i = 1; i < tl_num_slots; ++i) {
            agg_combine_functor(&agg, &agg_size, tl_aggregate[i], tl_aggregate_size[i]);
        }

        // copy buffer
        *out = static_cast<uint8_t *>(malloc(agg_size));
        memcpy(*out, agg, agg_size);
        *out_size = agg_size;

        freeAggregates();

        return true;
    }

    uint8_t* combineBuckets(uint8_t* bucketA, uint8_t* bucketB) {
        // if one is null, just return the other
        if (!bucketA && !bucketB)
            return nullptr;
        if (bucketA && !bucketB)
            return bucketA;
        if (!bucketA && bucketB)
            return bucketB;

        // both are valid
        assert(bucketA && bucketB);
        assert(bucketA != bucketB);

        auto sizeA = *(int64_t*)bucketA;
        auto valA = static_cast<uint8_t*>(malloc(sizeA));
        // TODO: when we convert everything to thread locals, we should change agg_combine_functor to match the size | value format of agg_aggregate_functor so that we can roll aggregate into aggregateByKey and just using the nullbucket
        memcpy(valA, bucketA + 8, sizeA);

        auto sizeB = *(uint64_t*)bucketB;
        auto valB = bucketB + 8;

        agg_combine_functor(&valA, &sizeA, valB, sizeB);

        // allocate the output buffer (should be avoided by the above TODO eventually)
        auto ret = static_cast<uint8_t*>(malloc(sizeA + 8));
        *(int64_t*)ret = sizeA;
        memcpy(ret + 8, valA, sizeA);
        free(valA); free(bucketA);
        return ret;
    }

    void aggregateValues(uint8_t** bucket, char *buf, size_t buf_size) {
        // if this is the first one, we need to initialize
        if(*bucket == nullptr) {
            // initialize
            uint8_t* init_val = nullptr;
            int64_t init_size = 0;
            agg_init_functor(&init_val, &init_size);
            // allocate the bucket
            auto *new_bucket = static_cast<uint8_t *>(malloc(init_size + 8));
            *(int64_t*)new_bucket = init_size;
            memcpy(new_bucket+8, init_val, init_size);
            // set the bucket value
            *bucket = new_bucket;
        }

        // aggregate the value -> knows size | buffer construct
        agg_aggregate_functor(bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
    }


    codegen::write_row_f TransformTask::aggCombineCallback() {
        return reinterpret_cast<codegen::write_row_f>(combineAggregate);
    }

    void TransformTask::execute() {
        Timer timer;

        // check that there is one source and one sink, else skip
        if(!hasFileSink() && !hasMemorySink() && !hasHashTableSink())
            throw std::runtime_error("task has no sink");

        if(!hasMemorySource() && !hasFileSource())
            throw std::runtime_error("task has no source");

        // if file sink assigned, open file & print header
        if(hasFileSink()) {

            if(_outputFilePath == URI::INVALID)
                throw std::runtime_error("invalid URI to writeToFile Task given");

            _outFile = VirtualFileSystem::open_file(_outputFilePath, VirtualFileMode::VFS_WRITE);
            if(!_outFile)
                throw std::runtime_error("could not open " + _outputFilePath.toPath() + " in write mode.");

            // write header if desired...
            bool writeHeader = stringToBool(get_or(_outOptions, "header", "false"));
            if(writeHeader) {
                // fetch special var csvHeader
                auto headerLine = _outOptions["csvHeader"];
                _outFile->write(headerLine.c_str(), headerLine.length());
            }

            if(hasFilePrefix() && _outPrefix.size()) {
                _outFile->write(_outPrefix.buffer(), _outPrefix.size());
            }
        }

        // alloc hashmap if required
        if(hasHashTableSink()) {
            _htable.hm = hashmap_new();
            _htable.null_bucket = nullptr;
        }

        // free runtime memory
        runtime::rtfree_all();

        // check functor is valid
        if(!_functor)
            throw std::runtime_error("compiled functor not set, task failed.");

        // check what type of source exists
        if(hasFileSource()) {
            processFileSource();
        } else if(hasMemorySource()) {
            if(_inputPartitions.empty())
                throw std::runtime_error("no input partition assigned!");
            if (_updateInputExceptions)
                processMemorySourceWithExp();
            else
                processMemorySource();
        } else {
            throw std::runtime_error("no source (file/memory) specified, error!");
        }

        // @TODO: use setjmp buffer etc. here to stop execution... ==> each thread needs one context -.- -> might be difficult...
        // alternative is to generate early leave in code-generated function...


        // free runtime memory
        runtime::rtfree_all();

        // close file
        if(hasFileSink())
            _outFile->close();


        // // task was successful if bytes were written
        // // negative numbers for failure (i.e. -1 = TASK_FAILURE)
        // // However there is a special case: The whole task may lead to exceptions. This is not a task failure,
        // // but UDF will also return 0. Address here.
        // _success = _totalBytesWritten >= 0 || numExceptionRows() == numInputRows;
        // if(_success) {
        //     owner()->info("[Task Finished] Map on " + uuidToString(_input->uuid()) + " ["
        //                            + pluralize(numInputRows, "row") + ", "
        //                            + pluralize(numExceptionRows(), "exception") + "] in "
        //                            + std::to_string(timer.time()) + "s");
        // } else {
        //     owner()->warn("[TASK FAILED] Map on " + uuidToString(_input->uuid()));
        // }

        std::string mode;
        if(hasMemorySink())
            mode = "to mem ";
        if(hasHashTableSink())
            mode = "to in-memory hash table ";
        if(hasFileSink())
            mode = "to file ";

        // save time
        _wallTime = timer.time();


        std::stringstream ss;
        ss<<"[Task Finished] Transform "<<mode<<"in "
          <<std::to_string(wallTime())<<"s (";

        if(hasHashTableSink()) {
            size_t numBuckets = 0;
            size_t numOutputRows = 0;

            // count from hashmap
            if(_htableFormat == HashTableFormat::BYTES) hashmap_info(_htable.hm, &numOutputRows, &numBuckets);
            else if(_htableFormat == HashTableFormat::UINT64)
                int64_hashmap_info(_htable.hm, &numOutputRows, &numBuckets);
            else throw std::runtime_error("Unknown hashtable format!");
            // add +1 if null bucket exists!
            if(_htable.null_bucket)
                numBuckets++;

            // count rows from input & output buckets
            ss<<pluralize(numOutputRows, "normal row")<<", "
              <<pluralize(getNumExceptions(), "exception")<<", "
              <<pluralize(numBuckets, "bucket")<<")";
        } else
        ss<<pluralize(getNumOutputRows(), "normal row")<<", "<<pluralize(getNumExceptions(), "exception")<<")";
        owner()->info(ss.str());

#warning "check these numbers, test? is that correct??"


        // before sending status, UNLOCK ALL PARTITIONS! especially the exceptional ones...
        // send updates to history server
        sendStatusToHistoryServer();
    }

    size_t TransformTask::getNumExceptions() const {
        size_t exception_count = 0;
        for(auto p : getExceptionPartitions()) {
            exception_count += p->getNumRows();
        }
        return exception_count;
    }

    void TransformTask::resetSources() {
        _invalidateSourceAfterUse = false; // per default, no invalidation happens.

        _numInputRowsRead = 0;

        // reset file sources
        _inputFilePath = URI::INVALID;


        // reset memory sources
        _inputPartitions.clear();
    }

    void TransformTask::resetSinks() {

        _numOutputRowsWritten = 0;

        // reset file sink
        _outputFilePath = URI::INVALID;
        _outFile.reset(nullptr);
        _outPrefix.reset();
        _outTopLimit = std::numeric_limits<size_t>::max(); // write all rows
        _outBottomLimit = 0;

        // reset memory sink
        _output.reset();
        _outputSchema = Schema::UNKNOWN;
        _outputDataSetID =  -1;
        _contextID = -1;

        // reset exception memory sink
        _exceptions.reset();

        // reset htable (TODO: free if necessary?)
        _htable = HashTableSink();

        // reset output row counter...
        _outputRowCounter = 0;
    }

    void TransformTask::processMemorySourceWithExp() {
        assert(!_inputPartitions.empty());
        assert(_functor);

#warning "there's a temp fix here, better to write partition/thread system and transfer ownership of partitions to write too upfront."

        _numInputRowsRead = 0;
        _numOutputRowsWritten = 0;

        int64_t  num_normal_rows = 0, num_bad_rows = 0;

        auto functor = reinterpret_cast<codegen::read_block_exp_f>(_functor);

        int64_t totalNormalRowCounter = 0;
        int64_t totalGeneralRowCounter = 0;
        int64_t totalFallbackRowCounter = 0;
        int64_t totalFilterCounter = 0;

        std::vector<uint8_t*> generalPartitions(_generalPartitions.size(), nullptr);
        for (int i = 0; i < _generalPartitions.size(); ++i)
            generalPartitions[i] = _generalPartitions[i]->lockWriteRaw(true);
        int64_t numGeneralPartitions = _generalPartitions.size();
        int64_t generalIndexOffset = 0;
        int64_t generalRowOffset = 0;
        int64_t generalByteOffset = 0;

        std::vector<uint8_t*> fallbackPartitions(_fallbackPartitions.size(), nullptr);
        for (int i = 0; i < _fallbackPartitions.size(); ++i)
            fallbackPartitions[i] = _fallbackPartitions[i]->lockWriteRaw(true);
        int64_t numFallbackPartitions = _fallbackPartitions.size();
        int64_t fallbackIndexOffset = 0;
        int64_t fallbackRowOffset = 0;
        int64_t fallbackByteOffset = 0;

        // go over all input partitions.
        for(auto &inputPartition : _inputPartitions) {
            // lock ptr, extract number of rows ==> store them
            // lock raw & call functor!
            int64_t inSize = inputPartition->size();
            const uint8_t *inPtr = inputPartition->lockRaw();
            _numInputRowsRead += static_cast<size_t>(*((int64_t*)inPtr));

            // call functor
            auto bytesParsed = functor(this, inPtr, inSize, &num_normal_rows, &num_bad_rows, false,
                                       &totalFilterCounter, &totalNormalRowCounter, &totalGeneralRowCounter, &totalFallbackRowCounter,
                                       &generalPartitions[0], numGeneralPartitions, &generalIndexOffset, &generalRowOffset, &generalByteOffset,
                                       &fallbackPartitions[0], numFallbackPartitions, &fallbackIndexOffset, &fallbackRowOffset, &fallbackByteOffset);

            // save number of normal rows to output rows written if not writeTofile
            if(hasMemorySink())
                _numOutputRowsWritten += num_normal_rows;

            // unlock memory sinks if necessary
            unlockAllMemorySinks();

            inputPartition->unlock();

            // delete partition if desired...
            if(_invalidateSourceAfterUse)
                inputPartition->invalidate();
        }

        if (generalIndexOffset < numGeneralPartitions) {
            auto curGeneralPtr = generalPartitions[generalIndexOffset];
            auto numRowsInPartition = *((int64_t*)curGeneralPtr);
            curGeneralPtr += sizeof(int64_t) + generalByteOffset;
            while (generalRowOffset < numRowsInPartition) {
                *((int64_t*)curGeneralPtr) -= totalFilterCounter;
                curGeneralPtr += 4 * sizeof(int64_t) + ((int64_t*)curGeneralPtr)[3];
                generalRowOffset += 1;

                if (generalRowOffset == numRowsInPartition && generalIndexOffset < numGeneralPartitions - 1) {
                    generalIndexOffset += 1;
                    curGeneralPtr = generalPartitions[generalIndexOffset];
                    numRowsInPartition = *((int64_t*)curGeneralPtr);
                    curGeneralPtr += sizeof(int64_t);
                    generalByteOffset = 0;
                    generalRowOffset = 0;
                }
            }
        }

        if (fallbackIndexOffset < numFallbackPartitions) {
            auto curFallbackPtr = fallbackPartitions[fallbackIndexOffset];
            auto numRowsInPartition = *((int64_t*)curFallbackPtr);
            curFallbackPtr += sizeof(int64_t) + fallbackByteOffset;
            while (fallbackRowOffset < numRowsInPartition) {
                *((int64_t*)curFallbackPtr) -= totalFilterCounter;
                curFallbackPtr += 4 * sizeof(int64_t) + ((int64_t*)curFallbackPtr)[3];
                fallbackRowOffset += 1;

                if (fallbackRowOffset == numRowsInPartition && fallbackIndexOffset < numFallbackPartitions - 1) {
                    fallbackIndexOffset += 1;
                    curFallbackPtr = fallbackPartitions[fallbackIndexOffset];
                    numRowsInPartition = *((int64_t*)curFallbackPtr);
                    curFallbackPtr += sizeof(int64_t);
                    fallbackByteOffset = 0;
                    fallbackRowOffset = 0;
                }
            }
        }

        for (auto &p : _generalPartitions)
            p->unlockWrite();

        for (auto &p : _fallbackPartitions)
            p->unlockWrite();

#ifndef NDEBUG
        owner()->info("Trafo task memory source exhausted (" + pluralize(_inputPartitions.size(), "partition") + ", "
                      + pluralize(num_normal_rows, "normal row") + ", " + pluralize(num_bad_rows, "exceptional row") + ")");
#endif
    }

    bool TransformTask::limitReached() const {
        size_t numTopCompleted = 0;
        size_t numBottomCompleted = 0;
        bool isTopLimitReached = false;
        bool isBottomLimitReached = false;

        tuplex::g_rowsDoneMutex.lock();
        if (_outTopLimit == 0) {
            isTopLimitReached = true;
        } else {
            for (size_t i = 0; tuplex::g_rowsDone.count(i) != 0; i++) {
                numTopCompleted += tuplex::g_rowsDone[i];
                if (numTopCompleted >= _outTopLimit) {
                    isTopLimitReached = true;
                    break;
                }
            }
        }

        if (_outBottomLimit == 0) {
            isBottomLimitReached = true;
        } else {
            for (size_t i = tuplex::g_maxOrder; tuplex::g_rowsDone.count(i) != 0; i--) {
                numBottomCompleted += tuplex::g_rowsDone[i];
                if (numBottomCompleted >= _outBottomLimit) {
                    isBottomLimitReached = true;
                    break;
                }
            }
        }
        tuplex::g_rowsDoneMutex.unlock();

        return isTopLimitReached && isBottomLimitReached;
    }

    void TransformTask::updateLimits() {
        tuplex::g_rowsDoneMutex.lock();
        tuplex::g_rowsDone[getOrder(0)] += getNumOutputRows();
        tuplex::g_rowsDoneMutex.unlock();
    }

    void TransformTask::processMemorySource() {
        assert(!_inputPartitions.empty());
        assert(_functor);

        _numInputRowsRead = 0;
        _numOutputRowsWritten = 0;

        int64_t  num_normal_rows = 0, num_bad_rows = 0;

        auto functor = reinterpret_cast<codegen::read_block_f>(_functor);

        // go over all input partitions.
        for(const auto &inputPartition : _inputPartitions) {
            if (limitReached()) {
                // skip the execution, enough is done
                break;
            }

            // lock ptr, extract number of rows ==> store them
            // lock raw & call functor!
            int64_t inSize = inputPartition->size();
            const uint8_t *inPtr = inputPartition->lockRaw();
           _numInputRowsRead += static_cast<size_t>(*((int64_t*)inPtr));

            // call functor
            auto bytesParsed = functor(this, inPtr, inSize, &num_normal_rows, &num_bad_rows, false);

            // save number of normal rows to output rows written if not writeTofile
            if(hasMemorySink())
                _numOutputRowsWritten += num_normal_rows;

            // unlock memory sinks if necessary
            unlockAllMemorySinks();

            inputPartition->unlock();

            // delete partition if desired...
            if(_invalidateSourceAfterUse)
                inputPartition->invalidate();

            updateLimits();
        }

#ifndef NDEBUG
        owner()->info("Trafo task memory source exhausted (" + pluralize(_inputPartitions.size(), "partition") + ", "
        + pluralize(num_normal_rows, "normal row") + ", " + pluralize(num_bad_rows, "exceptional row") + ")");
#endif
    }

    void TransformTask::processFileSource() {
        assert(_inputFilePath != URI::INVALID);
        assert(_functor);

        assert(_reader);

        _reader->read(_inputFilePath);

        _numInputRowsRead = _reader->inputRowCount();
        // get output from _reader ~~> i.e. any IO exceptions or so...

        // unlock memory sinks if necessary
        unlockAllMemorySinks();
    }

    int64_t TransformTask::writeRowToFile(uint8_t *buf, int64_t bufSize) {


        // @TODO: How to improve this
        // => for faster IO, probably better to write everything to a temp buffer and then to file
        // => could be done using partitions or simply alloc manager...
        // i.e. less sys calls...

        // @TODO: count here lines written + limit them if necessary!!!
        assert(_outFile);

        // skip rows? limit rows??

        if(_numOutputRowsWritten < _outTopLimit) {
            if(_outFile->write(buf, bufSize) != VirtualFileSystemStatus::VFS_OK)
                return ecToI32(ExceptionCode::IOERROR);
        }

        _numOutputRowsWritten++;
        _outputRowCounter++; // TODO: unify with numOutputRowsWritten??

        return ecToI32(ExceptionCode::SUCCESS);
    }

    int64_t TransformTask::writeRowToMemory(uint8_t *buf, int64_t size) {
        _outputRowCounter++;
        return rowToMemorySink(owner(), _output, _outputSchema, _outputDataSetID, contextID(), buf, size);
    }

    // note: could also use a int64_t, int64_t hashmap for string when string key is stored in bucket...
    void TransformTask::writeRowToHashTable(char* key, size_t key_len, bool bucketize, char *buf, size_t buf_size) {
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable.hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // @TODO: is there a memory bug here when it comes to storing the key???
        // put into hashmap or null bucket
        if(key != nullptr && key_len > 0) {
            // put into hashmap!
            uint8_t *bucket = nullptr;
            if(bucketize) { //@TODO: maybe get rid off this if by specializing pipeline better for unique case...
                hashmap_get(_htable.hm, key, key_len, (void **) (&bucket));
                // update or new entry
                bucket = extend_bucket(bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
            }
            hashmap_put(_htable.hm, key, key_len, bucket);
        } else {
            // goes into null bucket, no hash
            _htable.null_bucket = extend_bucket(_htable.null_bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
        }
    }

    void TransformTask::writeRowToHashTableAggregate(char* key, size_t key_len, bool bucketize, char *buf, size_t buf_size) {
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable.hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // @TODO: is there a memory bug here when it comes to storing the key???
        // get the bucket
        uint8_t *bucket = nullptr;
        if(key != nullptr && key_len > 0) {
            // get current bucket
            hashmap_get(_htable.hm, key, key_len, (void **) (&bucket));
        } else {
            // goes into null bucket, no hash
            bucket = _htable.null_bucket;
        }

        // aggregate in the new value
        aggregateValues(&bucket, buf, buf_size);

        // write back the bucket
        if(key != nullptr && key_len > 0) {
            hashmap_put(_htable.hm, key, key_len, bucket);
        } else {
            _htable.null_bucket = bucket;
        }
    }

    void TransformTask::writeRowToHashTable(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size) {
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable.hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // put into hashmap or null bucket
        if(!key_null) {
            // put into hashmap!
            uint8_t *bucket = nullptr;
            if(bucketize) { //@TODO: maybe get rid off this if by specializing pipeline better for unique case...
                int64_hashmap_get(_htable.hm, key, (void **) (&bucket));
                // update or new entry
                bucket = extend_bucket(bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
            }
            int64_hashmap_put(_htable.hm, key, bucket);
        } else {
            // goes into null bucket, no hash
            _htable.null_bucket = extend_bucket(_htable.null_bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
        }
    }

    void TransformTask::writeRowToHashTableAggregate(uint64_t key, bool key_null, bool bucketize, char *buf, size_t buf_size) {
        // saves key + rest in buckets (incl. null bucket)
        assert(_htable.hm);
        assert(_htableFormat != HashTableFormat::UNKNOWN);

        // get the bucket
        uint8_t *bucket = nullptr;
        if(!key_null) {
            // get current bucket
            int64_hashmap_get(_htable.hm, key, (void **) (&bucket));
        } else {
            bucket = _htable.null_bucket;
        }
        // aggregate in the new value
        aggregateValues(&bucket, buf, buf_size);
        if(!key_null) {
            // get current bucket
            int64_hashmap_put(_htable.hm, key, bucket);
        } else {
            // goes into null bucket, no hash
            _htable.null_bucket = bucket;
        }
    }

    void TransformTask::writeExceptionToFile(const int64_t ecCode, const int64_t opID, const int64_t row,
                                             const uint8_t *buf, const size_t bufSize) {
        std::cout<<"exception row written to file, not yet supported!"<<std::endl;
        incExceptionCounts(ecCode, opID);
    }

    void TransformTask::writeExceptionToMemory(const int64_t ecCode, const int64_t opID, const int64_t row,
                                               const uint8_t *buf, const size_t bufSize) {


        // row is the input row number where the exception occured.
        // however, here want to have potential output row number so merging later works!
        // hence use outputRowCounter and inc. for each exception because do not know whether it will produce or not!

        // Note: At some point the whole Trafo task should be code generated...
        // ==> this would things EVEN faster...
        size_t bufferSize = 0;
        auto buffer = serializeExceptionToMemory(ecCode, opID, _outputRowCounter++, buf, bufSize, &bufferSize);
        int64_t code = rowToMemorySink(owner(), _exceptions, _inputSchema, _outputDataSetID, contextID(), buffer, bufferSize);
        free(buffer);
        incExceptionCounts(ecCode, opID);
    }

    void TransformTask::sinkOutputToFile(const URI &uri, const std::unordered_map<std::string, std::string> &options) {
        // reset sinks
        resetSinks();

        // init file variables
        _outputFilePath = uri;
        _outOptions = options;
    }

    void TransformTask::sinkOutputToMemory(const Schema& outputSchema, int64_t outputDataSetID, int64_t contextID) {
        assert(outputDataSetID >= 0);

        // reset sinks
        resetSinks();

        // memory sinks
        _outputSchema = outputSchema;
        _outputDataSetID = outputDataSetID;
        _contextID = contextID;
    }

    void TransformTask::setInputMemorySource(tuplex::Partition *partition, bool invalidateAfterUse) {
        assert(partition);

        // reset sources
        resetSources();

        _inputPartitions = std::vector<Partition*>{partition};
        _invalidateSourceAfterUse = invalidateAfterUse;
    }

    void TransformTask::setInputMemorySources(const std::vector<Partition *> &partitions, bool invalidateAfterUse) {
        for(auto p : partitions)
            assert(p);

        // reset sources
        resetSources();

        _inputPartitions = partitions;
        _invalidateSourceAfterUse = invalidateAfterUse;
    }

    void TransformTask::setInputFileSource(const URI& inputFile,
                                           bool makeParseExceptionsInternal,
                                           int64_t operatorID,
                                           const python::Type& rowType,
                                           const std::vector<std::string>& header,
                                           bool cellBasedFunctor,
                                           size_t numColumns,
                                           size_t rangeStart, size_t rangeSize,
                                           char delimiter, char quotechar,
                                           const std::vector<bool>& colsToKeep,
                                           size_t partitionSize,
                                           FileFormat fmt) {
        resetSources();

        assert(rowType.isTupleType());
        // this assert fails when selection pushdown is involved...
        // if(!header.empty())
        //   assert(header.size() == rowType.parameters().size());

        _inputFilePath = inputFile;
        _inputSchema = Schema(Schema::MemoryLayout::ROW, rowType);

        // completely compiled parser or the smaller version?
        if(cellBasedFunctor) {
            switch (fmt) {
                case FileFormat::OUTFMT_CSV: {
                    auto csv = new CSVReader(this, reinterpret_cast<codegen::cells_row_f>(_functor), makeParseExceptionsInternal, operatorID, exceptionCallback(), numColumns, delimiter,
                                             quotechar, colsToKeep);
                    csv->setRange(rangeStart, rangeStart + rangeSize);
                    csv->setHeader(header);
                    _reader.reset(csv);
                    break;
                }
                case FileFormat::OUTFMT_TEXT: {
                    auto text = new TextReader(this, reinterpret_cast<codegen::cells_row_f>(_functor));
                    text->setRange(rangeStart, rangeStart+rangeSize);
                    _reader.reset(text);
                    break;
                }
                case FileFormat::OUTFMT_ORC: {

#ifdef BUILD_WITH_ORC
                    auto orc = new OrcReader(this, reinterpret_cast<codegen::read_block_f>(_functor),
                                             operatorID, contextID(), partitionSize, _inputSchema);
                    orc->setRange(rangeStart, rangeSize);
                    _reader.reset(orc);
#else
                    throw std::runtime_error(MISSING_ORC_MESSAGE);
#endif
                    break;
                }
                default:
                    throw std::runtime_error("unsupported input filetype");
            }
        } else {
            switch (fmt) {
                case FileFormat::OUTFMT_CSV: {
                    auto csv = new JITCompiledCSVReader(this, reinterpret_cast<codegen::read_block_f>(_functor), numColumns,
                                                        delimiter,
                                                        quotechar); // pass this as user data for all the other callbacks.
                    csv->setRange(rangeStart, rangeStart + rangeSize);
                    csv->setHeader(header);
                    _reader.reset(csv);
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported code-generated input filetype");
            }
        }
    }

    void TransformTask::sendStatusToHistoryServer() {

        // check first if history server exists
        // note important to save in variable here. Multi threads may change this...
        auto hs = owner()->historyServer();
        if(!hs)
            return;

        // num input rows read / num output rows written + exception counts + exception partitions (to update sample IF necessary)
#warning "here assumes we have exceptions serialized to memory!"
        hs->sendTrafoTask(_stageID, _numInputRowsRead, getNumOutputRows(), exceptionCounts(), _exceptions.partitions);
    }

    void TransformTask::incExceptionCounts(int64_t ecCode, int64_t opID) {
        using namespace std;
        auto key = make_tuple(opID, i32ToEC(ecCode));
        auto it = _exceptionCounts.find(key);
        if(it == _exceptionCounts.end())
            _exceptionCounts[key] = 0;

        _exceptionCounts[key]++;
    }

    size_t TransformTask::getNumOutputRows() const {
        // two options:
        // 1.) memory sink => get from IExceptionableTask
        if(!hasFileSink()) {
            return IExecutorTask::getNumOutputRows();
        } else {
            //
            return _numOutputRowsWritten;
        }
    }

    void TransformTask::sinkOutputToHashTable(HashTableFormat fmt, int64_t outputDataSetID) {
        _htableFormat = fmt;
        _outputDataSetID = outputDataSetID;
    }
}
