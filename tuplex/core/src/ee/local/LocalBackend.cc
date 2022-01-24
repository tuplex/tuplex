//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <LocalEngine.h>
#include <ee/local/LocalBackend.h>
#include <RuntimeInterface.h>
#include <physical/ResolveTask.h>
#include <physical/TransformTask.h>
#include <physical/SimpleFileWriteTask.h>
#include <physical/SimpleOrcWriteTask.h>

#include <memory>

#include <ee/IBackend.h>
#include <physical/PhysicalPlan.h>

#include <hashmap.h>
#include <int_hashmap.h>
#include <PartitionWriter.h>
#include <physical/HashProbeTask.h>
#include <physical/LLVMOptimizer.h>
#include <HybridHashTable.h>
#include <int_hashmap.h>

namespace tuplex {


    void freeTasks(std::vector<IExecutorTask*>& tasks) {
        // delete tasks
        for(auto& task : tasks) {
            // delete task
            delete task;
            task = nullptr;
        }
        tasks.clear();
    }

    LocalBackend::LocalBackend(const Context& context) : IBackend(context), _compiler(nullptr), _options(context.getOptions()) {

        // initialize driver
        auto& logger = this->logger();

        // load runtime

        // load runtime library
        auto runtimePath = _options.RUNTIME_LIBRARY().toPath();
        if(!runtime::init(runtimePath)) {
            // runtime not present is a fatal error.
            logger.error("FATAL ERROR: Could not load runtime library");
            exit(1);
        }
        logger.info("loaded runtime library from" + runtimePath);

        logger.info("initializing LLVM backend");
        logger.warn("init JIT compiler also only in local mode");
        _compiler = std::make_unique<JITCompiler>();

        // connect to history server if given
        if(_options.USE_WEBUI()) {

            TUPLEX_TRACE("initializing REST/Curl interface");
            // init rest interface if required (check if already done by AWS!)
            RESTInterface::init();
            TUPLEX_TRACE("creating history server connector");
            _historyConn = HistoryServerConnector::connect(_options.WEBUI_HOST(),
                                                           _options.WEBUI_PORT(),
                                                           _options.WEBUI_DATABASE_HOST(),
                                                           _options.WEBUI_DATABASE_PORT());
            TUPLEX_TRACE("connection established");
        }

        // init local threads
        initExecutors(_options);
    }

    LocalBackend::~LocalBackend() {

        // remove partitions belonging to context of backend...
        if(_driver)
            _driver->freeAllPartitionsOfContext(&context());
        for(auto exec : _executors)
            exec->freeAllPartitionsOfContext(&context());

        freeExecutors();
    }

    void LocalBackend::initExecutors(const ContextOptions& options) {

        // fetch executors from local engine.
        // @TODO: use condition variable to put executors on sleep
        _executors = LocalEngine::instance().getExecutors(options.EXECUTOR_COUNT(),
                                                          options.EXECUTOR_MEMORY(),
                                                          options.PARTITION_SIZE(),
                                                          options.RUNTIME_MEMORY(),
                                                          options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                                          options.SCRATCH_DIR());

        _driver = LocalEngine::instance().getDriver(options.DRIVER_MEMORY(),
                                                    options.PARTITION_SIZE(),
                                                    options.RUNTIME_MEMORY(),
                                                    options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                                    options.SCRATCH_DIR());
    }

    void LocalBackend::freeExecutors() {
        LocalEngine::instance().freeExecutors(_executors);
        _executors.clear();
        assert(_executors.empty());
    }

    Executor *LocalBackend::driver() {
      assert(_driver);
      return _driver;
    }

    void LocalBackend::execute(tuplex::PhysicalStage *stage) {
        assert(stage);

        if(!stage)
            return;

        // history server connection should be established
        bool useWebUI = _options.USE_WEBUI();
        // register new job
        // checks if we should use the WebUI and if we are starting a new
        // job (hence there are no stages that come before the current stage
        // we are executing).
        if(useWebUI && stage->predecessors().empty()) {
            _historyServer.reset();
            _historyServer = HistoryServerConnector::registerNewJob(_historyConn,
                    "local backend", stage->plan(), _options);
            if(_historyServer) {
                logger().info("track job under " + _historyServer->trackURL());
                _historyServer->sendStatus(JobStatus::STARTED);
            }
            stage->setHistoryServer(_historyServer);
            // attach to driver as well
            _driver->setHistoryServer(_historyServer.get());
        }

        // check what type of stage it is
        auto tstage = dynamic_cast<TransformStage*>(stage);
        if(tstage)
            executeTransformStage(tstage);
        else if(dynamic_cast<HashJoinStage*>(stage)) {
            executeHashJoinStage(dynamic_cast<HashJoinStage*>(stage));
        } else if(dynamic_cast<AggregateStage*>(stage)) {
            executeAggregateStage(dynamic_cast<AggregateStage*>(stage));
        } else
            throw std::runtime_error("unknown stage encountered in local backend!");

        // send final message to history server to signal job ended
        // checks whether the historyserver has been set as well as
        // if all stages have been iterated through (we are currently on the
        // last stage) because this means the job is finished.
        if(_historyServer && stage->predecessors().size() == stage->plan()->getNumStages() - 1) {
            _historyServer->sendStatus(JobStatus::FINISHED);
            _driver->setHistoryServer(nullptr);
        }
    }

    int64_t calc_bucket_size(uint8_t* ptr) {
        auto start_ptr = ptr;
        int64_t num_rows = *((int64_t*)ptr);
        int64_t size = sizeof(int64_t);
        ptr += sizeof(int64_t);
        for(int i = 0; i < num_rows; ++i) {
            auto row_size = *((int64_t*)ptr);
            size += row_size + sizeof(int64_t);
            ptr += sizeof(int64_t) + row_size;
        }

        assert(ptr - start_ptr == size);
        return size;
    }

    // helper function
    int64_t writeHashRow(PartitionWriter* pw, uint8_t* buf, int64_t bufSize) {
        pw->writeData(buf, bufSize);
        return 0; // success
    }


    void LocalBackend::executeHashJoinStage(tuplex::HashJoinStage *hstage) {
        assert(hstage);

        Timer hashJoinTimer;

        // codegen
        auto irCode = hstage->generateCode();

        if(_options.OPT_DETAILED_CODE_STATS()) {
            logger().info("Stage " + std::to_string(hstage->number()) + " code size is " + sizeToMemString(irCode.length()));

            // remove to make sure this is not the root cause!
            logger().info(codegen::moduleStats(irCode));
        }

        if(_options.USE_LLVM_OPTIMIZER()) {
            // use optimizer
            LLVMOptimizer optimizer;
            irCode = optimizer.optimizeIR(irCode);
            logger().info("Optimization via LLVM passes took " + std::to_string(hashJoinTimer.time() * 1000.0) + " ms");

            if(_options.OPT_DETAILED_CODE_STATS()) {
                logger().info("Stage " + std::to_string(hstage->number()) + " code size after optimization is " + sizeToMemString(irCode.length()));
                logger().info(codegen::moduleStats(irCode));
            }
        }

        _compiler->registerSymbol(hstage->writeRowFunctionName(), HashProbeTask::writeRowCallback());

        // compile it!
        if(!_compiler->compile(irCode))
            throw std::runtime_error("could not compile code for stage " + std::to_string(hstage->number()));

        // fetch functor
        //*functor = reinterpret_cast<codegen::read_block_f>(_compiler->getAddrOfSymbol(hstage->funcName()));
        auto probeFunction = reinterpret_cast<void(*)(void*, map_t, const uint8_t*)>(_compiler->getAddrOfSymbol(hstage->probeFunctionName()));

        assert(probeFunction);

        // old code

        // Note: for now, generated code is super naive. later code-gen should be done smarter. I.e when this function here is invoked,
        // then basically both the left & right stages have been executed.

        // Step 1: Build phase, for the right stage put elements in the hashmap!
        auto rightStage = hstage->right();
        auto rsRight = rightStage->resultSet();
        if(!rsRight)
            throw std::runtime_error("no resultset for right stage!!!");

        assert(hstage->rightType().isTupleType());
        assert(hstage->rightKeyIndex() < hstage->rightType().parameters().size());
        auto rightKeyIndex = hstage->rightKeyIndex();
        auto rightKeyType = hstage->rightType().parameters()[rightKeyIndex];

        // find opt position in bitmap (because only opt & null vals are counted here!)
        auto rightKeyIndices = codegen::getTupleIndices(hstage->rightType(), rightKeyIndex);
        int rightKeyBitmapPos = std::get<2>(rightKeyIndices);

        int rightKeyBitmapElementPos = rightKeyBitmapPos / 64;
        int rightKeyBitmapIdx = rightKeyBitmapPos % 64;
        int numBitmapElements = codegen::calcBitmapElementCount(hstage->rightType()); // can be 0, 1, 2, ... for 0-64, 65-... nullables...


        // the hashmap
        auto hmap = hashmap_new();

        // get some information about the left stage
        auto leftStage = hstage->left();
        auto rsLeft = leftStage->resultSet();
        assert(hstage->leftType().isTupleType());
        assert(hstage->leftKeyIndex() < hstage->leftType().parameters().size());
        auto leftKeyIndex = hstage->leftKeyIndex();
        auto leftKeyType = hstage->leftType().parameters()[leftKeyIndex];

        Timer timer;
        // BUILD phase
        // TODO: codegen build phase. I.e. a function should be code generated which hashes a partition to a hashmap.
        while(rsRight->hasNextPartition()) {
            Partition* p = rsRight->getNextPartition();

            // lock partition!
            auto ptr = p->lockRaw();
            int64_t numRows = *((int64_t*)ptr);
            ptr += sizeof(int64_t);

            // @TODO: building not anymore correct because of bitmap issue...
            for(auto i = 0; i < numRows; ++i) {
                // grab key (or later key UDF) and hash it
                // check what type of key it is and form appropriate hash
                // ==> fetch row length
                Deserializer ds(Schema(Schema::MemoryLayout::ROW, hstage->rightType()));
                size_t rowLength = ds.inferLength(ptr);

                // bitmap present?
                int64_t bitmap = 0;
                if(numBitmapElements > 0)
                    bitmap = *(((int64_t*)ptr) + rightKeyBitmapElementPos);

                /// @TODO: bitmap & Co are here completely off...

                char *skey = nullptr;
                size_t skey_size = 0;
                // type:
                if(rightKeyType == python::Type::STRING) {
                    int64_t info = *( ((int64_t*)ptr) + rightKeyIndex + numBitmapElements);

                    // construct offset & fetch key...
                    // get offset
                    int64_t offset = info;
                    // offset is in the lower 32bit, the upper are the size of the var entry
                    int64_t size = ((offset & (0xFFFFFFFFl << 32)) >> 32);

                    assert(size >= 1); // strings are zero terminated so size should >= 1!
                    offset = offset & 0xFFFFFFFF;

                    // data is ptr + offset
                    char* str = (char*)(ptr + offset + (numBitmapElements + rightKeyIndex) * sizeof(int64_t));
                    assert(strlen(str) == size - 1);

                    // strcpy (incl. '\0' at end)
                    skey = new char[size];               // memory leak, fix later...
                    memcpy(skey, str, size);
                    skey_size = size;
                } else if(rightKeyType == python::Type::I64) {

                    // TODO: specialized hashmap for integer keys, which is faster...
                    int64_t key = *( ((int64_t*)ptr) + rightKeyIndex + numBitmapElements);

                    // hash ==> use int64_t keymap!
                    skey = new char[9]; // MEMORY leak, fix later...
                    memset(skey, 0, 9);
                    *((int64_t*)skey) = key;
                    skey_size = 9;

//                    std::cout<<"key: "<<key<<" skey: ";
//                    core::hexdump(std::cout, skey, 9);
//                    std::cout<<std::endl;

                } else if(rightKeyType == python::Type::makeOptionType(python::Type::STRING)) {

                    // check bit
                    if(bitmap & (1UL << rightKeyBitmapIdx)) {
                        if(leftKeyType == python::Type::makeOptionType(python::Type::STRING)) {
                            // key is empty string
                            skey = new char[1];
                            skey[0] = '\0';
                            skey_size = 1;
                        } // if the left is just str, not Option[str], don't insert anything for None (because this can never match)
                    } else {

                        // prefix key with _ to indicate validity
                        // extract string but prefix to indicate zero or not!
                        int64_t info = *( ((int64_t*)ptr) + rightKeyIndex + numBitmapElements);

                        // construct offset & fetch key...
                        // get offset
                        int64_t offset = info;
                        // offset is in the lower 32bit, the upper are the size of the var entry
                        int64_t size = ((offset & (0xFFFFFFFFl << 32)) >> 32);

                        assert(size >= 1); // strings are zero terminated so size should >= 1!
                        offset = offset & 0xFFFFFFFF;

                        // data is ptr + offset
                        char* str = (char*)(ptr + offset + (numBitmapElements + rightKeyIndex) * sizeof(int64_t));
                        assert(strlen(str) == size - 1);

                        // strcpy (incl. '\0' at end)
                        if(leftKeyType == python::Type::makeOptionType(python::Type::STRING)) {
                            skey = new char[size + 1];               // memory leak, fix later...
                            skey[0] = '_'; // some dummy val.
                            memcpy(skey + 1, str, size);
                            skey_size = size + 1;
                        } else { // if left is str, no need to prefix
                            skey = new char[size];               // memory leak, fix later...
                            memcpy(skey, str, size);
                            skey_size = size;
                        }
                    }
                } else {
                    throw std::runtime_error("unsupported key type in hashjoin stage found!");
                }

                // bucket format is as following:
                // 1.) N ... int64_t for how many rows in that bucket
                // 2.) then N times int64_t|data with size/data.

                // first, need to check whether entry exists in hashmap or not. If so, append to bucket!
                // (multi key map)
                char *value = nullptr;
                if(skey && MAP_OK == hashmap_get(hmap, skey, skey_size, (void**)(&value))) {

                    // old entry exists, free it & copy it over
                    // determine size
                    int64_t bucket_size = calc_bucket_size((uint8_t*)value);
                    int64_t num_rows = *((int64_t*)value);

                    // check calculation is not off, i.e. less than one MB for the bucket.
                    // else probably probed with the bigger table -.-
                    // assert(bucket_size < 1024 * 1024);

                    uint8_t* sdata = new uint8_t[bucket_size + sizeof(int64_t) + rowLength]; // memory leak, fix later...
                    memcpy(sdata, value, bucket_size);
                    *((int64_t*)sdata) = num_rows + 1;
                    *(((int64_t*)(sdata + bucket_size))) = rowLength;
                    memcpy(sdata + bucket_size + sizeof(int64_t), ptr, rowLength);
                    hashmap_put(hmap, skey, skey_size, sdata);

                    // check
                    assert(calc_bucket_size(sdata) == bucket_size + sizeof(int64_t) + rowLength);

                    delete [] value;
                } else {
                    // new entry

                    uint8_t* sdata = new uint8_t[sizeof(int64_t) * 2 + rowLength]; // memory leak, fix later...
                    *((int64_t*)sdata) = 1;
                    *(((int64_t*)sdata) + 1) = rowLength;
                    memcpy(sdata + 2 * sizeof(int64_t), ptr, rowLength);
                    hashmap_put(hmap, skey, skey_size, sdata);
                }
                ptr += rowLength;
            }

            p->unlock();
            p->invalidate();
        }

        logger().info("[Hash Join] Build phase took " + std::to_string(timer.time()) + "s");

        // Step 2: Hash phase, hash for each tuple in left stage key and check whether right stage key exists.
        // @TODO: codegen, i.e. a function which probes one partition against a hashmap (read-only).
        // each function invocation will yield one or more partitions as output. ==> probe tasks?
        assert((leftKeyType == rightKeyType) ||
         (leftKeyType.isOptionType() && leftKeyType.getReturnType() == rightKeyType) ||
         (rightKeyType.isOptionType() && rightKeyType.getReturnType() == leftKeyType) ||
         (leftKeyType.isOptionType() && rightKeyType == python::Type::NULLVALUE) ||
         (rightKeyType.isOptionType() && leftKeyType == python::Type::NULLVALUE));
        if(!rsLeft)
            throw std::runtime_error("left stage has no resultset!");

        // @TODO: multithreaded execution of probe tasks!
        // issue HashTasks
        auto combinedType = hstage->combinedType();
        Schema combinedSchema(Schema::MemoryLayout::ROW, combinedType);
        std::vector<IExecutorTask*> probeTasks;
        for(auto partition : rsLeft->partitions()) {
            probeTasks.emplace_back(new HashProbeTask(partition, hmap, probeFunction,
                                                      hstage->combinedType(),
                                                      hstage->outputDataSetID(),
                                                      hstage->context().id()));
        }

        auto completedTasks = performTasks(probeTasks);
        // sort tasks to restore partition output order
        sortTasks(completedTasks);

        // fetch output partitions
        std::vector<Partition*> outputPartitions;
        for(auto& task : completedTasks) {
            auto output = task->getOutputPartitions();
            outputPartitions.insert(outputPartitions.end(), output.begin(), output.end());
        }

        logger().info("[Hash Join] Probing took " + std::to_string(timer.time()) + "s");

        // free hashmap
        hashmap_free(hmap);

        // set result set based on partition writer result (no exceptions here!!!)
        hstage->setResultSet(std::make_shared<ResultSet>(combinedSchema, outputPartitions));

        std::stringstream ss;
        ss<<"[Hash Join Stage] Stage "<<hstage->number()<<" took "<<hashJoinTimer.time()<<"s";
        Logger::instance().defaultLogger().info(ss.str());
    }

    std::vector<IExecutorTask*> LocalBackend::createLoadAndTransformToMemoryTasks(
            TransformStage *tstage,
            const tuplex::ContextOptions &options,
            const std::shared_ptr<TransformStage::JITSymbols>& syms) {

        using namespace std;
        vector<IExecutorTask*> tasks;
        assert(tstage);
        assert(syms);

        size_t readBufferSize = options.READ_BUFFER_SIZE();
        bool normalCaseEnabled = options.OPT_NULLVALUE_OPTIMIZATION(); // this is important so exceptions get upgraded to internal ones

        // use normal case schemas here
        auto inputSchema = tstage->normalCaseInputSchema();
        auto inputRowType = inputSchema.getRowType();
        auto outputSchema = tstage->normalCaseOutputSchema();

        // check what type of input the pipeline has (memory or files)
        if(tstage->fileInputMode()) {
            // files
            // input is multiple files, use split file strategy here.
            // and issue tasks to executor workqueue!

            assert(tstage->inputMode() == EndPointMode::FILE);

            // split input files into multiple tasks
            // => for now simply one task per file
            auto fileSchema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING, python::Type::I64}));

            std::vector<std::string> header;
            // fetch from first FileInputOperator number of input columns (BEFORE optimization/projection pushdown!)
            size_t numColumns = tstage->csvNumFileInputColumns();


            // CSV, set header
            if(tstage->csvHasHeader()) {
                // because of projection pushdown, need to decode from input params!
                header = tstage->csvHeader();
            }


            // other CSV params
            char delimiter = tstage->csvInputDelimiter();
            char quotechar = tstage->csvInputQuotechar();

            vector<bool>  colsToKeep = tstage->columnsToKeep(); // after projection pushdown, what to keep

            for(auto partition : tstage->inputPartitions()) {
                // get num
                auto numFiles = partition->getNumRows();
                const uint8_t* ptr = partition->lock();
                size_t bytesRead = 0;
                // found
                for(int i = 0; i < numFiles; ++i) {
                    // found file -> create task / split into multiple tasks
                    Row row = Row::fromMemory(fileSchema, ptr, partition->capacity() - bytesRead);
                    URI uri(row.getString(0));
                    size_t file_size = row.getInt(1);

                    // split files if splitsize != 0
                    if(options.INPUT_SPLIT_SIZE() == 0) {
                        // one task per URI
                        auto task = new TransformTask();
                        task->setFunctor(syms->functor);
                        task->setInputFileSource(uri, normalCaseEnabled, tstage->fileInputOperatorID(), inputRowType, header,
                                                 !options.OPT_GENERATE_PARSER(),
                                                 numColumns, 0, 0, delimiter, quotechar, colsToKeep, options.PARTITION_SIZE(), tstage->inputFormat());
                        // hash table or memory output?
                        if(tstage->outputMode() == EndPointMode::HASHTABLE) {
                            if (tstage->hashtableKeyByteWidth() == 8)
                                task->sinkOutputToHashTable(HashTableFormat::UINT64,
                                                            tstage->outputDataSetID());
                            else
                                task->sinkOutputToHashTable(HashTableFormat::BYTES,
                                                            tstage->outputDataSetID());
                        } else {
                            assert(tstage->outputMode() == EndPointMode::FILE ||
                            tstage->outputMode() == EndPointMode::MEMORY);
                            task->sinkOutputToMemory(outputSchema, tstage->outputDataSetID(), tstage->context().id());
                        }

                        task->sinkExceptionsToMemory(inputSchema);
                        task->setStageID(tstage->getID());
                        task->setOutputLimit(tstage->outputLimit());
                        // add to tasks
                        tasks.emplace_back(std::move(task));
                    } else {
                        // split files according to split size
                        size_t s = 0;
                        size_t splitSize = options.INPUT_SPLIT_SIZE();
                        int num_parts = 0;

                        // two options: 1.) file is larger than split size => split 2.) one task for fiel_size <= split size
                        if(file_size <= splitSize) {
                            // 1 task (range 0,0 to indicate full file)
                            auto task = new TransformTask();
                            task->setFunctor(syms->functor);
                            task->setInputFileSource(uri, normalCaseEnabled, tstage->fileInputOperatorID(), inputRowType, header,
                                                     !options.OPT_GENERATE_PARSER(),
                                                     numColumns, 0, 0, delimiter,
                                                     quotechar, colsToKeep, options.PARTITION_SIZE(), tstage->inputFormat());
                            // hash table or memory output?
                            if(tstage->outputMode() == EndPointMode::HASHTABLE) {
                                if (tstage->hashtableKeyByteWidth() == 8)
                                    task->sinkOutputToHashTable(HashTableFormat::UINT64,
                                                                tstage->outputDataSetID());
                                else
                                    task->sinkOutputToHashTable(HashTableFormat::BYTES,
                                                                tstage->outputDataSetID());
                            }
                            else {
                                assert(tstage->outputMode() == EndPointMode::FILE ||
                                       tstage->outputMode() == EndPointMode::MEMORY);
                                task->sinkOutputToMemory(outputSchema, tstage->outputDataSetID(), tstage->context().id());
                            }
                            task->sinkExceptionsToMemory(inputSchema);
                            task->setStageID(tstage->getID());
                            task->setOutputLimit(tstage->outputLimit());
                            // add to tasks
                            tasks.emplace_back(std::move(task));
                            num_parts++;
                        } else {
                            // split into multiple tasks
                            while(s + splitSize <= file_size) {

                                auto rangeStart = s;
                                auto rangeEnd = std::min(s + splitSize, file_size);

                                // last task should go to file end, i.e. modify accordingly
                                if(file_size - rangeEnd < splitSize)
                                    rangeEnd = file_size;

                                auto task = new TransformTask();
                                task->setFunctor(syms->functor);
                                task->setInputFileSource(uri, normalCaseEnabled, tstage->fileInputOperatorID(), inputRowType, header,
                                                         !options.OPT_GENERATE_PARSER(),
                                                         numColumns, rangeStart, rangeEnd - rangeStart, delimiter,
                                                         quotechar, colsToKeep, options.PARTITION_SIZE(), tstage->inputFormat());
                                // hash table or memory output?
                                if(tstage->outputMode() == EndPointMode::HASHTABLE) {
                                    if (tstage->hashtableKeyByteWidth() == 8)
                                        task->sinkOutputToHashTable(HashTableFormat::UINT64,
                                                                    tstage->outputDataSetID());
                                    else
                                        task->sinkOutputToHashTable(HashTableFormat::BYTES,
                                                                    tstage->outputDataSetID());
                                }
                                else {
                                    assert(tstage->outputMode() == EndPointMode::FILE ||
                                           tstage->outputMode() == EndPointMode::MEMORY);
                                    task->sinkOutputToMemory(outputSchema, tstage->outputDataSetID(), tstage->context().id());
                                }
                                task->sinkExceptionsToMemory(inputSchema);
                                task->setStageID(tstage->getID());
                                task->setOutputLimit(tstage->outputLimit());
                                // add to tasks
                                tasks.emplace_back(std::move(task));

                                s += splitSize;
                                num_parts++;
                            }
                        }

                        stringstream ss;
                        ss<<"split "<<uri.toPath()<<" into "<<pluralize(num_parts, "task");
                        logger().info(ss.str());
                    }

                    ptr += row.serializedLength();
                    bytesRead += row.serializedLength();
                }

                partition->unlock();
            }
        } else {
            // memory
            // create all tasks
            // input are memory partitions
            // --> issue for each memory partition a transform task and put it into local workqueue
            assert(tstage->inputMode() == EndPointMode::MEMORY);


            // restrict after input limit
            size_t numInputRows = 0;
            auto inputPartitions = tstage->inputPartitions();
            for(int i = 0; i < inputPartitions.size(); ++i) {
                auto partition = inputPartitions[i];
                auto task = new TransformTask();
                if (tstage->updateInputExceptions()) {
                    task->setFunctor(syms->functorWithExp);
                } else {
                    task->setFunctor(syms->functor);
                }
                task->setUpdateInputExceptions(tstage->updateInputExceptions());
                task->setInputMemorySource(partition, !partition->isImmortal());
                // hash table or memory output?
                if(tstage->outputMode() == EndPointMode::HASHTABLE) {
                    if (tstage->hashtableKeyByteWidth() == 8)
                        task->sinkOutputToHashTable(HashTableFormat::UINT64,
                                                    tstage->outputDataSetID());
                    else
                        task->sinkOutputToHashTable(HashTableFormat::BYTES,
                                                    tstage->outputDataSetID());
                }
                else {
                    assert(tstage->outputMode() == EndPointMode::FILE ||
                           tstage->outputMode() == EndPointMode::MEMORY);
                    task->sinkOutputToMemory(outputSchema, tstage->outputDataSetID(), tstage->context().id());
                }

                auto partitionId = uuidToString(partition->uuid());
                auto info = tstage->partitionToExceptionsMap()[partitionId];
                task->setInputExceptionInfo(info);
                task->setInputExceptions(tstage->inputExceptions());
                task->sinkExceptionsToMemory(inputSchema);
                task->setStageID(tstage->getID());
                task->setOutputLimit(tstage->outputLimit());
                tasks.emplace_back(std::move(task));
                numInputRows += partition->getNumRows();

                // input limit exhausted? break!
                if(numInputRows >= tstage->inputLimit())
                    break;
            }
        }

        return tasks;
    }

    PyObject* preparePythonPipeline(const std::string& py_code, const std::string& pipeline_name) {
        PyObject* pip_object = nullptr;

#ifndef NDEBUG
        stringToFile("python_code_" + pipeline_name + ".py", py_code);
#endif

        // decode
        if(!py_code.empty()) {

            assert(!pipeline_name.empty());

            python::lockGIL();

            //Note: maybe put all these user-defined functions into fake, tuplex module??

            // get main module
            // Note: This needs to get called BEFORE globals/locals...
            auto main_mod = python::getMainModule();
            auto moduleDict = PyModule_GetDict(main_mod);
            assert(moduleDict);

            // set globals & locals to main dict.
            PyRun_String(py_code.c_str(), Py_file_input, moduleDict, moduleDict);

            // check for errors
            if(PyErr_Occurred()) {
                Logger::instance().defaultLogger().error("while interpreting python pipeline code, an error occurred.");
                PyErr_Print();
                std::cerr<<std::endl;
                std::cout.flush();
                std::cerr.flush();
                pip_object = nullptr;
                PyErr_Clear();
            } else {
                // fetch function object
                pip_object = PyDict_GetItemString(moduleDict, pipeline_name.c_str());
                if(!pip_object) {
                    python::unlockGIL();
                    throw std::runtime_error("could not find function '" + pipeline_name + "' in main dict");
                }

                if(PyErr_Occurred()) {
                    PyErr_Print();
                    std::cerr<<std::endl;
                    std::cout.flush();
                    std::cerr.flush();
                    pip_object = nullptr;
                    PyErr_Clear();
                }
            }

            python::unlockGIL();
        }
        return pip_object;
    }

    std::vector<std::tuple<size_t, PyObject*>> inputExceptionsToPythonObjects(const std::vector<Partition *>& partitions, Schema schema) {
        using namespace tuplex;

        std::vector<std::tuple<size_t, PyObject*>> pyObjects;
        for (const auto &partition : partitions) {
            auto numRows = partition->getNumRows();
            const uint8_t* ptr = partition->lock();

            python::lockGIL();
            for (int i = 0; i < numRows; ++i) {
                int64_t rowNum = *((int64_t*)ptr);
                ptr += sizeof(int64_t);
                int64_t ecCode = *((int64_t*)ptr);
                ptr += 2 * sizeof(int64_t);
                int64_t objSize = *((int64_t*)ptr);
                ptr += sizeof(int64_t);

                PyObject* pyObj = nullptr;
                if (ecCode == ecToI64(ExceptionCode::PYTHON_PARALLELIZE)) {
                    pyObj = python::deserializePickledObject(python::getMainModule(), (char *) ptr, objSize);
                } else {
                    pyObj = python::rowToPython(Row::fromMemory(schema, ptr, objSize), true);
                }

                ptr += objSize;
                pyObjects.emplace_back(rowNum, pyObj);
            }
            python::unlockGIL();

            partition->unlock();
            partition->invalidate();
        }

        return pyObjects;
    }

    void setExceptionInfo(const std::vector<Partition*> &normalOutput, const std::vector<Partition*> &exceptions, std::unordered_map<std::string, ExceptionInfo> &partitionToExceptionsMap) {
        if (exceptions.empty()) {
            for (const auto &p : normalOutput) {
                partitionToExceptionsMap[uuidToString(p->uuid())] = ExceptionInfo();
            }
            return;
        }

        auto expRowCount = 0;
        auto expInd = 0;
        auto expRowOff = 0;
        auto expByteOff = 0;

        auto expNumRows = exceptions[0]->getNumRows();
        auto expPtr = exceptions[0]->lockWrite();
        auto rowsProcessed = 0;
        for (const auto &p : normalOutput) {
            auto pNumRows = p->getNumRows();
            auto curNumExps = 0;
            auto curExpOff = expRowOff;
            auto curExpInd = expInd;
            auto curExpByteOff = expByteOff;

            while (*((int64_t *) expPtr) - rowsProcessed <= pNumRows + curNumExps && expRowCount < expNumRows) {
                *((int64_t *) expPtr) -= rowsProcessed;
                curNumExps++;
                expRowOff++;
                auto eSize = ((int64_t *)expPtr)[3] + 4*sizeof(int64_t);
                expPtr += eSize;
                expByteOff += eSize;
                expRowCount++;

                if (expRowOff == expNumRows && expInd < exceptions.size() - 1) {
                    exceptions[expInd]->unlockWrite();
                    expInd++;
                    expPtr = exceptions[expInd]->lockWrite();
                    expNumRows = exceptions[expInd]->getNumRows();
                    expRowOff = 0;
                    expByteOff = 0;
                    expRowCount = 0;
                }
            }

            rowsProcessed += curNumExps + pNumRows;
            partitionToExceptionsMap[uuidToString(p->uuid())] = ExceptionInfo(curNumExps, curExpInd, curExpOff, curExpByteOff);
        }

        exceptions[expInd]->unlockWrite();
    }

    void LocalBackend::executeTransformStage(tuplex::TransformStage *tstage) {

        Timer stageTimer;
        Timer timer; // for detailed measurements.

        // reset Partition stats
        Partition::resetStatistics();

        // special case: no input, return & set empty result
        // Note: file names & sizes are also saved in input partition!
        if (tstage->inputMode() != EndPointMode::HASHTABLE
            && tstage->predecessors().empty()
            && tstage->inputPartitions().empty()) {
            tstage->setEmptyResult();
            return;
        }

        // special case: skip stage, i.e. empty code and mem2mem
        if(tstage->code().empty() &&  !tstage->fileInputMode() && !tstage->fileOutputMode()) {
            auto pyObjects = inputExceptionsToPythonObjects(tstage->inputExceptions(), tstage->normalCaseInputSchema());
            tstage->setMemoryResult(tstage->inputPartitions(), std::vector<Partition*>{}, std::unordered_map<std::string, ExceptionInfo>(), pyObjects);
            pyObjects.clear();
            // skip stage
            Logger::instance().defaultLogger().info("[Transform Stage] skipped stage " + std::to_string(tstage->number()) + " because there is nothing todo here.");
            return;
        }

        bool merge_except_rows = _options.OPT_MERGE_EXCEPTIONS_INORDER();

        // when result of this stage is a hash table, merging makes no sense b.c.
        // rows will be partitioned! => there is no guarantee on the order!
        // => disable
        // also, b.c. we use fast file merging (no order), disable with endpoint file...
        // note: enabled again merging for files...
        //  || tstage->outputMode() == EndPointMode::FILE
        if(tstage->outputMode() == EndPointMode::HASHTABLE) {
            Logger::instance().defaultLogger().info("provided option to explicitly merge bad rows in order back, however rows will be hashed. Disabling option. To silence this warning, set"
                                                    "\ntuplex.optimizer.mergeExceptionsInOrder=false");
            merge_except_rows = false;
        }

        if (tstage->outputMode() == EndPointMode::FILE) {
            // decactivated because still buggy
            // // run output validation one more time here before execution (assume no changes then)
            // if(!validateOutputSpecification(tstage->outputURI())) {
            //     throw std::runtime_error("Failed to validate output specification,"
            //                              " can not write to " + tstage->outputURI().toString() + " (directory not empty?)");
            // }
        }

        // Processing of a transform stage works as follows:
        // 1.)  compile all functions required for the next steps
        // 2.)  spawn & execute all tasks which load data from files OR cpython
        //      process them using supplied UDFs in a pipeline
        //      and store the result in main-memory partitions
        // 3.)  If exceptions occurred AND resolvers exist,
        //      spawn resolve tasks which will merge in resolved rows and spawn new exceptions.
        // 4.)  If the stage is one that writes to disk, reorder tasks in the desired order and
        //      start file writing tasks with correct splitting etc. Else, if there is another
        //      stage coming, simply handover all the partitions.


        using namespace std;
        assert(tstage);

        // 1.) COMPILATION
        // compile code & link functions to tasks
        LLVMOptimizer optimizer;
        auto syms = tstage->compile(*_compiler, _options.USE_LLVM_OPTIMIZER() ? &optimizer : nullptr, false); // @TODO: do not compile slow path yet, do it later in parallel when other threads are already working!
        bool combineOutputHashmaps = syms->aggInitFunctor && syms->aggCombineFunctor && syms->aggAggregateFunctor;
        JobMetrics& metrics = tstage->PhysicalStage::plan()->getContext().metrics();
        double total_compilation_time = metrics.getTotalCompilationTime() + timer.time();
        metrics.setTotalCompilationTime(total_compilation_time);
        {
            std::stringstream ss;
            ss<<"[Transform Stage] Stage "<<tstage->number()<<" compiled to x86 in "<<timer.time()<<"s";
            Logger::instance().defaultLogger().info(ss.str());
        }

        // -------------------------------------------------------------------
        // 2.) MAIN MEMORY processing tasks
        timer.reset();

        // ==> init using optionally hashmaps from dependents
        int64_t init_rc = 0;
        if((init_rc = syms->initStageFunctor(tstage->initData().numArgs,
                                  reinterpret_cast<void**>(tstage->initData().hash_maps),
                                  reinterpret_cast<void**>(tstage->initData().null_buckets))) != 0)
            throw std::runtime_error("initStage() failed for stage " + std::to_string(tstage->number()) + " with code " + std::to_string(init_rc));


        // init aggregate by key
        if(syms->aggAggregateFunctor) {
            initThreadLocalAggregateByKey(syms->aggInitFunctor, syms->aggCombineFunctor, syms->aggAggregateFunctor);
        }
        else {
            if (syms->aggInitFunctor && syms->aggCombineFunctor) {
                initThreadLocalAggregates(_options.EXECUTOR_COUNT() + 1, syms->aggInitFunctor, syms->aggCombineFunctor);
            }
        }

        auto tasks = createLoadAndTransformToMemoryTasks(tstage, _options, syms);
        auto completedTasks = performTasks(tasks);

        // Note: this doesn't work yet because of the globals.
        // to make this work, need better global mapping...
//        auto completedTasks = performTasks(tasks, [&syms, &optimizer, &tstage, this]() {
//            // TODO/Note: could prepare code of parent stage already while current one is running! I.e. do this for the first dependent only to avoid conflicts...
//            syms = tstage->compile(*_compiler, _options.USE_LLVM_OPTIMIZER() ? &optimizer : nullptr, false);
//        });

        // calc number of input rows and total wall clock time
        size_t numInputRows = 0;
        double totalWallTime = 0.0;
        for(auto task : completedTasks) {
            numInputRows += task->getNumInputRows();
            totalWallTime += task->wallTime();
        }

        {
            std::stringstream ss;
            ss<<"[Transform Stage] Stage "<<tstage->number()<<" completed "<<completedTasks.size()<<" load&transform tasks in "<<timer.time()<<"s";
            Logger::instance().defaultLogger().info(ss.str());
        }

        {
            std::stringstream ss;
            double time_per_fast_path_row_in_ms = totalWallTime / numInputRows * 1000.0;
            ss<<"[Transform Stage] Stage "<<tstage->number()<<" total wall clock time: "
              <<totalWallTime<<"s, "<<pluralize(numInputRows, "input row")
              <<", time to process 1 row via fast path: "<<time_per_fast_path_row_in_ms<<"ms";
            Logger::instance().defaultLogger().info(ss.str());

            // fast path
            metrics.setFastPathTimes(tstage->number(), totalWallTime, timer.time(), time_per_fast_path_row_in_ms * 1000000.0);
        }

        // -------------------------------------------------------------------
        // 3.) check for exceptions + updates + resolution
        timer.reset();
        auto ecountsBeforeResolution = calcExceptionCounts(completedTasks);
        auto totalECountsBeforeResolution = totalExceptionCounts(ecountsBeforeResolution);

        // NEW: resolve using either 1) slow generated code path or 2) pure python code path (interpreter)
        // => there are fallback mechanisms...

        bool executeSlowPath = true;
        //TODO: implement pure python resolution here...
        // exceptions found or slowpath data given?
        if(totalECountsBeforeResolution > 0 || !tstage->inputExceptions().empty()) {
            stringstream ss;
            // log out what exists in a table
            ss<<"Exception details: "<<endl;

            bool normalCaseViolationFound = false;
            bool badParseInputFound = false;

            vector<string> headers{"OperatorID", "Exception", "Count"};
            vector<Row> lines;
            if(totalECountsBeforeResolution) {
                for(auto keyval : ecountsBeforeResolution) {
                    auto opid = std::get<0>(keyval.first);
                    auto ec = std::get<1>(keyval.first);

                    if(ec == ExceptionCode::NORMALCASEVIOLATION)
                        normalCaseViolationFound = true;
                    if(ec == ExceptionCode::BADPARSE_STRING_INPUT)
                        badParseInputFound = true;

                    lines.push_back(Row((int64_t)opid, exceptionCodeToPythonClass(ec), (int64_t)keyval.second));
                }
            }

            if(!tstage->inputExceptions().empty()) {
                size_t numExceptions = 0;
                for (auto &p : tstage->inputExceptions())
                    numExceptions += p->getNumRows();
                lines.push_back(Row("(input)", exceptionCodeToPythonClass(ExceptionCode::NORMALCASEVIOLATION), (int64_t)numExceptions));
                totalECountsBeforeResolution += numExceptions;
            }

            printTable(ss, headers, lines, false);
            auto msg = ss.str(); trim(msg);
            Logger::instance().defaultLogger().info(msg);
            ss.str("");

            // resolution
            // => for optimization purposes we might want to keep cases separate (cache operator)
            //    whereas for other purposes (hashing) we need to combine cases together
            if(tstage->persistSeparateCases()) {
                // deactivate merging in order
                merge_except_rows = false;
            }

            // should slow path get executed
            executeSlowPath = syms->resolveFunctor || !tstage->purePythonCode().empty();

            // any ops with resolver IDs?
            if(executeSlowPath && !tstage->operatorIDsWithResolvers().empty())
                executeSlowPath = true;
            else
                executeSlowPath = false;

            // any normalcase violation or parseinput?
            if(badParseInputFound || normalCaseViolationFound)
                executeSlowPath = true;

            // input exceptions or py objects?
            if(!tstage->inputExceptions().empty())
                executeSlowPath = true;

            if(executeSlowPath) {
                // only if functor or python is available, else there is simply no slow path to resolve!
                if(syms->resolveFunctor || !tstage->purePythonCode().empty()) {
                    using namespace  std;

                    // if resolution via compiled slow path is deactivated, use always the interpreter
                    // => this can be achieved by setting functor to nullptr!
                    auto resolveFunctor = _options.RESOLVE_WITH_INTERPRETER_ONLY() ? nullptr : syms->resolveFunctor;

                    // cout<<"*** num tasks before resolution: "<<completedTasks.size()<<" ***"<<endl;
                    completedTasks = resolveViaSlowPath(completedTasks, merge_except_rows, resolveFunctor, tstage, combineOutputHashmaps);
                    // cout<<"*** num tasks after resolution: "<<completedTasks.size()<<" ***";
                }

                // @TODO: if IO thing is deactivated, then need to process exceptions from previous stage as well via slow path...
                // => a cache operator would be really, really much smarter...

                auto ecountsAfterResolution = calcExceptionCounts(completedTasks);
                auto totalECountsAfterResolution = totalExceptionCounts(ecountsAfterResolution);

                double slow_path_total_time = timer.time();
                ss.str("");
                ss<<"slow path resolved "<<(totalECountsBeforeResolution - totalECountsAfterResolution)<<"/"<<totalECountsBeforeResolution<< " exceptions ";
                ss<<"in "<<slow_path_total_time<<"s";
                logger().info(ss.str());


                totalWallTime = 0.0;
                size_t slowPathNumInputRows = 0;
                for(auto task : completedTasks) {
                    if(task->type() == TaskType::RESOLVE) {
                        totalWallTime += task->wallTime();
                        slowPathNumInputRows += task->getNumInputRows();
                    }
                }
                double time_per_row_slow_path_ms = totalWallTime / slowPathNumInputRows * 1000.0;

                // print timing info for slow path
                ss.str("");
                ss<<"slow path for Stage "<<tstage->number()<<": total wall clock time: "<<totalWallTime<<"s, "
                  <<"time to process 1 row via slow path: "<<time_per_row_slow_path_ms<<"ms";
                logger().info(ss.str());
                metrics.setSlowPathTimes(tstage->number(), totalWallTime, slow_path_total_time,
                                         time_per_row_slow_path_ms * 1000000.0);
            }
        }

        // only print out resolve info, when there were exceptions found.
        if (totalECountsBeforeResolution > 0 && executeSlowPath) {
            std::stringstream ss;
            ss << "[Transform Stage] Stage " << tstage->number() << " completed "
               << pluralize(completedTasks.size(), "resolve task") << " in " << timer.time() << "s";
            Logger::instance().defaultLogger().info(ss.str());
        }

        // -------------------------------------------------------------------

        // 4.) reordering + optional file output
        // sort tasks AFTER their ord, this is necessary to have rows in the order they came in. ==> for optimization later
        // this might be dropped
#warning "later add switch to Tuplex which ignores order to save time! Will allow faster resolution..."
        timer.reset();

        // sorting only make sense when order is needed
        sortTasks(completedTasks);

        // set result according to endpoint mode
        switch(tstage->outputMode()) {
            case EndPointMode::FILE: {
                // i.e. if output format is tuplex, then attach special writer!
                // ==> could maybe codegen avro as output format, and then write to whatever??
                writeOutput(tstage, completedTasks);
                break;
            }
            case EndPointMode::MEMORY: {
                // memory output, fetch partitions & ecounts
                vector<Partition *> output;
                vector<Partition *> generalOutput;
                unordered_map<string, ExceptionInfo> partitionToExceptionsMap;
                vector<Partition*> remainingExceptions;
                vector<tuple<size_t, PyObject*>> nonConformingRows; // rows where the output type does not fit,
                                                                     // need to manually merged.
                unordered_map<tuple<int64_t, ExceptionCode>, size_t> ecounts;
                size_t rowDelta = 0;
                for (const auto& task : completedTasks) {
                    auto taskOutput = getOutputPartitions(task);
                    auto taskRemainingExceptions = getRemainingExceptions(task);
                    auto taskGeneralOutput = generalCasePartitions(task);
                    auto taskNonConformingRows = getNonConformingRows(task);
                    auto taskExceptionCounts = getExceptionCounts(task);

                    // update exception counts
                    ecounts = merge_ecounts(ecounts, taskExceptionCounts);

                    // update nonConforming with delta
                    for(int i = 0; i < taskNonConformingRows.size(); ++i) {
                        auto t = taskNonConformingRows[i];
                        t = std::make_tuple(std::get<0>(t) + rowDelta, std::get<1>(t));
                        taskNonConformingRows[i] = t;
                    }

                    // debug trace issues
                    using namespace std;
                    std::string task_name = "unknown";
                    if(task->type() == TaskType::UDFTRAFOTASK)
                        task_name = "udf trafo task";
                    if(task->type() == TaskType::RESOLVE)
                        task_name = "resolve";

                    setExceptionInfo(taskOutput, taskGeneralOutput, partitionToExceptionsMap);
                    std::copy(taskOutput.begin(), taskOutput.end(), std::back_inserter(output));
                    std::copy(taskRemainingExceptions.begin(), taskRemainingExceptions.end(), std::back_inserter(remainingExceptions));
                    std::copy(taskGeneralOutput.begin(), taskGeneralOutput.end(), std::back_inserter(generalOutput));
                    std::copy(taskNonConformingRows.begin(), taskNonConformingRows.end(), std::back_inserter(nonConformingRows));

                    // compute the delta used to offset records!
                    for (const auto &p : taskOutput)
                        rowDelta += p->getNumRows();
                    for (const auto &p : taskGeneralOutput)
                        rowDelta += p->getNumRows();
                    rowDelta += taskNonConformingRows.size();
                }

                tstage->setMemoryResult(output, generalOutput, partitionToExceptionsMap, nonConformingRows, remainingExceptions, ecounts);
                break;
            }
            case EndPointMode::HASHTABLE: {

                // need to merge hashtables of individual tasks together
                // note: this won't work when exceptions are involved -.-
                if(completedTasks.empty()) {
                    tstage->setHashResult(nullptr, nullptr);
                } else {
                    auto hsink = createFinalHashmap({completedTasks.cbegin(), completedTasks.cend()}, tstage->hashtableKeyByteWidth(), combineOutputHashmaps);
                    tstage->setHashResult(hsink.hm, hsink.null_bucket);
                }
                break;
            }
            default:
                throw std::runtime_error("unknown endpoint in execute");
        }

        // if aggregate stage, convert thread-local results to global one and assign as result set...
        if(syms->aggInitFunctor && syms->aggCombineFunctor && !syms->aggAggregateFunctor) {
            uint8_t* aggResult = nullptr;
            int64_t aggResultSize = 0;
            if(!fetchAggregate(&aggResult, &aggResultSize)) // this also frees the thread-local aggregates...
                throw std::runtime_error("failed to fetch global aggregate result.");

            if(!aggResult)
                throw std::runtime_error("invalid aggregate!");

            // convert to partitions.
            // right now, it's a general aggregate. So fairly easy todo.
            // later however, it's going to be hashaggregate which needs to be divided into partitions.

            Partition* p = _driver->allocWritablePartition(aggResultSize + sizeof(int64_t),
                                                           tstage->outputSchema(), tstage->outputDataSetID(),
                                                           tstage->context().id());
            auto ptr = p->lockWriteRaw();
            *(int64_t*)ptr = 1; // one row (it's a general aggregate for now...)
            memcpy(ptr + sizeof(int64_t), aggResult, aggResultSize);
            p->unlockWrite();

            free(aggResult);
            // set resultset!

            tstage->setMemoryResult(vector<Partition*>{p}); // @TODO: what about exceptions??
        }


        // call release func for stage globals
        if(syms->releaseStageFunctor() != 0)
            throw std::runtime_error("releaseStage() failed for stage " + std::to_string(tstage->number()));

        // add exception counts from previous stages to current one
        // @TODO: need to add test for this. I.e. the whole exceptions + joins needs to revised...

        // send final result count (exceptions + co)
        if(_historyServer) {
            size_t numOutputRows = 0;
            if (tstage->outputMode() == EndPointMode::HASHTABLE) {
                for (const auto& task : completedTasks) {
                    numOutputRows += task->getNumOutputRows();
                }
            } else {
                auto rs = tstage->resultSet();
                assert(rs);
                numOutputRows = rs->rowCount();
            }
            auto ecounts = tstage->exceptionCounts();
            _historyServer->sendStageResult(tstage->number(), numInputRows, numOutputRows, ecounts);
        }

        {
            std::stringstream ss;
            ss<<"[Transform Stage] Stage "<<tstage->number()<<" completed "<<completedTasks.size()<<" sink tasks in "<<timer.time()<<"s";
            Logger::instance().defaultLogger().info(ss.str());
        }

        freeTasks(completedTasks);

        // update metrics
        metrics.setDiskSpillStatistics(tstage->number(),
                                       Partition::statisticSwapInCount(),
                                       Partition::statisticSwapInBytesRead(),
                                       Partition::statisticSwapOutCount(),
                                       Partition::statisticSwapOutBytesWritten());

        // info how long the trafo stage took
        std::stringstream ss;
        ss<<"[Transform Stage] Stage "<<tstage->number()<<" took "<<stageTimer.time()<<"s";
        Logger::instance().defaultLogger().info(ss.str());
    }

    std::vector<IExecutorTask*> LocalBackend::resolveViaSlowPath(
            std::vector<IExecutorTask*> &tasks,
            bool merge_rows_in_order,
            codegen::resolve_f functor, tuplex::TransformStage *tstage, bool combineHashmaps) {

        using namespace std;
        assert(tstage);

        HashTableSink hsink;
        bool hasNormalHashSink = false;

        // make sure output mode is NOT hash table, not yet supported...
        if(tstage->outputMode() == EndPointMode::HASHTABLE) {
            // note: must hold that normal-case output type is equal to general-case output type
            assert(tstage->normalCaseOutputSchema() == tstage->outputSchema()); // must hold for hash table!

            // special case: create a global hash output result and put it into the FIRST resolve task.
            Timer timer;
            hsink = createFinalHashmap({tasks.cbegin(), tasks.cend()}, tstage->hashtableKeyByteWidth(), combineHashmaps);
            logger().info("created combined normal-case result in " + std::to_string(timer.time()) + "s");
            hasNormalHashSink = true;
        }

        Timer timer;
        // compile & prep python pipeline for this stage
        auto pip_object = preparePythonPipeline(tstage->purePythonCode(), tstage->pythonPipelineName());
        // transform dependents into python format!
        // when pip_object is nullptr, then something went wrong...
        if(!pip_object) {
            logger().error("python pipeline invalid, details: \n"
            + core::withLineNumbers(tstage->purePythonCode()));
            return tasks;
        }

        logger().info("compiled pure python pipeline in " + std::to_string(timer.time()) + "s");
        timer.reset();

        // fetch intermediates of previous stages (i.e. hash tables)
        // @TODO rewrite for general intermediates??
        auto input_intermediates = tstage->initData();

        // lazy init hybrids
        if(!input_intermediates.hybrids) {
            auto num_predecessors = tstage->predecessors().size();
            input_intermediates.hybrids = new PyObject*[num_predecessors];
            for(int i = 0; i < num_predecessors; ++i)
                input_intermediates.hybrids[i] = nullptr;
        }


        python::lockGIL();

        // construct intermediates from predecessors
        size_t num_predecessors = tstage->predecessors().size();
        for(unsigned i = 0; i < tstage->predecessors().size(); ++i) {
            auto pred = tstage->predecessors()[i];
            if(pred->outputMode() == EndPointMode::HASHTABLE) {
                auto ts = dynamic_cast<TransformStage*>(pred); assert(ts);
#ifndef NDEBUG
                // print out key information about hashresults...
                // cout<<"predecessor has output mode hash table"<<endl;
                // cout<<"key type: "<<ts->hashResult().keyType.desc()<<endl;
                // cout<<"bucket type: "<<ts->hashResult().bucketType.desc()<<endl;
#endif
                // create hybrid if not existing (could be if previous stage had output hashtable + slow resolve path!)
                if(input_intermediates.hash_maps[i] && !input_intermediates.hybrids[i]) {
#warning "fix this code after OSS, it's a memory bug."
                    HashTableSink* hs = new HashTableSink(); // memory leak...
                    hs->hm = input_intermediates.hash_maps[i];
                    hs->null_bucket = input_intermediates.null_buckets[i];
                    input_intermediates.hybrids[i] = reinterpret_cast<PyObject *>(CreatePythonHashMapWrapper(*hs,
                                                                                                             ts->hashResult().keyType.withoutOptions(),
                                                                                                             ts->hashResult().bucketType));
                }
            }
        }
        python::unlockGIL();

        // check whether hybrids exist. If not, create them quickly!
        assert(input_intermediates.hybrids);
        logger().info("creating hybrid intermediates took " + std::to_string(timer.time()) + "s");
        timer.reset();

        bool hashOutput = tstage->outputMode() == EndPointMode::HASHTABLE;

        std::vector<IExecutorTask*> tasks_result;
        std::vector<IExecutorTask*> resolveTasks;
        std::vector<size_t> maxOrder;

        auto opsToCheck = tstage->operatorIDsWithResolvers();

        auto outFormat = tstage->outputFormat();
        auto csvDelimiter = tstage->csvOutputDelimiter();
        auto csvQuotechar = tstage->csvOutputQuotechar();

        auto stageID = tstage->getID();

        auto compiledSlowPathOutputSchema = tstage->outputSchema();
        auto allowNumericTypeUnification = _options.AUTO_UPCAST_NUMBERS();
        auto targetNormalCaseOutputSchema = tstage->normalCaseOutputSchema();
        auto targetGeneralCaseOutputSchema = tstage->outputSchema();

        // should cases not get persisted separately?
        if(!tstage->persistSeparateCases())
            targetNormalCaseOutputSchema = targetGeneralCaseOutputSchema; // both are the same!

        for(const auto& task : tasks) {
            auto tt = dynamic_cast<TransformTask *>(task);

            // debug printing for info on resolve tasks
            std::stringstream ss;
            ss<<"Resolve tasks for stage "<<stageID<<endl;
            ss<<"\ttarget normal case output schema: "<<tstage->normalCaseOutputSchema().getRowType().desc()<<endl;
            ss<<"\ttarget general case output schema: "<<tstage->outputSchema().getRowType().desc()<<endl;
            ss<<"\tshould cases be persisted separately: "<<std::boolalpha<<tstage->persistSeparateCases()<<endl;
            ss<<"\tschema the regular exceptions are stored in: "<<tt->inputSchema().getRowType().desc()<<endl;
            ss<<"\tschema the compiled slow path outputs: "<<tstage->outputSchema().getRowType().desc()<<endl;

            if(tt == tasks.front())
                logger().debug(ss.str());

            if (!tt)
                throw std::runtime_error("FATAL ERROR: unknown task encountered");

            if(maxOrder.empty())
                maxOrder = tt->getOrder();
            else if(compareOrders(maxOrder, tt->getOrder()))
                maxOrder = tt->getOrder();

            if (tt->exceptionCounts().size() > 0 || tt->inputExceptionInfo().numExceptions > 0) {
                // task found with exceptions in it => exception partitions need to be resolved using special functor

                // hash-table output not yet supported
                if(tstage->outputMode() == EndPointMode::HASHTABLE)
                    assert(hashOutput);

                assert(tt->getStageID() == stageID);

                // this task needs to be resolved, b.c. exceptions occurred...
                // pretty simple, just create a ResolveTask
                auto exceptionInputSchema = tt->inputSchema(); // this could be specialized!
                auto rtask = new ResolveTask(stageID,
                                             tstage->context().id(),
                                             tt->getOutputPartitions(),
                                             tt->getExceptionPartitions(),
                                             tt->inputExceptions(),
                                             tt->inputExceptionInfo(),
                                             opsToCheck,
                                             exceptionInputSchema,
                                             compiledSlowPathOutputSchema,
                                             targetNormalCaseOutputSchema,
                                             targetGeneralCaseOutputSchema,
                                             merge_rows_in_order,
                                             allowNumericTypeUnification,
                                             outFormat,
                                             csvDelimiter,
                                             csvQuotechar,
                                             functor,
                                             pip_object);

                rtask->setOrder(tt->getOrder()); // copy order from original task for sorting later!

                // // debug: print out keys from first hash table!
                // if(tstage->predecessors().size() > 0) {
                //     auto hm = input_intermediates.hash_maps[0];
                //
                //     hashmap_iterator_t iterator = 0;
                //     const char *key = nullptr;
                //     uint64_t keylen = 0;
                //     std::cout<<"C++ hashtable:\n====\n"<<std::endl;
                //     while((key = hashmap_get_next_key(hm, &iterator, &keylen)) != nullptr) {
                //         std::cout<<"Hash table contains key: '"<<key<<"'"<<std::endl;
                //     }
                //
                //     auto hybrid_hm = input_intermediates.hybrids[0];
                //     // check: hybrid hash table!
                //     python::lockGIL();
                //     PyObject *py_key = nullptr, *py_val = nullptr;
                //     Py_ssize_t pos = 0;  // must be initialized to 0 to start iteration, however internal iterator variable. Don't use semantically.
                //     while(PyDict_Next(hybrid_hm, &pos, &py_key, &py_val)) {
                //         std::cout<<"Py/C++ hashtable contains: "<<python::PyString_AsString(py_key)<<std::endl;
                //     }
                //     python::unlockGIL();
                // }

                // to implement, store i.e. tables within tasks...
                rtask->setHybridIntermediateHashTables(tstage->predecessors().size(), input_intermediates.hybrids);

                // hash output?
                if(hashOutput) {
                    // this should be ONLY true if the last operator is a join or unique?
                    // => the other aggregates are handled differently...
                    // note: currently there's only the unique supported, because need a "extractKeyFunc" for the
                    //       aggregateByKey version.

                    // is it the first task? If so, set the current combined result!
                    if(hasNormalHashSink) {
                        rtask->sinkOutputToHashTable(tt->hashTableFormat(), tstage->dataAggregationMode(), tstage->hashOutputKeyType().withoutOptions(), tstage->hashOutputBucketType(), hsink.hm, hsink.null_bucket);
                        hasNormalHashSink = false;
                    } else {
                        rtask->sinkOutputToHashTable(tt->hashTableFormat(), tstage->dataAggregationMode(), tstage->hashOutputKeyType().withoutOptions(), tstage->hashOutputBucketType());
                    }
                }
#ifndef NDEBUG
                {
                    int normal_rows = 0;
                    int exception_rows = 0;
                    for(auto p : tt->getOutputPartitions())
                        normal_rows += p->getNumRows();
                    for(auto p : tt->getExceptionPartitions())
                        exception_rows += p->getNumRows();
                    // debug output
                    logger().debug("Creating new resolve task with " + std::to_string(normal_rows)  + " normal rows and " + std::to_string(exception_rows) + " exception rows.");
                }
#endif
                resolveTasks.push_back(rtask);

                // TODO: delete original task...
                delete tt;
                tt = nullptr;
            } else {
                // just append to output
                tasks_result.push_back(task);
            }
        }

        logger().info("Created " + pluralize(resolveTasks.size(), "resolve task") + " in " + std::to_string(timer.time()) + "s");
        logger().info(std::to_string(resolveTasks.size()) + "/" + pluralize(tasks.size(), "task") + " require executing the slow path.");
        timer.reset();

        // add all resolved tasks to the result
        // cout<<"*** need to compute "<<resolveTasks.size()<<" resolve tasks ***"<<endl;
        auto resolvedTasks = performTasks(resolveTasks);
        // cout<<"*** git "<<resolvedTasks.size()<<" resolve tasks ***"<<endl;
        std::copy(resolvedTasks.cbegin(), resolvedTasks.cend(), std::back_inserter(tasks_result));

        // Invalidate partitions after all resolve tasks execute because shared among tasks
        for (auto& p : tstage->inputExceptions()) {
            p->invalidate();
        }

        // cout<<"*** total number of tasks to return is "<<tasks_result.size()<<endl;
        return tasks_result;
    }

    std::vector<IExecutorTask*> LocalBackend::performTasks(std::vector<IExecutorTask*> &tasks, std::function<void()> driverCallback) {
        // perform tasks in main memory
        // start workqueue
        WorkQueue& wq = LocalEngine::instance().getQueue();
        wq.clear();

        // check if ord is set, if not issue warning & add
        bool orderlessTaskFound = false;
        for(int i = 0; i < tasks.size(); ++i) {
            if(tasks[i]->getOrder().size() == 0) {
                tasks[i]->setOrder(i);
                orderlessTaskFound = true;
            }
        }

#ifndef NDEBUG
        if(orderlessTaskFound) {
            logger().debug("task without order found, please fix in code.");
        }
#endif

        // add all tasks to queue
        for(auto& task : tasks) wq.addTask(task);
        // clear
        tasks.clear();

        // attach executors
        for(auto& exec : _executors) {
            // set for each executor the history server & jobID so they can send out updates
            exec->setHistoryServer(_historyServer.get()); // ! important to call this before attach work queue !
        }

        for(auto& exec : _executors)
            exec->attachWorkQueue(&wq);

        // sometimes driver can do some work in parallel, so do this first before driver joins workqueue
        driverCallback();

        // Let all the threads do their work & also work on the driver!
        bool flushToPython = _options.REDIRECT_TO_PYTHON_LOGGING();
        wq.workUntilAllTasksFinished(*driver(), flushToPython);

        // release here runtime memory...
        runtime::rtfree_all();

        // remove executors from current queue
        for(auto& exec : _executors)
            exec->removeFromQueue();

        // reset history server
        for(auto& exec : _executors)
            exec->setHistoryServer(nullptr);

        // fetch tasks from queue
        return wq.popCompletedTasks();
    }

    /*!
     * get default file extension for supported file formats
     * @param fmt
     * @return string
     */
    std::string fileFormatDefaultExtension(FileFormat fmt) {
        switch (fmt) {
            case FileFormat::OUTFMT_TEXT:
                return ".txt";
            case FileFormat::OUTFMT_TUPLEX:
                return ".bin";
            case FileFormat::OUTFMT_CSV:
                return ".csv";
            case FileFormat::OUTFMT_ORC:
                return ".orc";
            default:
                throw std::runtime_error("file format: " + std::to_string((int) fmt) + " not yet supported!");
        }
    }

    /*!
     * construct output path based either on a base URI or via a udf
     * @param udf
     * @param baseURI
     * @param partNo
     * @return
     */
    URI outputURI(const UDF& udf, const URI& baseURI, int64_t partNo, FileFormat fmt) {

        // check if UDF is valid, if so pass it partNo and get filename back
        if(udf.getCode().length() > 0) {
            // get GIL, execute UDF!
            throw std::runtime_error("udf output uri not yet supported...");

        } else {
            if(baseURI == URI::INVALID)
                throw std::runtime_error("accessing invalid URI!");
            std::string base; // base which to use to form string
            std::string ext; // base

            auto path = baseURI.toPath();
            auto ext_pos = path.rfind('.'); // searches for extension
            auto slash_pos = path.rfind('/');

            // two cases: 1) extension found 2.) extension not found
            bool isFolder = (ext_pos == std::string::npos) ||
                    (path.back() == '/') || ((slash_pos != std::string::npos) && (ext_pos < slash_pos));

            if(isFolder) {
                // --> call ensureOutputFolderExists before using this function here!

                // change to correct file format extension
                return URI(path + "/part" + std::to_string(partNo) + fileFormatDefaultExtension(fmt));
            } else {
                base = path.substr(0, ext_pos);
                ext = path.substr(ext_pos + 1);

                // return new URI with part number
                return URI(base + ".part" + std::to_string(partNo) + "." + ext);
            }
        }
    }

    std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> LocalBackend::calcExceptionCounts(
            const std::vector<IExecutorTask*> &tasks) {

        using namespace std;

        unordered_map<tuple<int64_t, ExceptionCode>, size_t> ecounts;
        for(const auto& task : tasks) {

            // i.e. resolve task is an exceptionable one...
            auto etask = dynamic_cast<IExceptionableTask*>(task);
            if(etask)
                ecounts = merge_ecounts(ecounts, etask->exceptionCounts());

            // special case, Trafo task!
            auto ttask = dynamic_cast<TransformTask*>(task);
            if(ttask)
                ecounts = merge_ecounts(ecounts, ttask->exceptionCounts());
        }

        return ecounts;
    }

    void LocalBackend::executeAggregateStage(tuplex::AggregateStage *astage) {
        using namespace std;

        assert(astage);

        auto rs = astage->predecessors()[0]->resultSet(); assert(rs);
        auto rowType = rs->schema().getRowType();

        // hacked together for 311...
        int numBitmapElements = codegen::calcBitmapElementCount(rowType); // can be 0, 1, 2, ... for 0-64, 65-... nullables...
        // the hashmap
        auto hmap = hashmap_new();

        std::set<std::string> uniqueSet;

        Timer timer;

        bool nullFound = false;

        // first a dummy implementation:
        // basically hash the complete row (can be done faster later) into a hashmap and then write back the result...
        while(rs->hasNextPartition()) {
            Partition* p = rs->getNextPartition();

            // lock partition!
            auto ptr = p->lockRaw();
            int64_t numRows = *((int64_t*)ptr);
            ptr += sizeof(int64_t);

           logger().info("processing " + std::to_string(numRows) + " rows for unique aggregate...");

            for(auto i = 0; i < numRows; ++i) {
                // grab key (or later key UDF) and hash it
                // check what type of key it is and form appropriate hash
                // ==> fetch row length
                Deserializer ds(Schema(Schema::MemoryLayout::ROW, rowType));
                size_t rowLength = ds.inferLength(ptr);

                int64_t bitmap = 0;
                int rightKeyBitmapElementPos = 0;
                if(numBitmapElements > 0)
                    bitmap = *(((int64_t*)ptr) + rightKeyBitmapElementPos);

                if(bitmap & 0x1) {
                    nullFound = true;
                } else {
                    // it's a string here
                    int64_t info = *( ((int64_t*)ptr) + numBitmapElements);

                    // construct offset & fetch key...
                    // get offset
                    int64_t offset = info;
                    // offset is in the lower 32bit, the upper are the size of the var entry
                    int64_t size = ((offset & (0xFFFFFFFFl << 32)) >> 32);

                    assert(size >= 1); // strings are zero terminated so size should >= 1!
                    offset = offset & 0xFFFFFFFF;

                    // data is ptr + offset
                    char* str = (char*)(ptr + offset + (numBitmapElements) * sizeof(int64_t));
                    assert(strlen(str) == size - 1);

                    string s(str);

                    uniqueSet.insert(s);
                }

                ptr += rowLength;
            }

            p->unlock();
            p->invalidate();
        }

        // write output to one or more partitions...
        // include null if found

        // schema is option[str] for this query...
        PartitionWriter pw(driver(), rs->schema(), astage->outputDataSetID(),
                           astage->context().id(), _options.PARTITION_SIZE());


        // bug in here, leave out...
        //if(nullFound)
        //    pw.writeRow(Row(option<std::string>::none));
        for(auto el : uniqueSet) {
            pw.writeRow(Row(option<std::string>(el)));
        }

        stringstream ss;
        ss<<"finished aggregate stage "<<astage->number()<<" in "<<timer.time()<<"s";
        logger().info(ss.str());
        ss.str("");
        ss<<"Aggregate stage "<<astage->number()<<" yielded "<<uniqueSet.size()+nullFound<<" unique rows";
        logger().info(ss.str());
        astage->setResultSet(std::make_shared<ResultSet>(rs->schema(), pw.getOutputPartitions(true)));
    }

    // merge buckets and delete them then...
    uint8_t* merge_buckets(uint8_t* bucketA, uint8_t* bucketB) {
        // if one is null, just return the other
        if(!bucketA && !bucketB)
            return nullptr;
        if(bucketA && !bucketB)
            return bucketA;
        if(!bucketA && bucketB)
            return bucketB;

        // both are valid
        assert(bucketA && bucketB);
        assert(bucketA != bucketB);


        // extract size, num rows etc. and merge
        uint64_t infoA = *(uint64_t*)bucketA;
        auto bucket_size_A = infoA & 0xFFFFFFFF;
        auto num_elements_A = (infoA >> 32ul);
        uint64_t infoB = *(uint64_t*)bucketB;
        auto bucket_size_B = infoB & 0xFFFFFFFF;
        auto num_elements_B = (infoB >> 32ul);

        // -8 bytes to not double count info
        auto bucket_size = bucket_size_A + bucket_size_B - sizeof(int64_t);
        auto num_elements = num_elements_A + num_elements_B;
        uint64_t info = (num_elements << 32ul) | bucket_size;

        // realloc and copy contents to end of bucket...
        auto bucket = (uint8_t*)malloc(bucket_size);
        *(uint64_t*)bucket = info;

        // copy bucketA contents
        memcpy(bucket + sizeof(int64_t), bucketA + sizeof(int64_t), bucket_size_A - sizeof(int64_t));
        // copy bucketB contents
        memcpy(bucket + sizeof(int64_t) + bucket_size_A - sizeof(int64_t), bucketB + sizeof(int64_t), bucket_size_B - sizeof(int64_t));
        free(bucketA);
        free(bucketB);
        bucketA = nullptr;
        bucketB = nullptr;
        return bucket;
    }

    // /*
    // * Iterate the function parameter over each element in the hashmap.  The
    // * additional any_t argument is passed to the function as its first
    // * argument and the hashmap element is the second.
    // */
    //int hashmap_iterate(map_t in, PFany f, any_t item) {
    //    int i;
    //
    //    /* Cast the hashmap */
    //    hashmap_map *m = (hashmap_map *) in;
    //
    //    /* On empty hashmap, return immediately */
    //    if (hashmap_length(m) <= 0)
    //        return MAP_MISSING;
    //
    //    /* Linear probing */
    //    for (i = 0; i < m->table_size; i++)
    //        if (m->data[i].in_use != 0) {
    //            any_t data = (any_t) (m->data[i].data);
    //            int status = f(item, data);
    //            if (status != MAP_OK) {
    //                return status;
    //            }
    //        }
    //
    //    return MAP_OK;
    //}

    // call this func in iterate
    static int rehash_bucket(map_t hm, hashmap_element* entry) {
        assert(hm);
        auto key = entry->key;
        auto keylen = entry->keylen;
        auto data = (uint8_t*)entry->data;
        // data is a bucket. Check in combined hashmap hm
        uint8_t* bucket = nullptr;
        hashmap_get(hm, key, keylen, reinterpret_cast<any_t*>(&bucket));
        bucket = merge_buckets(bucket, data);
        hashmap_put(hm, key, keylen, bucket);
        // @TODO: there might be a memory leak for the keys...
        // => anyways need to rewrite this slow hashmap...
        return MAP_OK;
    }

    static int combine_bucket(map_t hm, hashmap_element* entry) {
        assert(hm);
        auto key = entry->key;
        auto keylen = entry->keylen;
        auto data = (uint8_t*)entry->data;
        // data is a bucket. Check in combined hashmap hm
        uint8_t* bucket = nullptr;
        hashmap_get(hm, key, keylen, reinterpret_cast<any_t*>(&bucket));
        bucket = combineBuckets(bucket, data);
        hashmap_put(hm, key, keylen, bucket);
        // @TODO: there might be a memory leak for the keys...
        // => anyways need to rewrite this slow hashmap...
        return MAP_OK;
    }

    static int int64_rehash_bucket(map_t hm, int64_hashmap_element* entry) {
        assert(hm);
        auto key = entry->key;
        auto data = (uint8_t*)entry->data;
        // data is a bucket. Check in combined hashmap hm
        uint8_t* bucket = nullptr;
        int64_hashmap_get(hm, key, reinterpret_cast<any_t*>(&bucket));
        bucket = merge_buckets(bucket, data);
        int64_hashmap_put(hm, key, bucket);
        return MAP_OK;
    }

    static int int64_combine_bucket(map_t hm, int64_hashmap_element* entry) {
        assert(hm);
        auto key = entry->key;
        auto data = (uint8_t*)entry->data;
        // data is a bucket. Check in combined hashmap hm
        uint8_t* bucket = nullptr;
        int64_hashmap_get(hm, key, reinterpret_cast<any_t*>(&bucket));
        bucket = combineBuckets(bucket, data);
        int64_hashmap_put(hm, key, bucket);
        return MAP_OK;
    }


    HashTableSink getHashSink(const IExecutorTask* exec_task) {
        if(!exec_task)
            return HashTableSink();

        switch(exec_task->type()) {
            case TaskType::UDFTRAFOTASK: {
                auto task = dynamic_cast<const TransformTask*>(exec_task); assert(task);
                return task->hashTableSink();
            }
            case TaskType::RESOLVE: {
                auto task = dynamic_cast<const ResolveTask*>(exec_task); assert(task);
                return task->hashTableSink();
            }
            default:
                throw std::runtime_error("unknown task type" + FLINESTR);
        }
    }

    HashTableSink LocalBackend::createFinalHashmap(const std::vector<const IExecutorTask*>& tasks, int hashtableKeyByteWidth, bool combine) {
        if(tasks.empty()) {
            HashTableSink sink;
            if(hashtableKeyByteWidth == 8) sink.hm = int64_hashmap_new();
            else sink.hm = hashmap_new();
            sink.null_bucket = nullptr;
            return sink;
        } else if(1 == tasks.size()) {
            // no merge necessary, just directly return result
            // fetch hash table from task
            assert(tasks.front()->type() == TaskType::UDFTRAFOTASK || tasks.front()->type() == TaskType::RESOLVE);
            return getHashSink(tasks.front());
        } else {

            // @TODO: getHashSink should be updated to also work with hybrids. Yet, the merging of normal hashtables
            //        with resolve hashtables is done by simply setting the merged normal result as input to the first resolve task.

            // need to merge.
            // => fetch hash table form first
            auto sink = getHashSink(tasks.front());

            // init hashmap
            if(!sink.hm) {
                if (hashtableKeyByteWidth == 8) sink.hm = int64_hashmap_new();
                else sink.hm = hashmap_new();
            }

            // merge in null bucket + other buckets from other tables (this could be slow...)
            for(int i = 1; i < tasks.size(); ++i) {
                auto task_sink = getHashSink(tasks[i]);
                if(combine) combineBuckets(sink.null_bucket, task_sink.null_bucket);
                else sink.null_bucket = merge_buckets(sink.null_bucket, task_sink.null_bucket);

                // fetch all buckets in hashmap & place into new hashmap
                if(task_sink.hm) {
                    if(combine) {
                        if(hashtableKeyByteWidth == 8) int64_hashmap_iterate(task_sink.hm, int64_combine_bucket, sink.hm);
                        else hashmap_iterate(task_sink.hm, combine_bucket, sink.hm);
                    }
                    else {
                        if(hashtableKeyByteWidth == 8) int64_hashmap_iterate(task_sink.hm, int64_rehash_bucket, sink.hm);
                        else hashmap_iterate(task_sink.hm, rehash_bucket, sink.hm);
                    }
                }

                if(hashtableKeyByteWidth == 8)
                    int64_hashmap_free(task_sink.hm); // remove hashmap (keys and buckets already handled)
                else
                    hashmap_free(task_sink.hm); // remove hashmap (keys and buckets already handled)
            }
            return sink;
        }
    }

    void LocalBackend::writeOutput(TransformStage *tstage, std::vector<IExecutorTask*> &tasks) {
        using namespace std;

        Timer timer;
        // check output format to be supported
        assert(tstage->outputMode() == EndPointMode::FILE);

        // now simply go over the partitions and write the full buffers out
        // check all the params from TrafoStage
        size_t limit = tstage->outputLimit();
        size_t splitSize = tstage->splitSize();
        size_t numOutputFiles = tstage->numOutputFiles();
        URI uri = tstage->outputURI();
        UDF udf = tstage->outputPathUDF();
        auto fmt = tstage->outputFormat();

        // count number of output rows in tasks
        size_t numTotalOutputRows = 0;
        vector<Partition *> outputs; // collect all output partitions in this vector
        for (const auto &task : tasks) {
            numTotalOutputRows += task->getNumOutputRows();
            auto partitions = task->getOutputPartitions();
            outputs.insert(outputs.end(), partitions.begin(), partitions.end());
        }

        auto ecounts = calcExceptionCounts(tasks);

        // write to one file
        int partNo = 0;
        auto outputFilePath = outputURI(udf, uri, partNo, fmt);

        // check that outputFilePath is NOT empty.

        auto outFile = VirtualFileSystem::open_file(outputFilePath, VirtualFileMode::VFS_WRITE);
        if (!outFile)
            throw std::runtime_error("could not open " + outputFilePath.toPath() + " in write mode.");

        // check how many bytes need to be written to disk
        size_t totalRows = 0;
        size_t totalBytes = 0;
        for (auto p : outputs) {
            totalRows += p->getNumRows();
            totalBytes += p->bytesWritten();
        }

        stringstream ss;
        ss << "Writing " << pluralize(totalRows, "row") << " as output to file (" << sizeToMemString(totalBytes) << ")";
        Logger::instance().defaultLogger().info(ss.str());

        // create CSV header if desired
        uint8_t *header = nullptr;
        size_t header_length = 0;

        // write header if desired...
        auto outOptions = tstage->outputOptions();
        bool writeHeader = stringToBool(get_or(outOptions, "header", "false"));
        if(writeHeader) {
            // fetch special var csvHeader
            auto headerLine = outOptions["csvHeader"];
            header_length = headerLine.length();
            header = new uint8_t[header_length+1];
            memset(header, 0, header_length +1);
            memcpy(header, (uint8_t *)headerLine.c_str(), header_length);
        }

        // create write tasks (evenly distribute partitions over executors)
        auto numExecutors = 1 + _options.EXECUTOR_COUNT();
        size_t bytesPerExecutor = totalBytes / numExecutors;
        vector<Partition*> partitions;
        vector<IExecutorTask*> wtasks;
        size_t bytesInList = 0;
        for(const auto& p : outputs) {
            partitions.push_back(p);
            bytesInList += p->bytesWritten();

            if(bytesInList >= bytesPerExecutor) {
                // spawn task
                // const URI& uri, uint8_t *header, size_t header_length, const std::vector<Partition *> &partitions
                IExecutorTask* wtask;
                switch(tstage->outputFormat()) {
                    case FileFormat::OUTFMT_CSV:
                        wtask = new SimpleFileWriteTask(outputURI(udf, uri, partNo++, fmt), header, header_length, partitions);
                        break;
                    case FileFormat::OUTFMT_ORC:
#ifdef BUILD_WITH_ORC
                        wtask = new SimpleOrcWriteTask(outputURI(udf, uri, partNo++, fmt), partitions, tstage->outputSchema(), outOptions["columnNames"]);
#else
                        throw std::runtime_error(MISSING_ORC_MESSAGE);
#endif
                        break;
                    default:
                        throw std::runtime_error("file output format not supported.");
                }
                wtasks.emplace_back(wtask);
                partitions.clear();
                bytesInList = 0;
            }
        }
        // add last task (remaining partitions)
        if(!partitions.empty()) {
            IExecutorTask* wtask;
            switch (tstage->outputFormat()) {
                case FileFormat::OUTFMT_CSV: {
                    wtask = new SimpleFileWriteTask(outputURI(udf, uri, partNo++, fmt), header, header_length, partitions);
                    break;
                }
                case FileFormat::OUTFMT_ORC: {
#ifdef BUILD_WITH_ORC
                    wtask = new SimpleOrcWriteTask(outputURI(udf, uri, partNo++, fmt), partitions, tstage->outputSchema(), outOptions["columnNames"]);
#else
                    throw std::runtime_error(MISSING_ORC_MESSAGE);
#endif
                    break;
                }
                default:
                    throw std::runtime_error("file output format not supported.");
            }
            wtasks.emplace_back(wtask);
            partitions.clear();
        }

        // perform tasks
        // run using queue!
        // execute tasks using work queue.
        auto completedTasks = performTasks(wtasks);

        if(header) {
            delete [] header;
            header = nullptr;
        }

        Logger::instance().defaultLogger().info("writing output took " + std::to_string(timer.time()) + "s");
        tstage->setFileResult(ecounts);
    }
} // namespace tuplex