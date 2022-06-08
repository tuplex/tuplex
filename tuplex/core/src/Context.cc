//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/LogicalOperator.h>
#include <logical/ParallelizeOperator.h>
#include <logical/FileInputOperator.h>
#include "ErrorDataSet.h"
#include "EmptyDataset.h"
#include <jit/JITCompiler.h>
#include <jit/RuntimeInterface.h>
#include <VirtualFileSystem.h>
#include <ee/local/LocalBackend.h>
#include <utils/Signals.h>
#ifdef BUILD_WITH_AWS
#include <ee/aws/AWSLambdaBackend.h>
#endif
#include <Context.h>
namespace tuplex {

    int Context::_contextIDGenerator = 10000;

    Context::Context(const ContextOptions& options) : _datasetIDGenerator(0), _compilePolicy(compilePolicyFromOptions(options)), _id(getNextContextID()) {
        // init metrics
        _lastJobMetrics = std::make_unique<JobMetrics>();
        // make sure this is called without holding the GIL
        if(python::isInterpreterRunning())
            assert(!python::holdsGIL());

        auto& logger = Logger::instance().logger("core");

        _uuid = getUniqueID();
        _name = "context-" + uuid().substr(0, 8);

        // change this to be context dependent....
        // i.e. if a context requests this much memory, than add on top of the memory manager!
        // ==> free blocks after that on context destruction...
        _options = options;

#ifdef BUILD_WITH_AWS
        // init AWS SDK to get access to S3 filesystem
        auto aws_credentials = AWSCredentials::get();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
#endif

        // start backend depending on options
        switch(options.BACKEND()) {
            case Backend::LOCAL: {
                // creates a new local backend! --> maybe reuse for multiple contexts?
                _ee = std::make_unique<LocalBackend>(*this);
                break;
            }
            case Backend::LAMBDA: {
#ifndef BUILD_WITH_AWS
                throw std::runtime_error("Build Tuplex with -DBUILD_WITH_AWS to enable the AWS Lambda backend");
#else
                // warn if credentials are not found.
                if(!aws_init_rc) {
                    if(aws_credentials.access_key.empty() || aws_credentials.secret_key.empty())
                        throw std::runtime_error("To use Tuplex Lambda backend, please specify valid AWS credentials."
                                                 " E.g., run aws configure or add two environment variables"
                                                 " AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID");
                    else
                        throw std::runtime_error("Requesting Tuplex Lambda backend, but initialization failed.");
                }

                // @TODO: function name should come from options!
                _ee = std::make_unique<AwsLambdaBackend>(*this, AWSCredentials::get(), "tuplex-lambda-runner");
#endif
                break;
            }
            default: {
                throw std::runtime_error("unknown backend encountered. Supported so far are only local or lambda.");
                break;
            }
        }
    }

    // destructor needs to free memory of datasets!
    Context::~Context() {
        using namespace std;

        if(!_datasets.empty())
            for(DataSet* ptr : _datasets) {
                if(ptr)
                    delete ptr;
                ptr = nullptr;
            }

        // free logical operators associated with context
        _operators.clear();
    }

    Partition* Context::requestNewPartition(const Schema &schema, const int dataSetID, size_t minBytesRequired) {
        if(!_ee)
            throw std::runtime_error("no backend initialized");
        auto driver = _ee->driver();
        if(!driver)
            throw std::runtime_error("driver not initialized for backend");

        size_t bytes_to_alloc = std::max(minBytesRequired + sizeof(int64_t), _options.PARTITION_SIZE());
        return driver->allocWritablePartition(bytes_to_alloc, schema, dataSetID, id());
    }

    DataSet* Context::createDataSet(const Schema& schema) {

        int id = getNextDataSetID();

        // transfer data => do this later lazily, i.e. when graph is executed
        // write out in column order
        DataSet *dsptr = new DataSet();
        dsptr->_context = this;
        dsptr->_schema = schema;
        dsptr->_id = id;

        // transfer ptr management to context
        _datasets.push_back(dsptr);
        return dsptr;
    }


    DataSet& Context::makeError(const std::string &error) {
        // add a new error dataset to this context
        DataSet *es = new ErrorDataSet(error);
        assert(es);
        es->_context = this;

        _datasets.push_back(es);

        return *es;
    }

    DataSet& Context::makeEmpty() {
        // add a new error dataset to this context
        DataSet *es = new EmptyDataset();
        assert(es);
        es->_context = this;

        _datasets.push_back(es);

        return *es;
    }

    void Context::addPartition(DataSet *ds, Partition *partition) {
        assert(ds);
        assert(partition);
        partition->setDataSetID(ds->getID());
        ds->_partitions.push_back(partition);
    }

    std::string node_descriptor(LogicalOperator* node) {
        std::string s = node->name();
        if(node->getDataSet())
            s += "(id: " + std::to_string(node->getDataSet()->getID()) + ")";
        return s;
    }

    void Context::visualizeOperationGraph(GraphVizBuilder& builder) {
        // go through all operators
        std::map<LogicalOperator*, bool> visited;
        std::map<LogicalOperator*, int> graphIDs;
        for(const auto& el : _operators)
            visited[el.get()] = false;

        for(const auto& node : _operators) {
            if(!visited[node.get()]) {
                int id = -1;
                if(graphIDs.find(node.get()) == graphIDs.end()) {
                    id = builder.addHTMLNode(node_descriptor(node.get()));
                    graphIDs[node.get()] = id;
                } else {
                    id = graphIDs[node.get()];
                }

                // go through children
                for(const auto& c : node->children()) {
                    int cid = -1;
                    if(graphIDs.find(c.get()) == graphIDs.end()) {
                        cid = builder.addHTMLNode(node_descriptor(c.get()));
                        graphIDs[c.get()] = cid;
                    } else {
                        cid = graphIDs[node.get()];
                    }

                    builder.addEdge(id, cid);
                }
            }
        }
    }

    DataSet& Context::fromPartitions(const Schema& schema,
                                     const std::vector<Partition*>& partitions,
                                     const std::vector<std::string>& columns,
                                     const std::vector<std::tuple<size_t, PyObject*>> &badParallelizeObjects,
                                     const std::vector<size_t> &numExceptionsInPartition,
                                     const SamplingMode& sampling_mode) {
        auto dataSetID = getNextDataSetID();
        DataSet *dsptr = createDataSet(schema);

        assert(!(schema == Schema::UNKNOWN));
        assert(dsptr);

        dsptr->_schema = schema;

        // empty?
        if(partitions.empty()) {
            dsptr->setColumns(columns);
            addParallelizeNode(dsptr, badParallelizeObjects, numExceptionsInPartition, sampling_mode);
            return *dsptr;
        } else {
            size_t numRows = 0;

            for(Partition* partition : partitions) {
                assert(partition);
                // make sure schema matches
                assert(partition->schema() == schema);

                numRows += partition->getNumRows();
                addPartition(dsptr, partition);
            }

            // set rows
            dsptr->setColumns(columns);
            addParallelizeNode(dsptr, badParallelizeObjects, numExceptionsInPartition, sampling_mode);

            // signal check
            if(check_and_forward_signals()) {
#ifndef NDEBUG
                Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
                return makeError("job aborted (signal received)");
            }
            return *dsptr;
        }
    }

    DataSet& Context::parallelize(const std::vector<Row>& rows,
                                  const std::vector<std::string>& columnNames,
                                  const SamplingMode& sampling_mode) {

        Schema schema;
        int dataSetID = getNextDataSetID();
        DataSet *dsptr = createDataSet(schema);

        if(rows.empty()) {
            // parallelizing empty dataset...
            // just return what has been initialized so far
            dsptr->setColumns(columnNames);
            addParallelizeNode(dsptr, {}, {}, sampling_mode);
            return *dsptr;
        } else {
            // get row type from first element @TODO: should be inferred from sample, no?
            auto rtype = rows.front().getRowType();
            schema = Schema(Schema::MemoryLayout::ROW, rtype);
            dsptr->_schema = schema;
            int numRows = rows.size();

            size_t minBytesRequired = rows.front().serializedLength();

            int numPartitions = 0;
            Partition *partition = requestNewPartition(schema, dataSetID, minBytesRequired);
            numPartitions++;
            int numWrittenRowsInPartition = 0;
            if(!partition)
                return makeError("no memory left to hold data in driver memory");

            uint8_t* base_ptr = (uint8_t*)partition->lock();

            int bytesPerPartitionTransferred = 0;
            int64_t totalBytesTransferred = 0;
            int64_t capacityRemaining = partition->capacity();
            int i = 0;
            while(i < numRows) {

                // different row type? => i.e. already exception here!
                if(rtype != rows[i].getRowType()) {
#ifndef NDEBUG
                    std::cout<<"Row has different type: "<<rows[i].toPythonString()<<std::endl;
#endif
                }

                int64_t bytesWritten = static_cast<int64_t>(rows[i].serializeToMemory(base_ptr, capacityRemaining));

                auto serializedLength = rows[i].serializedLength(); // can be 0 for null values, empty dict, empty tuple, ...

                minBytesRequired = std::max(minBytesRequired, serializedLength);

                // two possible results:
                // data to partition written or no space
                if(bytesWritten > 0 || serializedLength == 0) {
                    // all ok, inc counters
                    totalBytesTransferred += bytesWritten;
                    base_ptr += bytesWritten;
                    i++;
                    numWrittenRowsInPartition++;
                    capacityRemaining -= bytesWritten;
                } else {
                    // partition is full, request new one.
                    // create new partition...
                    partition->unlock();
                    partition->setNumRows(numWrittenRowsInPartition);
                    addPartition(dsptr, partition);
                    partition = requestNewPartition(schema, dataSetID, minBytesRequired);
                    numPartitions++;
                    numWrittenRowsInPartition = 0;
                    // check whether new requested partition is ok
                    if(!partition) {
                        return makeError("could not request partition to hold data. Out of Memory?");
                    }
                    capacityRemaining = partition->capacity();
                    base_ptr = (uint8_t*)partition->lock();
                }
            }

            partition->unlock();
            partition->setNumRows(numWrittenRowsInPartition);
            addPartition(dsptr, partition);

            Logger::instance()
                    .logger("core")
                    .info("materialized " + sizeToMemString(totalBytesTransferred) + " to " + std::to_string(numPartitions) + " partitions");

            // set rows
            dsptr->setColumns(columnNames);
            addParallelizeNode(dsptr, {}, {}, sampling_mode);

            // signal check
            if(check_and_forward_signals()) {
#ifndef NDEBUG
                Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
                return makeError("job aborted (signal received)");
            }

            return *dsptr;
        }
    }

    std::shared_ptr<LogicalOperator> Context::addOperator(const std::shared_ptr<LogicalOperator> &op) {
        _operators.push_back(op);
        return op;
    }

    void Context::serializePythonObjects(const std::vector<std::tuple<size_t, PyObject*>>& pythonObjects,
                                         const std::vector<size_t> &numExceptionsInPartition,
                                         const std::vector<Partition*> &normalPartitions,
                                         const int64_t opID,
                                         std::vector<Partition*> &serializedPythonObjects,
                                         std::unordered_map<std::string, ExceptionInfo> &pythonObjectsMap) {
        if (pythonObjects.empty()) {
            for (const auto &p : normalPartitions) {
                pythonObjectsMap[uuidToString(p->uuid())] = ExceptionInfo();
            }
            return;
        }

        Schema schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING}));
        const size_t allocMinSize = 1024 * 64; // 64KB

        Partition* partition = requestNewPartition(schema, -1, allocMinSize);
        int64_t* rawPtr = (int64_t*)partition->lockWriteRaw();
        *rawPtr = 0;
        uint8_t* ptr = (uint8_t*)(rawPtr + 1);
        size_t numBytesSerialized = 0;

        auto prevExpByteOffset = 0;
        auto prevExpRowOffset = 0;
        auto prevExpInd = 0;
        auto curNormalPartitionInd = 0;
        auto numNewExps = 0;

        // Serialize each exception to a partition using the following schema:
        // (1) is the field containing rowNum
        // (2) is the field containing ecCode
        // (3) is the field containing opID
        // (4) is the field containing pickledObjectSize
        // (5) is the field containing pickledObject
        for(auto &exception : pythonObjects) {
            auto rowNum = std::get<0>(exception);
            auto pyObj = std::get<1>(exception);
            auto ecCode = ecToI64(ExceptionCode::PYTHON_PARALLELIZE);
            auto pickledObject = python::pickleObject(python::getMainModule(), pyObj);
            auto pickledObjectSize = pickledObject.size();
            size_t requiredBytes = sizeof(int64_t) * 4 + pickledObjectSize;

            if (partition->capacity() < numBytesSerialized + requiredBytes) {
                partition->unlockWrite();
                serializedPythonObjects.push_back(partition);
                partition = requestNewPartition(schema, -1, allocMinSize);
                rawPtr = (int64_t *) partition->lockWriteRaw();
                *rawPtr = 0;
                ptr = (uint8_t * )(rawPtr + 1);
                numBytesSerialized = 0;
            }

            // Check if we have reached the number of exceptions in the input partition
            // Record the current exception index and offset and iterate to next one
            auto curNormalPartition = normalPartitions[curNormalPartitionInd];
            auto normalUUID = uuidToString(curNormalPartition->uuid());
            auto numExps = numExceptionsInPartition[curNormalPartitionInd];
            if (numNewExps >= numExps) {
                pythonObjectsMap[normalUUID] = ExceptionInfo(numExps, prevExpInd, prevExpRowOffset, prevExpByteOffset);
                prevExpRowOffset = *rawPtr;
                prevExpByteOffset = numBytesSerialized;
                prevExpInd = serializedPythonObjects.size();
                numNewExps = 0;
                curNormalPartitionInd++;
            }

            *((int64_t*)(ptr)) = rowNum; ptr += sizeof(int64_t);
            *((int64_t*)(ptr)) = ecCode; ptr += sizeof(int64_t);
            *((int64_t*)(ptr)) = opID; ptr += sizeof(int64_t);
            *((int64_t*)(ptr)) = pickledObjectSize; ptr += sizeof(int64_t);
            memcpy(ptr, pickledObject.c_str(), pickledObjectSize); ptr += pickledObjectSize;

            *rawPtr = *rawPtr + 1;
            numBytesSerialized += requiredBytes;
            numNewExps += 1;
        }

        // Record mapping for normal last partition
        auto curNormalPartition = normalPartitions[curNormalPartitionInd];
        auto normalUUID = uuidToString(curNormalPartition->uuid());
        auto numExceptions = numExceptionsInPartition[curNormalPartitionInd];
        pythonObjectsMap[normalUUID] = ExceptionInfo(numExceptions, prevExpInd, prevExpRowOffset, prevExpByteOffset);

        partition->unlockWrite();
        serializedPythonObjects.push_back(partition);
    }

    void Context::addParallelizeNode(DataSet *ds, const std::vector<std::tuple<size_t, PyObject*>> &badParallelizeObjects, const std::vector<size_t> &numExceptionsInPartition, const SamplingMode& sampling_mode) {
        assert(ds);

        // @TODO: make empty list as special case work. Also true for empty files.
        if(ds->getPartitions().empty())
            throw std::runtime_error("you submitted an empty list to be parallelized. Any pipeline transforming this list will yield an empty list! Aborting here.");

        assert(ds->_schema.getRowType() != python::Type::UNKNOWN);

        auto op = new ParallelizeOperator(ds->_schema, ds->getPartitions(), ds->columns(), sampling_mode);
        std::vector<Partition*> serializedPythonObjects;
        std::unordered_map<std::string, ExceptionInfo> pythonObjectsMap;
        serializePythonObjects(badParallelizeObjects, numExceptionsInPartition, ds->getPartitions(), op->getID(), serializedPythonObjects, pythonObjectsMap);
        op->setPythonObjects(serializedPythonObjects);
        op->setInputPartitionToPythonObjectsMap(pythonObjectsMap);

        // add new (root) node
        ds->_operator = addOperator(std::shared_ptr<LogicalOperator>(op));

        // set dataset
        ds->_operator->setDataSet(ds);
    }

    DataSet& Context::csv(const std::string &pattern,
                          const std::vector<std::string>& columns,
                          option<bool> hasHeader,
                          option<char> delimiter,
                          char quotechar,
                          const std::vector<std::string>& null_values,
                          const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                          const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                          const SamplingMode& sampling_mode) {
        using namespace std;

        Schema schema;
        int dataSetID = getNextDataSetID();
        DataSet *dsptr = createDataSet(schema);

        dsptr->_operator = addOperator(std::shared_ptr<LogicalOperator>(
                FileInputOperator::fromCsv(pattern, this->_options, hasHeader, delimiter, quotechar, null_values, columns,
                                      index_based_type_hints, column_based_type_hints, sampling_mode)));
        auto op = ((FileInputOperator*)dsptr->_operator.get());

        // check whether files were found, else return empty dataset!
        if(op->getURIs().empty()) {
            // note: dataset will be destroyed by context
            auto& ds = makeEmpty();
            op->setDataSet(&ds);
            return ds;
        }

        auto detectedColumns = ((FileInputOperator*)dsptr->_operator.get())->columns();
        dsptr->setColumns(detectedColumns);

        // check if columns are given
        if(!columns.empty()) {
            // compare with detected
            if(!detectedColumns.empty()) {
                bool identical = detectedColumns.size() == columns.size();
                for(int i = 0; i < std::min(detectedColumns.size(), columns.size()); ++i) {
                    if(detectedColumns[i] != columns[i])
                        identical = false;
                }

                if(!identical) {
                    // make error dataset
                    std::stringstream errStream;
                    errStream<<"detected columns "<<detectedColumns<<" do not match given columns "<<columns;
                    return makeError(errStream.str());
                }
            }

            dsptr->setColumns(columns);
            ((FileInputOperator*)dsptr->_operator.get())->setColumns(columns);
        }

        // set dataset to operator
        dsptr->_operator->setDataSet(dsptr);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return makeError("job aborted (signal received)");
        }

        return *dsptr;
    }

    DataSet& Context::text(const std::string &pattern, const std::vector<std::string>& null_values, const SamplingMode& sampling_mode) {
        using namespace std;

        Schema schema;
        int dataSetID = getNextDataSetID();
        DataSet *dsptr = createDataSet(schema);

        dsptr->_operator = addOperator(std::shared_ptr<LogicalOperator>(FileInputOperator::fromText(pattern, this->_options, null_values, sampling_mode)));

        auto detectedColumns = ((FileInputOperator*)dsptr->_operator.get())->columns();
        dsptr->setColumns(detectedColumns);

        // set dataset to operator
        dsptr->_operator->setDataSet(dsptr);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return makeError("job aborted (signal received)");
        }

        return *dsptr;
    }

    DataSet& Context::orc(const std::string &pattern,
                          const std::vector<std::string>& columns,
                          const SamplingMode& sampling_mode) {
        using namespace std;

#ifndef BUILD_WITH_ORC
        return makeError(MISSING_ORC_MESSAGE);
#endif

        Schema schema;
        int dataSetID = getNextDataSetID();
        DataSet *dsptr = createDataSet(schema);
        dsptr->_operator = addOperator(std::shared_ptr<LogicalOperator>(FileInputOperator::fromOrc(pattern, this->_options, sampling_mode)));
        auto op = ((FileInputOperator*)dsptr->_operator.get());

        // check whether files were found, else return empty dataset!
        if(op->getURIs().empty()) {
            // note: dataset will be destroyed by context
            auto& ds = makeEmpty();
            op->setDataSet(&ds);
            return ds;
        }

        auto detectedColumns = ((FileInputOperator*)dsptr->_operator.get())->columns();
        dsptr->setColumns(detectedColumns);

        // check if columns are given
        if(!columns.empty()) {
            // compare with detected
            if(!detectedColumns.empty()) {
                bool identical = detectedColumns.size() == columns.size();
                for(int i = 0; i < std::min(detectedColumns.size(), columns.size()); ++i) {
                    if(detectedColumns[i] != columns[i])
                        identical = false;
                }

                if(!identical) {
                    // make error dataset
                    std::stringstream errStream;
                    errStream<<"detected columns "<<detectedColumns<<" do not match given columns "<<columns;
                    return makeError(errStream.str());
                }
            }

            dsptr->setColumns(columns);
            ((FileInputOperator*)dsptr->_operator.get())->setColumns(columns);
        }

        // set dataset to operator
        dsptr->_operator->setDataSet(dsptr);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return makeError("job aborted (signal received)");
        }

        return *dsptr;
    }

    uint8_t* Context::partitionLockRaw(tuplex::Partition *partition) {
        return partition->lockWriteRaw();
    }

    void Context::partitionUnlock(tuplex::Partition *partition) {
        partition->unlockWrite();
    }

    size_t Context::partitionCapacity(tuplex::Partition *partition) {
        return partition->capacity();
    }

    void Context::setColumnNames(tuplex::DataSet *ds, const std::vector<std::string> &names) {
        ds->setColumns(names);
    }

    Executor* Context::getDriver() const {
        assert(_ee); return _ee->driver();
    }

    codegen::CompilePolicy compilePolicyFromOptions(const ContextOptions &options) {
        auto p = codegen::CompilePolicy();
        p.allowUndefinedBehavior = options.UNDEFINED_BEHAVIOR_FOR_OPERATORS();
        p.allowNumericTypeUnification = options.AUTO_UPCAST_NUMBERS();
        p.sharedObjectPropagation = options.OPT_SHARED_OBJECT_PROPAGATION();
        p.normalCaseThreshold = options.NORMALCASE_THRESHOLD();
        return p;
    }
}
