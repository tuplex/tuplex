//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/TransformStage.h>
#include <physical/TransformTask.h>
#include <physical/ResolveTask.h>
// to avoid conflicts with Python 3.7
#include "../include/Context.h"
#include <physical/LLVMOptimizer.h>
#include <limits>
#include <RuntimeInterface.h>
#include <functional>
#include <LocalEngine.h>
#include <physical/CSVParserGenerator.h>
#include <PartitionWriter.h>

#include <thread>
#include <random>
#include <chrono>
#include <StringUtils.h>
#include <algorithm>
#include <string>
#include <physical/BlockBasedTaskBuilder.h>
#include <physical/TuplexSourceTaskBuilder.h>
#include <physical/PipelineBuilder.h>
#include <logical/FileOutputOperator.h>
#include <physical/PythonPipelineBuilder.h>
#include <PartitionWriter.h>
#include <limits>
#include <physical/JITCSVSourceTaskBuilder.h>
#include <physical/CellSourceTaskBuilder.h>
#include <logical/JoinOperator.h>
#include <CSVUtils.h>
#include <JSONUtils.h>
#include <int_hashmap.h>
#include "physical/PhysicalPlan.h"

namespace tuplex {

    TransformStage::TransformStage(PhysicalPlan *plan, IBackend *backend,
                                   int64_t number,
                                   bool allowUndefinedBehavior) : PhysicalStage::PhysicalStage(plan, backend, number),
                                                                  _inputLimit(std::numeric_limits<size_t>::max()),
                                                                  _outputLimit(std::numeric_limits<size_t>::max()),
                                                                  _aggMode(AggregateType::AGG_NONE),
                                                                  _updateInputExceptions(false) {

        // TODO: is this code out of date? + is allowUndefinedBehavior needed here?
        // plan stage using operators.
        // there are 2x2 options:
        // 1. file -> memory
        // 2. file -> file
        // 3. memory -> file
        // 4. memory -> memory

//        assert(_operators.size() >= 2); // for now, no other staging etc. implemented
//
//        generateCode(allowUndefinedBehavior);

        // generate fallback, pure python pipeline
        // when is it necessary to actually generate pure python code?
        // ==> 1.) Parsing text data like CSV/JSON because of odd behavior and to resolve this via type sniffing
        // ==> 2.) when objects of different type are parsed to the backend in the PythonWrapper.

        // @TODO: generate this ONLY when either there is weird pyth
//        generatePythonCode();

//        auto& logger = Logger::instance().logger("physical planner");
//        if(!_irCode.empty()) {
//            std::stringstream ss;
//            ss<<"generated stage "<<this->number()<<" ("<<sizeToMemString(_irCode.size())<<" LLVM code)";
//            logger.info(ss.str());
//        }

        // // check if it is memory to memory
        // auto ftype = _operators.front()->type();
        // auto ltype = _operators.back()->type();
        // if(ftype == LogicalOperatorType::PARALLELIZE
        // && ltype == LogicalOperatorType::TAKE) {
        //     // memory -> memory
        // } else if(ftype == LogicalOperatorType::CSV && ltype == LogicalOperatorType::TAKE) {
        //     // file -> memory
        // } else if(ftype == LogicalOperatorType::PARALLELIZE && ltype == LogicalOperatorType::FILEOUTPUT) {
        //     // memory -> file
        // } else if(ftype == LogicalOperatorType::CSV && ltype == LogicalOperatorType::FILEOUTPUT) {
        //     // file -> file
        // } else {
        //     throw std::runtime_error("found unknown trafo stage: " + _operators.front()->name() + " -> ... -> " + _operators.back()->name());
        // }
    }

    void TransformStage::setInputFiles(const std::vector<URI> &uris, const std::vector<size_t> &sizes) {
        using namespace std;
        assert(uris.size() == sizes.size());
        assert(backend());

        vector<Row> rows;
        rows.reserve(uris.size());
        for (int i = 0; i < uris.size(); ++i) {
            rows.push_back(Row(uris[i].toPath(), (int64_t) sizes[i]));
        }

        // write data to partitions
        int64_t dataSetID = 0; // no ID here
        _inputPartitions = rowsToPartitions(backend()->driver(), dataSetID, context().id(), rows);
    }


    void TransformStage::setFileResult(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &ecounts) {
        setExceptionCounts(ecounts);

        _rs = emptyResultSet();
    }

    void TransformStage::setMemoryResult(const std::vector<Partition *> &partitions,
                                         const std::vector<Partition*>& generalCase,
                                         const std::unordered_map<std::string, ExceptionInfo>& partitionToExceptionsMap,
                                         const std::vector<std::tuple<size_t, PyObject*>>& interpreterRows,
                                         const std::vector<Partition*>& remainingExceptions,
                                         const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &ecounts) {
        setExceptionCounts(ecounts);

        if (partitions.empty() && interpreterRows.empty() && generalCase.empty())
            _rs = emptyResultSet();
        else {
            std::vector<Partition *> limitedPartitions;
            auto schema = Schema::UNKNOWN;

            if(!partitions.empty()) {
                schema = partitions.front()->schema();
                for (auto partition : partitions) {
                    assert(schema == partition->schema());
                }

                // check output limit, adjust partitions if necessary
                size_t numOutputRows = 0;
                for (auto partition : partitions) {
                    numOutputRows += partition->getNumRows();
                    if (numOutputRows >= outputLimit()) {
                        // clip last partition & leave loop
                        auto clipped = outputLimit() - (numOutputRows - partition->getNumRows());
                        assert(clipped <= partition->getNumRows());
                        partition->setNumRows(clipped);
                        if (clipped > 0)
                            limitedPartitions.push_back(partition);
                        break;
                    } else {
                        // put full partition to output set
                        limitedPartitions.push_back(partition);
                    }
                }
            }

            // put ALL partitions to result set
            _rs = std::make_shared<ResultSet>(schema, limitedPartitions,
                                              generalCase, partitionToExceptionsMap, interpreterRows,
                                              outputLimit());
        }
    }

    std::unordered_map<std::string, std::string> TransformStage::outputOptions() const {
        // just return params...
        return _fileOutputParameters;
    }

    nlohmann::json TransformStage::getJSON() const {
        using namespace nlohmann;
        using namespace std;

        // get json from base class
        auto j = PhysicalStage::getJSON();

        // add operators...
        vector<json> ops;

        // @TODO: need to change this...
//        for(auto op : _operators) {
//            json val;
//            val["name"] = op->name();
//            val["id"] = "op" + std::to_string(op->getID());
//            val["columns"] = op->columns();
//
//            // UDF code for display...
//            if(hasUDF(op)) {
//                UDFOperator *udfop = (UDFOperator*)op;
//                assert(udfop);
//
//                val["udf"] = udfop->getUDF().getCode();
//            }
//            ops.push_back(val);
//        }
//
//        j["operators"] = ops;

        return j;
    }

    std::shared_ptr<ResultSet> TransformStage::emptyResultSet() const {
        // important to type it, else some converters will fail (rstocpython)
        return std::make_shared<ResultSet>(outputSchema(), std::vector<Partition *>());
    }

    // @TODO: unify hashsink + hashresult
    // Also this should take care of potential hybrid?
    // TODO: this function is basically converting ONLY keys...!
    static std::vector<Partition*> convertHashTableKeysToPartitions(const TransformStage::HashResult& result, const Schema &outputSchema, bool hasFixedSizeKeys, size_t fixedSizeKeyLength, const Context &context) {
        std::vector<std::pair<const char*, size_t>> unique_rows;
        size_t total_serialized_size = 0;
        const map_t &hashtable = result.hash_map;
        MessageHandler& logger = Logger::instance().defaultLogger();

        // check which hashmap is used
        // @TODO: Only two kinds right now supported, expand in the future...
        bool isi64hashmap = (hasFixedSizeKeys && fixedSizeKeyLength == 8);

        // on which executor to store?
        // -> driver for now. This is not ideal!
        auto driver = context.getDriver();
        int64_t outputDataSetID = -1;

        // check whether keys of hashtable can be upcast to output schema.
        // -> if not, error and return empty vector!

        // hashKeyType is the type in which the key is stored. (NOT INCLUDING OPT!)
        python::Type hashKeyType = result.keyType.withoutOptions(); // remove option b.c. of null-bucket design. @TODO: this is not 100% correct, because inner options will also get sacrificed by this...
        python::Type keyRowType = python::Type::propagateToTupleType(hashKeyType);

        bool requiresUpcast = false;
        if(python::canUpcastToRowType(keyRowType, outputSchema.getRowType())) {
            requiresUpcast = keyRowType != outputSchema.getRowType();
        } else {
            std::string err_message = "Hash table keys are given as rowtype " + keyRowType.desc() + ", yet output desired is " + outputSchema.getRowType().desc() + ". Can't upcast rows from hashtable to target type!";
            logger.error(err_message);
            throw std::runtime_error(err_message);
        }

        // shortcut: no rows, empty partitions
        if(!result.hash_map && !result.null_bucket)
           return std::vector<Partition*>();

        Partition* partition = nullptr;
        PartitionWriter pw(driver, outputSchema, outputDataSetID, context.id(), context.getOptions().PARTITION_SIZE());

        // check whether null-bucket is filled, if so output!
        if(result.null_bucket) {
            // make sure it's either NULLVALUE or option type for the row!
            auto out_row_type = outputSchema.getRowType();

            // want to write output to partitions!
            // case 1: NULLVALUE as schema => simply write a single row.
            if(python::Type::propagateToTupleType(python::Type::NULLVALUE) == out_row_type) {
                Row r(Field::null());
                pw.writeRow(r);
            }
            // case 2: Option Type as schema => write null according to schema!
            else if(out_row_type.isTupleType() && out_row_type != python::Type::EMPTYTUPLE && out_row_type.parameters().size() == 1 && out_row_type.parameters().front().isOptionType()) {
                Row r(Field::null());
                r = r.upcastedRow(out_row_type);
                pw.writeRow(r);
            }
            // error case
            else {
                std::string err_message = "null bucket is filled, yet desired output type is " + out_row_type.desc() + ", conversion not supported";
                logger.error(err_message);
                throw std::runtime_error(err_message);
            }
        }

        // get all the other rows if required...
        if(result.hash_map) {
            // i64? => faster copy possible!
            if(isi64hashmap) {
                // upcast required? => can be only into option type!
                if(requiresUpcast) {
                    // make sure it's Opt[i64] type!
                    assert(outputSchema.getRowType() == python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64)));
                    // also issue warning to user b.c. no support for other things yet...
                    if(outputSchema.getRowType() != python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64))) {
                        logger.error("attempting to decode i64 hashtable, yet output schema is "+ outputSchema.getRowType().desc() + ", expected (Opt[i64])");
                    }

                    int64_hashmap_iterator_t iterator = 0;
                    uint64_t key;

                    // an i64, non-null is simply 16bytes -> 0 | value
                    int64_t buf[2]; buf[0] = 0;
                    while(int64_hashmap_get_next_key(hashtable, &iterator, &key)) {
                        buf[1] = key;
                        // copy to partition
                        pw.writeData(reinterpret_cast<const uint8_t *>(buf), sizeof(int64_t) * 2);
                    }
                } else {
                    // super fast copy of integer results (could be done even faster, but this should do...)
                    int64_hashmap_iterator_t iterator = 0;
                    uint64_t key;

                    // an i64, non-null is simply 16bytes -> 0 | value
                    while(int64_hashmap_get_next_key(hashtable, &iterator, &key)) {
                        // copy to partition
                        pw.writeData(reinterpret_cast<const uint8_t *>(&key), sizeof(int64_t));
                    }
                }
            } else {
                // str/bytes based hashmap => slow copy requiring potentially upcasting...

                // this is the old version when keys are assumed to be fully serialized rows!
                // -> i.e. need to define key storage format!!!
                // @TODO: rewrite aggregate code.
                // // just copy over the keys (they're already the correct data format serialized!)
                // hashmap_iterator_t iterator = 0;
                // const char *key = nullptr;
                // uint64_t keylen = 0;
                // while((key = hashmap_get_next_key(hashtable, &iterator, &keylen)) != nullptr) {
                //     // there might be faster way...
                //     pw.writeData(reinterpret_cast<const uint8_t *>(key), keylen);
                // }

                // because string format to store in partitions is different from hashmap, need to convert!

                // get the unique rows + size
                hashmap_iterator_t iterator = 0;
                const char *key = nullptr;
                uint64_t keylen = 0;
                auto input_schema = Schema(Schema::MemoryLayout::ROW, result.keyType);
                auto out_row_type = outputSchema.getRowType();
                while((key = hashmap_get_next_key(hashtable, &iterator, &keylen)) != nullptr) {
                    Row r;

                    if(hashKeyType == python::Type::propagateToTupleType(python::Type::STRING)) {
                        // use directly key as str...
                        std::string s(key);
                        r = Row(s);
                        r = r.upcastedRow(out_row_type);
                    } else {
                        throw std::runtime_error("decoding of other types not yet supported...");

                        // // this is how it potentially should look like...
                        // // decode key into Row, upcast, serialize
                        // Row r = Row::fromMemory(input_schema, key, keylen);
                        // r = r.upcastedRow(out_row_type);
                    }

                    // save row to partition
                    pw.writeRow(r);
                }
            }
        }

        // TODO: need to make mechanism to pass non-conforming python objects along as well...
        // --> some clever refactoring might ease all of this...
        if(result.hybrid) {
            python::lockGIL();
            // check how many non-conforming objects there are...
            auto hybrid = reinterpret_cast<HybridLookupTable*>(result.hybrid);
            if(hybrid->backupItemCount() != 0) {
                logger.error("hash table result contains " + std::to_string(hybrid->backupItemCount()) + " non-schema conforming objects, dropping them for now. NOT YET IMPLEMENTED.");
            }
            python::unlockGIL();
        }

        return pw.getOutputPartitions();
    }

    static void appendToSerializer(Serializer &s, Deserializer &d, python::Type t, int col) {
        if(t == python::Type::I64) {
            s.append(d.getInt(col));
        } else if(t == python::Type::F64) {
            s.append(d.getDouble(col));
        } else if(t == python::Type::STRING) {
            s.append(d.getString(col));
        } else if(t == python::Type::BOOLEAN) {
            s.append(d.getBool(col));
        } else if(t.isListType()) {
            s.append(d.getList(col));
        } else if(t.isDictionaryType()) {
            s.append(d.getDictionary(col));
        } else {
            throw std::runtime_error("invalid type in appendToSerializer: " + t.desc());
        }
    }

    static void appendBucketToSerializer(Serializer &s, const uint8_t* bucket, const int64_t bucket_size, const python::Type &aggType) {
        Deserializer d(Schema(Schema::MemoryLayout::ROW, aggType)); d.deserialize(bucket, bucket_size);
        if(aggType.isTupleType()) {
            for(int i = 0; i < aggType.parameters().size(); i++) {
                appendToSerializer(s, d, aggType.parameters()[i], i);
            }
        } else {
            appendToSerializer(s, d, aggType, 0);
        }
    }

    static void appendBucketToSerializer(Serializer &s, const uint8_t* buffer, const python::Type &aggType) {
        int64_t bucket_size = *(int64_t*)buffer;
        const uint8_t *bucket = buffer + 8;
        appendBucketToSerializer(s, bucket, bucket_size, aggType);
    }

    static size_t appendRow(Serializer &s, std::vector<std::pair<const char *, size_t>> &rows) {
        auto final_length = s.length();
        auto final_val = new char[final_length];
        auto sanity_check = s.serialize(final_val, final_length);
        assert(sanity_check == final_length);
        rows.emplace_back(final_val, final_length);
        return final_length;
    }

    static size_t appendBucketAsPartition(std::vector<std::pair<const char *, size_t>> &rows, const uint8_t *buffer, uint64_t keylen, const char *key, const python::Type &keyType, const python::Type &aggType) {
        Serializer s;

        // get the key
        if(keyType.isOptionType() && key == nullptr) {
            assert(keylen == 0);
            if(keyType == python::Type::STRING) s.append(option<std::string>::none);
            else throw std::runtime_error("unsupported key type");
        }
        else if (keyType == python::Type::STRING) s.append(std::string(key, keylen - 1));
        else if(keyType == python::Type::makeOptionType(python::Type::STRING)) s.append(option<std::string>(std::string(key, keylen-1)));
        else if(keyType.isTupleType()) {
            appendBucketToSerializer(s, reinterpret_cast<const uint8_t *>(key), keylen, keyType);
        }
        else throw std::runtime_error("unsupported key type");

        // get the aggregated values
        appendBucketToSerializer(s, buffer, aggType);

        // save the row
        return appendRow(s, rows);
    }

    static size_t appendInt64BucketAsPartition(std::vector<std::pair<const char *, size_t>> &rows, const uint8_t *buffer, bool null, uint64_t key, const python::Type &keyType, const python::Type &aggType) {
        Serializer s;

        // get the key
        if(keyType.isOptionType() && null) {
            if(keyType == python::Type::STRING) s.append(option<std::string>::none);
            else throw std::runtime_error("unsupported key type");
        }
        else if (keyType == python::Type::I64) s.append(static_cast<int64_t>(key));
        else if(keyType == python::Type::makeOptionType(python::Type::I64)) s.append(option<int64_t>(static_cast<int64_t>(key)));
        else throw std::runtime_error("unsupported key type");

        // get the aggregated values
        appendBucketToSerializer(s, buffer, aggType);

        // save the row
        return appendRow(s, rows);
    }

    static std::vector<Partition*> convertHashTableToPartitionsAggByKey(const TransformStage::HashResult& result, const Schema &schema, const Context &context) {
        std::vector<std::pair<const char*, size_t>> unique_rows;
        size_t total_serialized_size = 0;
        const map_t &hashtable = result.hash_map;

        // check whether null-bucket is filled, if so output!
        auto full_type = schema.getRowType();
        auto agg_type = result.bucketType;

        // get the unique rows + size
        hashmap_iterator_t iterator = 0;
        const char *key = nullptr;
        uint64_t keylen = 0;
        uint8_t *bucket = nullptr;
        while((key = hashmap_get_next_key(hashtable, &iterator, &keylen)) != nullptr) {
            // get the value
            hashmap_get(hashtable, key, keylen, (void **) (&bucket));
            total_serialized_size += appendBucketAsPartition(unique_rows, bucket, keylen, key, result.keyType, result.bucketType);
        }

        if(result.keyType.isOptionType() && result.null_bucket != nullptr) {
            total_serialized_size += appendBucketAsPartition(unique_rows, result.null_bucket, 0, nullptr, result.keyType, result.bucketType);
        }

        // construct return partition
        auto p = context.getDriver()->allocWritablePartition(total_serialized_size + sizeof(uint64_t), schema, -1, context.id());
        auto data_region = reinterpret_cast<char *>(p->lockWrite());
        for(const auto& pr: unique_rows) {
            memcpy(data_region, pr.first, pr.second);
            data_region += pr.second;
        }
        p->setBytesWritten(total_serialized_size);
        p->setNumRows(unique_rows.size());

        // special case: if the type is 0 length serialized, add a dummy row!
        if(result.keyType.isZeroSerializationSize() && total_serialized_size == 0 && result.null_bucket != nullptr) {
            p->setNumRows(1);
        }

        p->unlockWrite();

        std::vector<Partition*> ret;
        ret.push_back(p);

        // @TODO: arbitrary python objects!

        return ret;
    }

    static std::vector<Partition*> convertInt64HashTableToPartitions(const TransformStage::HashResult& result, const Schema &schema, const Context &context) {
        std::vector<std::pair<const char*, size_t>> unique_rows;
        size_t total_serialized_size = 0;
        const map_t &hashtable = result.hash_map;

        // check whether null-bucket is filled, if so output!

        // get the unique rows + size
        int64_hashmap_iterator_t iterator = 0;
        uint64_t key;
        while(int64_hashmap_get_next_key(hashtable, &iterator, &key)) {
            Serializer s;
            s.append(key);
            size_t size = s.length();
            char *buf = new char[size];
            s.serialize(buf, size);
            // save the buffer
            unique_rows.emplace_back(buf, size);
            total_serialized_size += size;
        }

        // construct return partition
        auto p = context.getDriver()->allocWritablePartition(total_serialized_size + sizeof(uint64_t), schema, -1, context.id());
        auto data_region = reinterpret_cast<char *>(p->lockWrite());
        for(const auto& pr: unique_rows) {
            memcpy(data_region, pr.first, pr.second);
            data_region += pr.second;
        }
        p->setBytesWritten(total_serialized_size);
        p->setNumRows(unique_rows.size());
        p->unlockWrite();

        std::vector<Partition*> ret;
        ret.push_back(p);

        return ret;
    }

    static std::vector<Partition*> convertInt64HashTableToPartitionsAggByKey(const TransformStage::HashResult& result,
                                                                             const Schema &schema,
                                                                             const Context &context) {
        std::vector<std::pair<const char*, size_t>> unique_rows;
        size_t total_serialized_size = 0;
        const map_t &hashtable = result.hash_map;

        // check whether null-bucket is filled, if so output!
        auto full_type = schema.getRowType();
        auto agg_type = result.bucketType;

        // get the unique rows + size
        int64_hashmap_iterator_t iterator = 0;
        uint64_t key = 0;
        uint8_t *bucket = nullptr;
        while(int64_hashmap_get_next_key(hashtable, &iterator, &key)) {
            // get the value
            int64_hashmap_get(hashtable, key, (void **) (&bucket));
            total_serialized_size += appendInt64BucketAsPartition(unique_rows, bucket, false, key, result.keyType, result.bucketType);
        }

        if(result.keyType.isOptionType() && result.null_bucket != nullptr) {
            total_serialized_size += appendInt64BucketAsPartition(unique_rows, result.null_bucket, true, 0, result.keyType, result.bucketType);
        }

        // construct return partition
        auto p = context.getDriver()->allocWritablePartition(total_serialized_size + sizeof(uint64_t), schema, -1, context.id());
        auto data_region = reinterpret_cast<char *>(p->lockWrite());
        for(const auto& pr: unique_rows) {
            memcpy(data_region, pr.first, pr.second);
            data_region += pr.second;
        }
        p->setBytesWritten(total_serialized_size);
        p->setNumRows(unique_rows.size());
        p->unlockWrite();

        std::vector<Partition*> ret;
        ret.push_back(p);

        return ret;
    }

    void TransformStage::execute(const Context &context) {
        using namespace std;

        // // use this to log out stage dependence structure
        // stringstream ss;
        // ss<<"Stage"<<this->number()<<" depends on: ";
        // for(auto stage: predecessors())
        //     ss<<"Stage"<<stage->number()<<" ";
        // Logger::instance().defaultLogger().info(ss.str());

        // execute all predecessors (can be at most one!)
        // @TODO: this should be parallelized for tiny stages!
        for (auto stage : predecessors())
            stage->execute(context);

        // if output is hashtable, pass to init function!
        auto numPreds = predecessors().size();

        int64_t numArgs = 0;
        uint8_t** hash_maps = nullptr;
        uint8_t** null_buckets = nullptr;
        PyObject** hybrids = nullptr;

        // check if predecessors exist, if so set input from them.
        if (!predecessors().empty()) {
            hash_maps = new uint8_t*[numPreds];
            null_buckets = new uint8_t*[numPreds];

            vector<Partition*> partitions;
            for(int i = 0; i < numPreds; ++i) {
                hash_maps[i] = nullptr;
                null_buckets[i] = nullptr;

                auto stage = predecessors()[i];
                // hashmap output? fetch result!
                switch(stage->outputMode()) {
                    case EndPointMode::HASHTABLE: {
                        auto tstage = dynamic_cast<TransformStage*>(stage); assert(stage); // hacky version here
                        // ideally should be done in parallel, but for now let's do it single-threaded.
                        // following is fill-in code
                        if(tstage->dataAggregationMode() == AggregateType::AGG_NONE) {
                            hash_maps[numArgs] = (uint8_t*)tstage->hashResult().hash_map;
                            null_buckets[numArgs] = tstage->hashResult().null_bucket;
                            numArgs++;
                        } else if(tstage->dataAggregationMode() == AggregateType::AGG_UNIQUE) {
                            // convert to partitions, and set them via
                            // --> why do we need to do this for every hashtable?
                            // --> if we have a union operator, we need to unify multiple hashops eventually
                            std::vector<Partition *> p;

                            bool hashFixedSizeKeys = tstage->hashtableKeyByteWidth() == 8;
                            p = convertHashTableKeysToPartitions(tstage->hashResult(), tstage->outputSchema(), hashFixedSizeKeys, tstage->hashtableKeyByteWidth(), context);
                            std::copy(std::begin(p), std::end(p), std::back_inserter(partitions));
                        } else if(tstage->dataAggregationMode() == AggregateType::AGG_BYKEY) {
                            std::vector<Partition *> p;
                            // @TODO.
                            // buggy here as well if NVO is used...
                            if(context.getOptions().OPT_NULLVALUE_OPTIMIZATION())
                                Logger::instance().defaultLogger().error("aggregation resolution not supported yet with NVO. Deactivate to make this working. Other bugs might be here as well...");

                            if (tstage->hashtableKeyByteWidth() == 8)
                                p = convertInt64HashTableToPartitionsAggByKey(tstage->hashResult(), tstage->outputSchema(), context);
                            else
                                p = convertHashTableToPartitionsAggByKey(tstage->hashResult(), tstage->outputSchema(), context);
                            std::copy(std::begin(p), std::end(p), std::back_inserter(partitions));
                        } else {
                            throw std::runtime_error("invalid dataAggregationMode");
                        }
                        break;
                    }
                    case EndPointMode::MEMORY:
                    case EndPointMode::FILE: {
                        auto p = stage->resultSet()->partitions();
                        std::copy(std::begin(p), std::end(p), std::back_inserter(partitions));
                        break;
                    }
                    default:
                        throw std::runtime_error("unknown endpoint mode in execute");
                }
            }


            // set input partitions & init Data
            setInitData(numArgs, hash_maps, null_buckets);
            if(inputPartitions().empty()) {
                setInputPartitions(partitions);
            }
        }

        // execute stage via backend
        backend()->execute(this);

        // free hashmaps of dependents (b.c. it's a tree this is ok)
        if(numArgs > 0) {
            for(int i = 0; i < numPreds; ++i) {
                auto stage = predecessors()[i];
                if(stage->outputMode() == EndPointMode::HASHTABLE) {
                    // free hash table and all of its buckets!
                    // (i.e. the combined hash table!, the others have been already freed)

                    auto null_bucket = hashResult().null_bucket;
                    auto hm = hashResult().hash_map;
                    if(null_bucket)
                        free(null_bucket);

                    if(hm) {
                        hashmap_free_key_and_data(hm);
                        hashmap_free(hm);
                        hm = nullptr;
                    }
                }

                // others, nothing todo. Partitions should have been invalidated...
            }
        }
    }

    std::vector<std::string> TransformStage::csvHeader() const {
        using namespace std;

        assert(csvHasHeader());

        auto it = _fileInputParameters.find("csvHeader");
        assert(it != _fileInputParameters.end());

        auto headerLine = it->second;

        assert(!headerLine.empty());

        // decode csv from header line, later direct comparison for speed!
        vector<string> fields;
        size_t numParsedBytes = 0;
        auto code = parseRow(headerLine.c_str(), headerLine.c_str() + headerLine.length(), fields, numParsedBytes,
                             csvInputDelimiter(),
                             csvInputQuotechar());
        assert(numParsedBytes > 0);
        assert(code == ExceptionCode::SUCCESS);
        return fields;
    }

    std::shared_ptr<TransformStage::JITSymbols> TransformStage::compile(JITCompiler &jit, LLVMOptimizer *optimizer, bool excludeSlowPath, bool registerSymbols) {
        auto& logger = Logger::instance().defaultLogger();

        // lazy compile
        if(!_syms) {
            logger.debug("lazy init symbols");
            _syms = std::make_shared<JITSymbols>();
        }

        Timer timer;

        llvm::LLVMContext ctx;
        auto bit_code = bitCode();
        if(bit_code.empty())
            return _syms;

        auto mod = codegen::bitCodeToModule(ctx, bit_code);
        if(!mod)
            throw std::runtime_error("invalid bitcode");

        logger.debug("parse module in " + std::to_string(timer.time()));

        // because in Lambda there's no context yet, use some dummy object...
        JobMetrics dummy_metrics;
        JobMetrics& metrics = PhysicalStage::plan() ? PhysicalStage::plan()->getContext().metrics() : dummy_metrics;

        std::string unoptimizedIR;
        std::string optimizedIR = "Not currently optimized.";
        if (_historyServer) {
            unoptimizedIR = code();
        }

        // step 1: run optimizer if desired
        if(optimizer) {
            optimizer->optimizeModule(*mod.get());
            if (_historyServer) {
                optimizedIR = code();
            }
            double llvm_optimization_time = timer.time();
            metrics.setLLVMOptimizationTime(llvm_optimization_time);
            logger.info("Optimization via LLVM passes took " + std::to_string(llvm_optimization_time) + " ms");
            timer.reset();
        }

        logger.debug("registering symbols...");
        // step 2: register callback functions with compiler
        if(registerSymbols && !writeMemoryCallbackName().empty())
            jit.registerSymbol(writeMemoryCallbackName(), TransformTask::writeRowCallback(hasOutputLimit(), false));
        if(registerSymbols && !exceptionCallbackName().empty())
            jit.registerSymbol(exceptionCallbackName(), TransformTask::exceptionCallback(false));
        if(registerSymbols && !writeFileCallbackName().empty())
            jit.registerSymbol(writeFileCallbackName(), TransformTask::writeRowCallback(hasOutputLimit(), true));

        if(registerSymbols && outputMode() == EndPointMode::HASHTABLE && !_funcHashWriteCallbackName.empty()) {
            if (hashtableKeyByteWidth() == 8) {
                if(_aggregateAggregateFuncName.empty())
                    jit.registerSymbol(_funcHashWriteCallbackName, TransformTask::writeInt64HashTableCallback());
                else jit.registerSymbol(_funcHashWriteCallbackName, TransformTask::writeInt64HashTableAggregateCallback());
            }
            else {
                if(_aggregateAggregateFuncName.empty())
                    jit.registerSymbol(_funcHashWriteCallbackName, TransformTask::writeStringHashTableCallback());
                else jit.registerSymbol(_funcHashWriteCallbackName, TransformTask::writeStringHashTableAggregateCallback());
            }
        }
        assert(!_initStageFuncName.empty() && !_releaseStageFuncName.empty());
        if(registerSymbols && !_aggregateCombineFuncName.empty())
            jit.registerSymbol(aggCombineCallbackName(), TransformTask::aggCombineCallback());

        // compile & link with resolve tasks
        if(registerSymbols && !resolveWriteCallbackName().empty())
            jit.registerSymbol(resolveWriteCallbackName(), ResolveTask::mergeRowCallback());
        if(registerSymbols && !resolveExceptionCallbackName().empty())
            jit.registerSymbol(resolveExceptionCallbackName(), ResolveTask::exceptionCallback());

        if(registerSymbols && outputMode() == EndPointMode::HASHTABLE && !resolveExceptionCallbackName().empty()) {
            if(hashtableKeyByteWidth() == 8) {
                if(_aggregateAggregateFuncName.empty())
                    jit.registerSymbol(resolveHashCallbackName(), ResolveTask::writeInt64HashTableCallback());
                else jit.registerSymbol(resolveHashCallbackName(), ResolveTask::writeInt64HashTableAggregateCallback());
            }
            else {
                if(_aggregateAggregateFuncName.empty())
                    jit.registerSymbol(resolveHashCallbackName(), ResolveTask::writeStringHashTableCallback());
                else jit.registerSymbol(resolveHashCallbackName(), ResolveTask::writeStringHashTableAggregateCallback());
            }
        }

        logger.info("starting code compilation");

        // 3. compile code
        // @TODO: use bitcode or llvm Module for more efficiency...
        if(!jit.compile(std::move(mod))) {
            logger.error("could not compile code for stage " + std::to_string(number()));
            throw std::runtime_error("could not compile code for stage " + std::to_string(number()));
        }
        std::stringstream ss;

        logger.info("first compile done");

        // fetch symbols (this actually triggers the compilation first with register alloc etc.)
        if(!_syms->functor && !_updateInputExceptions)
            _syms->functor = reinterpret_cast<codegen::read_block_f>(jit.getAddrOfSymbol(funcName()));
        if(!_syms->functorWithExp && _updateInputExceptions)
            _syms->functorWithExp = reinterpret_cast<codegen::read_block_exp_f>(jit.getAddrOfSymbol(funcName()));
        logger.info("functor " + funcName() + " retrieved from llvm");
        if(_outputMode == EndPointMode::FILE && !_syms->writeFunctor)
                _syms->writeFunctor = reinterpret_cast<codegen::read_block_f>(jit.getAddrOfSymbol(writerFuncName()));
       logger.info("retrieving init/release stage functors");
        if(!_syms->initStageFunctor)
            _syms->initStageFunctor = reinterpret_cast<codegen::init_stage_f>(jit.getAddrOfSymbol(_initStageFuncName));
        if(!_syms->releaseStageFunctor)
            _syms->releaseStageFunctor = reinterpret_cast<codegen::release_stage_f>(jit.getAddrOfSymbol(_releaseStageFuncName));

        // get aggregate functors
        if(!_aggregateInitFuncName.empty())
            _syms->aggInitFunctor = reinterpret_cast<codegen::agg_init_f>(jit.getAddrOfSymbol(_aggregateInitFuncName));
        if(!_aggregateCombineFuncName.empty())
            _syms->aggCombineFunctor = reinterpret_cast<codegen::agg_combine_f>(jit.getAddrOfSymbol(_aggregateCombineFuncName));
        if(!_aggregateAggregateFuncName.empty())
            _syms->aggAggregateFunctor = reinterpret_cast<codegen::agg_agg_f>(jit.getAddrOfSymbol(_aggregateAggregateFuncName));

        // compile slow code path if desired
        if(!excludeSlowPath) {
            if(!_syms->resolveFunctor)
                _syms->resolveFunctor = !resolveWriteCallbackName().empty() ? reinterpret_cast<codegen::resolve_f>(jit.getAddrOfSymbol(resolveRowName())) : nullptr;
        }

        // check symbols are valid...
        bool hasValidFunctor = true;
        if (_updateInputExceptions && !_syms->functorWithExp)
            hasValidFunctor = false;
        if (!_updateInputExceptions && !_syms->functor)
            hasValidFunctor = false;
        if(!hasValidFunctor && _syms->initStageFunctor && _syms->releaseStageFunctor) {
            logger.error("invalid pointer address for JIT code returned");
            throw std::runtime_error("invalid pointer address for JIT code returned");
        }

        double compilation_time_via_llvm_this_number = timer.time();
        double compilation_time_via_llvm_thus_far = compilation_time_via_llvm_this_number +
                metrics.getLLVMCompilationTime();
        metrics.setLLVMCompilationTime(compilation_time_via_llvm_thus_far);
        ss<<"Compiled code paths for stage "<<number()<<" in "<<std::fixed<<std::setprecision(2)<<compilation_time_via_llvm_this_number<<" ms";

        logger.info(ss.str());

        if(_historyServer) {
            _historyServer->sendStagePlan("Stage" + std::to_string(number()), unoptimizedIR, optimizedIR, "");
        }
        return _syms;
    }

#ifdef BUILD_WITH_AWS

    static messages::FileFormat fileFormat_toproto(FileFormat fmt) {
        switch(fmt) {
            case FileFormat::OUTFMT_CSV:
                return messages::FileFormat::FF_CSV;
            case FileFormat::OUTFMT_TEXT:
                return messages::FileFormat::FF_TEXT;
            case FileFormat::OUTFMT_TUPLEX:
                return messages::FileFormat::FF_TUPLEX;
            case FileFormat::OUTFMT_UNKNOWN:
                return messages::FileFormat::FF_UNKNOWN;
            default:
                throw std::runtime_error("unknown file format " + std::to_string((int)fmt) + " seen in protobuf conversion");
        }
    }

    TransformStage* TransformStage::from_protobuf(const messages::TransformStage &msg) {
        auto stage = new TransformStage(nullptr, nullptr, msg.stagenumber(), true); // dummy, no backend/plan

        // decode columns
        for(int i = 0; i < msg.inputcolumns_size(); ++i)
            stage->_inputColumns.push_back(msg.inputcolumns(i));
        for(int i = 0; i < msg.outputcolumns_size(); ++i)
            stage->_outputColumns.push_back(msg.outputcolumns(i));

        // decode schemas
        stage->_readSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(msg.readschema()));
        stage->_inputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(msg.inputschema()));
        stage->_outputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(msg.outputschema()));
        stage->_normalCaseInputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(msg.normalcaseinputschema()));
        stage->_normalCaseOutputSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(msg.normalcaseoutputschema()));

        stage->_outputDataSetID = msg.outputdatasetid();
        stage->_inputNodeID = msg.inputnodeid();
        stage->_inputColumnsToKeep = std::vector<bool>(msg.numcolumns(), false);
        for(auto i : msg.inputcolumnstokeep())
            stage->_inputColumnsToKeep[i] = true;

        stage->_outputURI = msg.outputuri();
        stage->_inputFormat = proto_toFileFormat(msg.inputformat());
        stage->_outputFormat = proto_toFileFormat(msg.outputformat());

        // params??
        stage->_inputMode = static_cast<EndPointMode>(msg.inputmode());
        stage->_outputMode = static_cast<EndPointMode>(msg.outputmode());

        stage->_irBitCode = msg.bitcode();
        stage->_pyCode = msg.pycode();
        stage->_pyPipelineName = msg.pypipelinename();

        stage->_persistSeparateCases = msg.persistseparatecases();
        stage->_updateInputExceptions = msg.updateinputexceptions();

        stage->_funcStageName = msg.funcstagename();
        stage->_funcMemoryWriteCallbackName = msg.funcmemorywritecallbackname();
        stage->_funcExceptionCallback = msg.funcexceptioncallback();
        stage->_funcFileWriteCallbackName = msg.funcfilewritecallbackname();
        stage->_funcHashWriteCallbackName = msg.funchashwritecallbackname();
        stage->_writerFuncName = "";

        stage->_initStageFuncName = msg.funcinitstagename();
        stage->_releaseStageFuncName = msg.funcreleasestagename();
        stage->_resolveRowFunctionName = msg.resolverowfunctionname();
        stage->_resolveRowWriteCallbackName = msg.resolverowwritecallbackname();
        stage->_resolveRowExceptionCallbackName = msg.resolverowexceptioncallbackname();
        stage->_resolveHashCallbackName = msg.resolvehashcallbackname();

        // decode input/output params
        for(const auto& keyval : msg.inputparameters())
            stage->_fileInputParameters[keyval.first] = keyval.second;
        for(const auto& keyval : msg.outputparameters())
            stage->_fileOutputParameters[keyval.first] = keyval.second;

        stage->setInitData();
        return stage;
    }

    std::unique_ptr<messages::TransformStage> TransformStage::to_protobuf() const {
        auto msg = std::make_unique<messages::TransformStage>();

        msg->set_bitcode(_irBitCode);
        msg->set_pycode(_pyCode);
        msg->set_pypipelinename(_pyPipelineName);

        for(const auto& col : _inputColumns)
            msg->add_inputcolumns(col);
        for(const auto& col : _outputColumns)
            msg->add_outputcolumns(col);
        msg->set_readschema(_readSchema.getRowType().desc());
        msg->set_inputschema(_inputSchema.getRowType().desc());
        msg->set_outputschema(_outputSchema.getRowType().desc());
        msg->set_normalcaseinputschema(_normalCaseInputSchema.getRowType().desc());
        msg->set_normalcaseoutputschema(_normalCaseOutputSchema.getRowType().desc());
        msg->set_outputdatasetid(_outputDataSetID);
        msg->set_inputnodeid(_inputNodeID);
        msg->set_inputmode(static_cast<messages::EndPointMode>(_inputMode));
        msg->set_outputmode(static_cast<messages::EndPointMode>(_outputMode));

        msg->set_numcolumns(_inputColumnsToKeep.size());
        for(int i = 0; i < _inputColumnsToKeep.size(); ++i) {
            if(_inputColumnsToKeep[i])
                msg->add_inputcolumnstokeep(i);
        }

        msg->set_outputuri(_outputURI.toString()); // NOTE: this should be overwritten by the task!!!

        msg->set_inputformat(fileFormat_toproto(_inputFormat));
        msg->set_outputformat(fileFormat_toproto(_outputFormat));

        msg->set_persistseparatecases(_persistSeparateCases);
        msg->set_updateinputexceptions(_updateInputExceptions);
        msg->set_funcstagename(_funcStageName);
        msg->set_funcmemorywritecallbackname(_funcMemoryWriteCallbackName);
        msg->set_funcfilewritecallbackname(_funcFileWriteCallbackName);
        msg->set_funchashwritecallbackname(_funcHashWriteCallbackName);
        msg->set_funcexceptioncallback(_funcExceptionCallback);
        msg->set_funcinitstagename(_initStageFuncName);
        msg->set_funcreleasestagename(_releaseStageFuncName);
        msg->set_resolverowfunctionname(_resolveRowFunctionName);
        msg->set_resolverowwritecallbackname(_resolveRowWriteCallbackName);
        msg->set_resolverowexceptioncallbackname(_resolveRowExceptionCallbackName);
        msg->set_resolvehashcallbackname(_resolveHashCallbackName);
        msg->set_stagenumber(number());

        // file params
        auto& imap = *msg->mutable_inputparameters();
        for(const auto& keyval : _fileInputParameters)
            imap[keyval.first] = keyval.second;
        auto& omap = *msg->mutable_outputparameters();
        for(const auto& keyval : _fileOutputParameters)
            omap[keyval.first] = keyval.second;

        return msg;
    }
#endif

}