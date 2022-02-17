//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <TransformStageExceptionReservoir.h>
#include <cassert>
#include <physical/TransformStage.h>
#include <logical/ResolveOperator.h>

// TODO: put into separate helper file
nlohmann::json stringArrayToJSON(const std::vector<std::string>& strs) {
    std::vector<nlohmann::json> vals;
    vals.reserve(strs.size());
    for(auto s : strs)
        vals.emplace_back(s);
    return nlohmann::json(vals);
}

nlohmann::json rowsToJSONSample(const std::vector<tuplex::Row>& rows) {

    std::vector<nlohmann::json> arr;

    // each row to an array of strings
    for(auto row: rows) {
        auto cols = row.getAsStrings();
        arr.emplace_back(stringArrayToJSON(cols));
    }

    return nlohmann::json(arr);
}


namespace tuplex {
    TransformStageExceptionReservoir::TransformStageExceptionReservoir(const TransformStage *stage, std::vector<LogicalOperator*>& operators, size_t limit) : _limit(limit) {
        assert(stage);
        assert(_limit >= 1);

        // create sample processor
        if(operators.empty())
            return;

        std::vector<int64_t> lastOperatorIDs{operators.front()->getID()};

        for(int i = 1; i < operators.size(); ++i) {
            auto op = operators[i];

            if(op->type() == LogicalOperatorType::RESOLVE) {
                // add to resolve map & add current ID to operators
                for(auto id : lastOperatorIDs) {
                    ResolveOperator* rop = (ResolveOperator*)op;
                    assert(rop);
                    _resolverExists.emplace(std::make_tuple(id, rop->ecCode()));
                }
                lastOperatorIDs.emplace_back(op->getID());
            } else {
                // clear vector
                lastOperatorIDs.clear();
                lastOperatorIDs.emplace_back(op->getID());
            }
        }

        _processor = std::make_unique<SampleProcessor>(operators);
    }


    bool TransformStageExceptionReservoir::resolverExists(int64_t opID, tuplex::ExceptionCode ec) const {
        // try to find in lookup an entry
        return std::find(_resolverExists.begin(),
                      _resolverExists.end(),
                      std::make_tuple(opID, ec)) != _resolverExists.end();
    }

    std::unordered_map<int64_t, size_t> TransformStageExceptionReservoir::getTotalOperatorCounts(int64_t operatorID) {
        // check for each combo whether sample already exist, if not add them!
        std::lock_guard<std::mutex> lock(_mutex);

        std::unordered_map<int64_t, size_t> counts;
        for(auto keyval : _totalExceptionCounts) {
            // first element is operatorID
            if(std::get<0>(keyval.first) == operatorID) {
                int64_t ec = ecToI64(std::get<1>(keyval.first));
                auto it = counts.find(ec);
                if(it == counts.end())
                    counts[ec] = 0;
                counts[ec] += keyval.second;
            }
        }

        return counts;
    }

    bool TransformStageExceptionReservoir::addExceptions(
            const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &ecounts,
            const std::vector<Partition*> &exceptions, bool excludeAvailableResolvers) {
        // check for each combo whether sample already exist, if not add them!
        std::lock_guard<std::mutex> lock(_mutex);

        // check how many samples to process max from the partitions
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> countsToProcess;

        for(auto keyval : ecounts) {

            // check whether to exclude resolvers
            if(excludeAvailableResolvers)
                if(resolverExists(std::get<0>(keyval.first), std::get<1>(keyval.first)))
                    continue;

            // add to total counts
            auto tt = _totalExceptionCounts.find(keyval.first);
            if(tt == _totalExceptionCounts.end())
                _totalExceptionCounts[keyval.first] = 0;
            _totalExceptionCounts[keyval.first] += keyval.second;

            // check whether already exists in local counts for sample
            auto it = _sampleCounts.find(keyval.first);
            if(it == _sampleCounts.end()) {
                // no entry yet, process min of limit and available samples
                countsToProcess[keyval.first] = std::min(keyval.second, _limit);
                assert(keyval.second > 0); // should be more than 0!
            } else {
                // entry there, process remaining samples
                size_t numSamplesExisting = _sampleCounts[keyval.first];
                size_t remainingToProcess = std::min(keyval.second, (_limit - numSamplesExisting));
                countsToProcess[keyval.first] = remainingToProcess;
            }
        }

        // start processing partitions if any found
        if(!countsToProcess.empty())
            sampleExceptions(countsToProcess, exceptions, excludeAvailableResolvers);

        return !countsToProcess.empty();
    }

    std::vector<int64_t> TransformStageExceptionReservoir::getOperatorsToUpdate(
            const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &ecounts) {
        // check for each combo whether sample already exist, if not add them!
        std::lock_guard<std::mutex> lock(_mutex);

        // check how many samples to process max from the partitions
        std::set<int64_t> opIDs;

        for(auto keyval : ecounts) {
            // check whether already exists in local counts
            auto it = _sampleCounts.find(keyval.first);
            auto operatorID = std::get<0>(keyval.first);
            if(it == _sampleCounts.end()) {
               opIDs.insert(operatorID);
            } else {
                // entry there, process remaining samples
                size_t numSamplesExisting = _sampleCounts[keyval.first];
                size_t remainingToProcess = std::min(keyval.second, (_limit - numSamplesExisting));
                if(remainingToProcess > 0)
                    opIDs.insert(operatorID);
            }
        }

        return std::vector<int64_t>(opIDs.begin(), opIDs.end());
    }

    void TransformStageExceptionReservoir::sampleExceptions(
            const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &counts,
            const std::vector<Partition *> &exceptions, bool excludeAvailableResolvers) {
        using namespace std;

        // go through exceptions as long as there are counts
        auto max_to_sample = 0;
        for(auto keyval : counts)
            max_to_sample += keyval.second;

        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts = counts; // copy

        for(auto partition : exceptions) {
//            if(max_to_sample == 0)
//                break;

            const uint8_t* ptr = (uint8_t*)partition->lockRaw();
            auto endptr = ptr + partition->size();
            int64_t num_rows = *((int64_t*)ptr);
            ptr += sizeof(int64_t);

            for(int i = 0; i < num_rows; ++i) {
                // decode exception (same format as in resolvetask)
                auto rowNumber = *((int64_t*)ptr);
                ptr += sizeof(int64_t);
                int64_t ecCode = *((int64_t*)ptr);
                ptr += sizeof(int64_t);
                int64_t operatorID = *((int64_t*)ptr);
                ptr += sizeof(int64_t);
                int64_t eSize = *((int64_t*)ptr);
                ptr += sizeof(int64_t);

                // NOTE: depending on type of exception, data looks different!!!
                // ----> i.e. record schema mismatch exception has raw record here!!!\
                // thus, deserialize Row using special fromExceptionMemory Function
                auto row = Row::fromExceptionMemory(partition->schema(), i64ToEC(ecCode), ptr, endptr - ptr);
                ptr += eSize;

                // TODO: this causes a bug together with the webui...
                // prob. because resolvers are not ignored??
                // if opID/exception code matches trafo stage, process
                auto exKey = make_tuple(operatorID, i64ToEC(ecCode));
                if(ecounts[exKey] > 0 && _processor->getOperator(operatorID)) {
                    // the operator with id operatorID would throw the exception
                    auto es = _processor->generateExceptionSample(row, excludeAvailableResolvers);
                    assert(es.rows.size() == 1);

                    // check if sample exists already, if not add new entry. Else: append row
                    auto it = _samples.find(exKey);
                    if(it == _samples.end())
                        _samples[exKey] = es;
                    else {
                        _samples[exKey].rows.push_back(es.rows.front());
                    }

                    max_to_sample--; // one sample less to process.
                    ecounts[exKey]--;
                }
            }

            partition->unlock();
        }

        // update internal counts
        assert(max_to_sample == 0); // this should hold, else something went wrong...
        for(auto keyval : counts) {
            _sampleCounts[keyval.first] += keyval.second;
        }
    }

    LogicalOperator* TransformStageExceptionReservoir::getOperator(int64_t opID) {
        auto op = _processor->getOperator(opID);
        assert(op);
        return op;
    }

    int64_t TransformStageExceptionReservoir::getOperatorIndex(int64_t opID) {
        auto idx = _processor->getOperatorIndex(opID);
        assert(idx >= 0);
        return idx;
    }

    std::string TransformStageExceptionReservoir::getExceptionMessageAsJSONString(const std::string& jobID, int64_t operatorID) {
        std::lock_guard<std::mutex> lock(_mutex);


        // fetch samples for operator and create message
        std::vector<ExceptionSummary> exceptions;
        ExceptionSummary es;
        for(auto keyval : _samples) {
            // matching?
            auto opID = std::get<0>(keyval.first);
            if(operatorID == opID) {
                auto& sample = _samples[keyval.first];

                es.code = ecToI64(std::get<1>(keyval.first));
                es.count = sample.rows.size();

                auto op = _processor->getOperator(operatorID);

                // special case CSV operator
                if(op->type() == LogicalOperatorType::FILEINPUT) {
                    // here, do not recompute the error but simply display bad input rows to be fixed
                    es.first_row_traceback = "schema mismatch";
                    es.sample = sample.rows; // need to be split up
                    es.sample_column_names = std::vector<std::string>{"row"};
                } else {

                    es.first_row_traceback = sample.first_row_traceback;
                    es.sample = sample.rows;
                    es.sample_column_names = beautifulColumnNames(opID);

                    // special case: MapColumn Resolver OR resolve with MapColumn as parent
                    bool singleArg = false;
                    int singleColumnIndex = -1;
                    if(op->type() == LogicalOperatorType::MAPCOLUMN) {
                        singleArg = true;
                        singleColumnIndex = ((MapColumnOperator*)op)->getColumnIndex();
                    }
                    if(op->type() == LogicalOperatorType::RESOLVE) {
                        auto parent = ((ResolveOperator*)op)->getNormalParent();
                        if(parent->type() == LogicalOperatorType::MAPCOLUMN) {
                            singleArg = true;
                            singleColumnIndex = ((MapColumnOperator*)parent)->getColumnIndex();
                        }
                    }

                    // single arg? only return column with single item!
                    if(singleArg) {
                        std::vector<Row> v;
                        for(auto row : sample.rows) {
                            assert(singleColumnIndex >= 0 && singleColumnIndex < row.getNumColumns());
                            Field f(row.get(singleColumnIndex));
                            v.emplace_back(Row(f));
                        }

                        es.sample = v;
                        es.sample_column_names = std::vector<std::string>{es.sample_column_names[singleColumnIndex]};
                    }
                }

                exceptions.emplace_back(es);
            }
        }

        // ------
        // convert to JSON
        nlohmann::json obj;
        obj["jobid"] = jobID;
        obj["opid"] = "op" + std::to_string(operatorID);


        // from array...
        obj["sample_column_names"] = std::vector<std::string>();

        // correct number of rows
        assert(!exceptions.empty());
        assert(!exceptions.front().sample.empty());

        int num_cols = exceptions.front().sample.front().getNumColumns();

        // send multiple exceptions for one vector
        std::vector<nlohmann::json> excs;
        for(auto es : exceptions) {
            nlohmann::json exc;
            exc["code"] = exceptionCodeToPythonClass(i32ToEC(es.code));
            exc["count"] = es.count;

            // convert rows to sample...
            exc["sample"] = rowsToJSONSample(es.sample);

            exc["first_row_traceback"] = es.first_row_traceback;

            excs.emplace_back(exc);


            // quick hack for number of columns
            assert(!es.sample.empty());
            num_cols = es.sample.front().getNumColumns();
        }

        // construct column strings
        // THIS IS REQUIRED FOR CORRECT DISPLAY OF THE SAMPLE...
        // @Todo: improve this
        std::vector<std::string> col_names;
        for(unsigned i = 0; i < num_cols; ++i)
            col_names.emplace_back("column" + std::to_string(i));


        if(exceptions.front().sample_column_names.empty())
            obj["sample_column_names"] = stringArrayToJSON(col_names);
        else
            obj["sample_column_names"] = stringArrayToJSON(exceptions.front().sample_column_names);

        obj["exceptions"] = excs;

        return obj.dump();
    }

    std::vector<std::string> TransformStageExceptionReservoir::getParentsColumnNames(int64_t operatorID) {

        auto op = getOperator(operatorID);
        if(!op)
            return std::vector<std::string>();

        auto parent = op->parent(); assert(parent);

        if(parent) {
            auto dsptr = parent->getDataSet();
            if(!dsptr) {
                // invalid dsptr means, operator was produced by context.
                // i.e., directly return column names from operator
                switch(parent->type()) {
                    case LogicalOperatorType::PARALLELIZE: {
                        auto pllop = (ParallelizeOperator*)parent;
                        return pllop->columns();
                    }
                    case LogicalOperatorType::FILEINPUT: {
                        auto csvop = (FileInputOperator*)parent;
                        return csvop->columns();
                    }

                    default:
                        return std::vector<std::string>();
                }
            }
            return dsptr->columns();
        }

        return std::vector<std::string>();
    }

    std::vector<std::string> TransformStageExceptionReservoir::beautifulColumnNames(int64_t operatorID) {
        auto columns = getParentsColumnNames(operatorID);

        // empty? beautify!
        if(columns.empty()) {
            auto op = getOperator(operatorID);
            if(op) {
                // op is valid
                assert(op);
                switch(op->type()) {
                    case LogicalOperatorType::MAP:
                    case LogicalOperatorType::FILTER:
                    case LogicalOperatorType::WITHCOLUMN: {
                        // these ops all take a column.
                        // ==> check whether single arg or multi arg
                        auto udfop = dynamic_cast<UDFOperator*>(op); assert(udfop);
                        auto params = udfop->getUDF().getInputParameters();
                        if(params.size() == 1) {
                            // tuple? or single arg?
                            auto row_type = std::get<1>(params.front());
                            if(row_type.isTupleType()) {
                                for(int i = 0; i < row_type.parameters().size(); ++i) {
                                    columns.push_back(std::get<0>(params.front()) + "[" + std::to_string(i) + "]");
                                }
                            } else columns.push_back(std::get<0>(params.front()));
                        } else {
                            for(auto param : params) {
                                columns.push_back(std::get<0>(param));
                            }
                        }

                        break;
                    }
                    default:
                        // others ops have no cols?
                        break;
                }
            }
        }
        return columns;
    }

}