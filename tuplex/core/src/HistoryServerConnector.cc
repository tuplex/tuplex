//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <HistoryServerConnector.h>
#include <StringUtils.h>
#include <logical/UDFOperator.h>
#include <logical/ResolveOperator.h>
#include <logical/MapColumnOperator.h>
#include <physical/PhysicalPlan.h>
#include <physical/TransformStage.h>

// fix for EOF (should be defined in cstdio)
#ifndef EOF
#define EOF     (-1)
#endif
#include <nlohmann/json.hpp>



#include <logical/FileInputOperator.h>
#include <logical/ParallelizeOperator.h>

using json=nlohmann::json;

namespace tuplex {

    std::string base_uri(const std::string& host, uint16_t port) {
        if(host.rfind("http://", 0) != 0)
            return "http://" + host + ":" + std::to_string(port);
        else
            return host + ":" + std::to_string(port);
    }

    std::string mongo_uri(const std::string& mongo_host, uint16_t mongo_port)  {
        // starts with mongodb?
        if(mongo_host.rfind("mongodb://", 0) == 0)
            return mongo_host + ":" + std::to_string(mongo_port) + "/tuplex-history";
        else
            return "mongodb://" + mongo_host + ":" + std::to_string(mongo_port) + "/tuplex-history";
    }

    HistoryServerConnection HistoryServerConnector::connect(const std::string &host, uint16_t port,
                                                                   const std::string &mongo_host,
                                                                   uint16_t mongo_port) {
        auto& logger = Logger::instance().logger("history server");

        HistoryServerConnection hsc;
        // fit url
        hsc.host = host;
        hsc.port = port;
        hsc.db_host = mongo_host;
        hsc.db_port = mongo_port;
        hsc.connected = false;


        // validate by GET request to /api/version
        RESTInterface ri;

        auto url = base_uri(hsc.host, hsc.port) + "/api/version";
        auto response = ri.get(url);

        if(response.empty()) {
            logger.warn("could not connect to " + url + ", if you wish to disable the webui consider setting tuplex.webui=False for the context.");
            return hsc;
        }

        try {
            // try to extract json
            auto obj = json::parse(response);

            auto r_mongo_host = obj["mongodb"]["host"].get<std::string>();
            auto r_mongo_port = obj["mongodb"]["port"].get<int64_t>() & 0xFFFF;
            auto r_version = obj["version"].get<std::string>();

            // compare mongoDB configs
            if(r_mongo_host.compare(mongo_host) != 0 || r_mongo_port != mongo_port) {

                //mongodb://localhost:27017/tuplex-history
                logger.warn("MongoDB uri of running Tuplex WebUI " + mongo_uri(mongo_host, mongo_port) +
                            " differs from submitted uri " + mongo_uri(r_mongo_host, r_mongo_port) + ". "
                                                                                   "Defaulting back to MongoDB instance from WebUI");

                hsc.db_host = r_mongo_host;
                hsc.db_port = r_mongo_port;
            }
            hsc.connected = true;
            logger.info("connected to history server running under " + base_uri(hsc.host, hsc.port));
        } catch(const json::parse_error& jse) {
            //logger.error(std::string("internal json error, details: ") + jse.what());
        }

        return hsc;
    }

    std::shared_ptr<HistoryServerConnector> HistoryServerConnector::registerNewJob(const tuplex::HistoryServerConnection &conn,
                                                                  const std::string &contextName,
                                                                  const PhysicalPlan* plan,
                                                                  const tuplex::ContextOptions &options,
                                                                  unsigned int maxExceptions) {
        auto& logger = Logger::instance().logger("history server");

        // post job json
        // note: this might be a large request and thus quite slow

        json job;
        job["status"] = jobStatusToString(JobStatus::SCHEDULED);
        job["started"] = "";
        job["submitted"] = currentISO8601TimeUTC();
        job["finished"] = "";

        // tuplex.env. variables auto populated...
        // --> i.e. add them to the context options...

        json context;
        context["user"] = options.get("tuplex.env.user");
        context["name"] = contextName;
        context["mode"] = options.get("tuplex.env.mode");
        context["host"] = options.get("tuplex.env.hostname");

        if(options.containsKey("tuplex.env.jupyter_url"))
            context["jupyter_url"] = options.get("tuplex.env.jupyter_url");

        // last stage id
        job["lastStageId"] = plan->lastStageID();
        job["action"] = plan->actionName();

        // push options into config
        auto store = options.store();
        json config;
        for(auto entry : store) {
            config[entry.first] = entry.second;
        }
        context["config"] = config;

        job["context"] = context;
        // use stage builtin function to generate json with representation of staged operators
        job["stages"] = plan->getStagedRepresentationAsJSON();

        json obj;
        obj["job"] = job;

        // add operators...
        std::vector<json> ops;
        assert(plan);
        plan->foreachStage([&](const PhysicalStage* stage) {
            for(auto op: stage->operators()) {
                json val;
                val["name"] = op->name();
                val["id"] = "op" + std::to_string(op->getID());
                val["columns"] = std::vector<std::string>();
                val["stageid"] = stage->getID();
                if(hasUDF(op)) {
                    UDFOperator *udfop = (UDFOperator*)op;
                    assert(udfop);

                    val["udf"] = udfop->getUDF().getCode();
                } else if (op->type() == LogicalOperatorType::AGGREGATE) {
                    AggregateOperator *udfop = (AggregateOperator*)op;
                    val["combiner_udf"] = udfop->combinerUDF().getCode();
                    val["aggregator_udf"] = udfop->aggregatorUDF().getCode();
                }
                ops.push_back(val);
            }
        });

        obj["operators"] = ops;

        // post
        RESTInterface ri;
        auto response = ri.postJSON(base_uri(conn.host, conn.port) + "/api/job", obj.dump());

        if(response.empty()) {
            logger.warn("Could not register job, is history server running? To disable this warning,"
                         " set webui=False in the context configuration.");
            return nullptr;
        } else {
            logger.info("notifying history server of new job");
        }


        std::string jobID, track_url;

        // extract json from response
        try {
            // try to extract json
            obj = json::parse(response);

            jobID = obj["job"]["id"].get<std::string>();
            track_url = obj["job"]["track_url"].get<std::string>();

            logger.info("history server registered new job under id " + jobID);

        } catch(const json::parse_error& jse) {
            logger.error(std::string("internal json error while attempting to decode jobID, details: ") + jse.what());
            return nullptr;
        }


        // create HistoryServerConnector object
        return std::shared_ptr<HistoryServerConnector>(new HistoryServerConnector(conn, jobID, contextName,
                track_url, options.WEBUI_EXCEPTION_DISPLAY_LIMIT(), plan, maxExceptions));
    }

    HistoryServerConnector::HistoryServerConnector(const tuplex::HistoryServerConnection &conn,
                                                   const std::string &jobID,
                                                   const std::string &contextName,
                                                   const std::string &trackURL,
                                                   size_t exceptionDisplayLimit,
                                                   const PhysicalPlan* plan,
                                                   unsigned int maxExceptions) : _jobID(jobID),
                                                                                 _contextName(contextName),
                                                                                 _trackURL(trackURL),
                                                                                 _conn(conn),
                                                                                 _exceptionDisplayLimit(exceptionDisplayLimit) {
        // init lookup map for resolvers
        initResolverLookupMap(plan);
    }

    void HistoryServerConnector::initResolverLookupMap(const PhysicalPlan* plan) {

        // not yet implemented...

//        assert(plan);
//
//        // go through each stage
//        plan->foreachStage([this](const PhysicalStage* stage) {
//            assert(stage);
//
//            // is trafo stage?
//            const TransformStage* tstage = nullptr;
//            if(tstage = dynamic_cast<const TransformStage*>(stage)) {
//                auto operators = tstage->operators();
//                if(operators.empty())
//                    return;
//                auto reservoir = std::make_shared<TransformStageExceptionReservoir>(tstage, _exceptionDisplayLimit);
//
//                for(auto& op : operators)
//                    _reservoirLookup[op->getID()] = reservoir;
//                _reservoirs.emplace_back(reservoir);
//            }
//        });
        assert(plan);

        // go through each stage
        plan->foreachStage([this](const PhysicalStage* stage) {
            assert(stage);

            // is trafo stage?
            const TransformStage* tstage = nullptr;
            if((tstage = dynamic_cast<const TransformStage*>(stage))) {
                auto operators = tstage->operators();
                if(operators.empty())
                    return;
                auto reservoir = std::make_shared<TransformStageExceptionReservoir>(tstage, operators, _exceptionDisplayLimit);

                for(const auto& op : operators)
                    _reservoirLookup[op->getID()] = reservoir;
                _reservoirs.emplace_back(reservoir);
            }
        });
    }

    void HistoryServerConnector::sendStatus(tuplex::JobStatus status, unsigned num_open_tasks, unsigned num_finished_tasks) {
        json obj;

        obj["time"] = currentISO8601TimeUTC();
        obj["jobid"] = _jobID;
        obj["status"] = jobStatusToString(status);


        json progress;
        json tasks;
        tasks["open"] = num_open_tasks;
        tasks["done"] = num_finished_tasks;
        progress["tasks"] = tasks;

        obj["progress"] = progress;


        // perform post request
        // post request to /api/job
        RESTInterface ri;
        auto response = ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/job/update", obj.dump());
    }

    void HistoryServerConnector::sendStatus(tuplex::JobStatus status) {
        json obj;

        obj["time"] = currentISO8601TimeUTC();
        obj["jobid"] = _jobID;
        obj["status"] = jobStatusToString(status);

        // perform post request
        // post request to /api/job
        RESTInterface ri;
        auto response = ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/job/update", obj.dump());
    }

    void HistoryServerConnector::sendExceptionCounts(int64_t opID, const std::unordered_map<int64_t, size_t> &ecounts) {

        json obj;
        obj["jobid"] = _jobID;
        obj["opid"] = "op" + std::to_string(opID);//json::value::number(opID);

        // detailed counts
        json detailed_ecounts;
        auto num_exceptions = 0;
        for(auto kv : ecounts) {
            auto code = exceptionCodeToPythonClass(i32ToEC(kv.first));
            detailed_ecounts[code] = (int64_t)kv.second;
            num_exceptions += kv.second;
        }

        obj["detailed_ecounts"] = detailed_ecounts;
        obj["ecount"] = num_exceptions;
        obj["ncount"] = 0;

        RESTInterface ri;
        ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/operator", obj.dump());
    }

    void HistoryServerConnector::sendTrafoTask(int stageID, int64_t num_input_rows, int64_t num_output_rows,
                                               const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts,
                                               const std::vector<Partition*> &exceptions,
                                               bool excludeAvailableResolvers) {
        using namespace std;

        auto num_normal_rows = num_output_rows;
        int64_t num_exception_rows = 0;
        for(auto keyval : ecounts)
            num_exception_rows += keyval.second;


        // step 1: simple task done message
        // first send simple, task done message
        RESTInterface ri;

        json obj;
        obj["jobid"] = _jobID;
        obj["icount"] = num_input_rows;
        obj["ncount"] = num_normal_rows;
        obj["ecount_raw"] = num_exception_rows;
        obj["stageid"] = stageID;

        auto num_unresolved_exceptions = 0;

        if(excludeAvailableResolvers) {
            for(auto keyval : ecounts) {
                int64_t operatorID = std::get<0>(keyval.first);
                ExceptionCode ec = std::get<1>(keyval.first);

               if(resolverExists(operatorID, ec))
                    continue;

                num_unresolved_exceptions += keyval.second;
            }
        } else num_unresolved_exceptions = num_exception_rows;

        // count ecount without resolvers
        obj["ecount"] = num_unresolved_exceptions;

        // perform post request
        ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/task", obj.dump());

        // step 2: messages with details
        // add them to the exception reservoirs
        // --> check to which stage they belong!
        if(!ecounts.empty()) {
            auto it = ecounts.begin();
            assert(it != ecounts.end());
            auto opID = std::get<0>(it->first);
            auto opsToUpdate = _reservoirLookup[opID]->getOperatorsToUpdate(ecounts);
            if(_reservoirLookup[opID]->addExceptions(ecounts, exceptions, excludeAvailableResolvers)) {
                // change in sampling?
                // ==> get for all ops the exception message and send it!
                for(auto id : opsToUpdate) {
                    auto msg = _reservoirLookup[opID]->getExceptionMessageAsJSONString(_jobID, id);
                    ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/exception", msg);

                    // send accumulated exception counts
                    sendExceptionCounts(id, _reservoirLookup[opID]->getTotalOperatorCounts(id));
                }
            }
        }
    }

    bool HistoryServerConnector::resolverExists(int64_t opID, tuplex::ExceptionCode ec) {
//        // is there a resolver? then skip counts...
//        if (std::find(_resolverExists.begin(),
//                      _resolverExists.end(),
//                      std::make_tuple(operatorID, ec)) != _resolverExists.end())

        // go through Trafo Stage reservoirs
        for(auto& reservoir : _reservoirs)
            if(reservoir->resolverExists(opID, ec))
                return true;

        return false;
    }


//    bool HistoryServerConnector::addExceptionSamples(int64_t operatorID, const ExceptionCode& ec, size_t ecCount, const std::vector<Partition*>& exceptions, size_t limit, bool excludeAvailableResolvers) {
//
//        auto sampleProcessor = getStageProcessor(operatorID);
//        if(!sampleProcessor) {
//#ifndef NDEBUG
//            Logger::instance().defaultLogger().warn("unknown operator without sampleProcessor seen");
//#endif
//            return false;
//        }
//
//        // lock mutex over messages
//        std::unique_lock<std::mutex> lock(_exceptionLock);
//
//        // check whether opID is contained within vector
//        auto it = std::find_if(_exceptionMessages.begin(), _exceptionMessages.end(), [operatorID](const ExceptionMessage& em) {
//            return operatorID == em.operatorID;
//        });
//
//        if(it == _exceptionMessages.end()) {
//
//            // checks whether there are exceptions for which no resolver in the path exists
//            bool unresolvedExceptions = false;
//
//            // new entry for this operator. I.e. send full message.
//            ExceptionMessage em;
//            em.operatorID = operatorID;
//
//            // because it is a new message, simply add a ExceptionSummary for this pair
//            // first, check whether resolver exists for opID + exceptionCode
//            if(excludeAvailableResolvers && resolverExists(operatorID, ec))
//               return false;
//
//            unresolvedExceptions = true;
//            ExceptionSummary es;
//            es.code = ecToI64(ec);
//            es.count = ecCount;
//
//            auto op = sampleProcessor->getOperator(operatorID);
//
//            // use sample processor to reconstruct error sample (stateless nature of UDFs!!!)
//            auto sample_inrows = exceptionPartitionsToRows(exceptions, _exceptionDisplayLimit);
//
//            // special case CSV operator
//            if(op->type() == LogicalOperatorType::CSV) {
//                // here, do not recompute the error but simply display bad input rows to be fixed
//                es.first_row_traceback = "schema mismatch";
//                es.sample = sample_inrows; // need to be split up
//                es.sample_column_names = std::vector<std::string>{"row"};
//            } else {
//                auto sp_res = sampleProcessor->generateExceptionSample(sample_inrows, opID);
//
//                es.first_row_traceback = sp_res.first_row_traceback;
//                es.sample = sp_res.rows;
//                es.sample_column_names = getParentsColumnNames(opID);
//
//                // special case: MapColumn Resolver OR resolve with MapColumn as parent
//                bool singleArg = false;
//                int singleColumnIndex = -1;
//                if(op->type() == LogicalOperatorType::MAPCOLUMN) {
//                    singleArg = true;
//                    singleColumnIndex = ((MapColumnOperator*)op)->getColumnIndex();
//                }
//                if(op->type() == LogicalOperatorType::RESOLVE) {
//                    auto parent = ((ResolveOperator*)op)->getNormalParent();
//                    if(parent->type() == LogicalOperatorType::MAPCOLUMN) {
//                        singleArg = true;
//                        singleColumnIndex = ((MapColumnOperator*)parent)->getColumnIndex();
//                    }
//                }
//
//                // single arg? only return column with single item!
//                if(singleArg) {
//                    std::vector<Row> v;
//                    for(auto row : sp_res.rows) {
//                        assert(singleColumnIndex >= 0 && singleColumnIndex < row.getNumColumns());
//                        Field f(row.get(singleColumnIndex));
//                        v.emplace_back(Row(f));
//                    }
//
//                    es.sample = v;
//                    es.sample_column_names = std::vector<std::string>{es.sample_column_names[singleColumnIndex]};
//                }
//            }
//
//            em.exceptions.emplace_back(es);
//
//            // // place into list & send info
//            // if(unresolvedExceptions) {
//            //     _exceptionMessages.emplace_back(em);
//            //     em.sendDetails(_jobID, base_uri(_conn.host, _conn.port) + "/api/exceptioMultin");
//            // }
//
//        } else {
//            ExceptionMessage& em = *it;
//            bool sendUpdate = false;
//
//            // update entry & send message
//            for(auto keyVal : ecMap) {
//                int64_t ec = keyVal.first;
//
//                // check whether resolver exists for opID + exceptionCode
//                if(excludeAvailableResolvers && std::find(_resolverExists.begin(),
//                                                          _resolverExists.end(),
//                                                          std::make_tuple(opID, ec)) != _resolverExists.end())
//                    continue;
//
//                // for each ec in the current _exceptionMessages message, check if limit was reached.
//                // else supplement by current exceptions
//                auto jt = std::find_if(em.exceptions.begin(), em.exceptions.end(), [ec](const ExceptionSummary& es) {
//                    return es.code == ec;
//                });
//
//                // entry for this exception code already there?
//                // if not add
//                // else supplement
//                if(jt == em.exceptions.end()) {
//                    int64_t count = 0;
//                    for(auto p : keyVal.second) {
//                        count += p->getNumRows();
//                    }
//
//                    ExceptionSummary es;
//                    es.code = ec;
//                    es.count = count;
//
//                    // use sample processor to reconstruct error sample (stateless nature of UDFs!!!)
//                    auto sample_inrows = exceptionPartitionsToRows(keyVal.second, _exceptionDisplayLimit);
//                    auto sp_res = sampleProcessor->generateExceptionSample(sample_inrows, opID);
//
//                    es.first_row_traceback = sp_res.first_row_traceback;
//                    es.sample = sp_res.rows;
//                    es.sample_column_names = sampleProcessor->getColumnNames(opID);
//
//                    em.exceptions.emplace_back(es);
//                    sendUpdate = true;
//                } else {
//                    // avoids computing first row traceback...
//                    assert(jt->count > 0);
//
//                    // check how many exceptions there are, if less than limit, supplement
//                    if(jt->count < _exceptionDisplayLimit) {
//                        // use sample processor to reconstruct error sample (stateless nature of UDFs!!!)
//                        auto sample_inrows = exceptionPartitionsToRows(keyVal.second, _exceptionDisplayLimit - jt->count);
//                        auto sp_res = sampleProcessor->generateExceptionSample(sample_inrows, opID);
//
//                        jt->count += sample_inrows.size();
//                        jt->sample.insert(jt->sample.end(), sample_inrows.begin(), sample_inrows.end());
//                        sendUpdate = true;
//                    }
//                }
//            }
//            if(sendUpdate)
//                em.sendDetails(_jobID, base_uri(_conn.host, _conn.port) + "/api/exception");
//        }
//
//        return true;
//    }


    void HistoryServerConnector::sendStagePlan(const std::string &stageName, const std::string &unoptimizedIR,
                                               const std::string &optimizedIR, const std::string &assemblyCode) {
        // step 1: simple task done message
        // first send simple, task done message
        RESTInterface ri;

        // @todo: produces exception when dumping non-utf8 string. May occur when usng pickled functions in llvm ir...

        try {
            json stageObj;
            stageObj["name"] = stageName;
            stageObj["unoptimizedIR"] = unoptimizedIR;
            stageObj["optimizedIR"] = optimizedIR;
            stageObj["assembly"] = assemblyCode;

            json obj;
            obj["jobid"] = _jobID;
            obj["stage"] = stageObj;

            ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/plan", obj.dump());
        } catch(nlohmann::json::type_error& e) {
            Logger::instance().defaultLogger().warn(std::string("non-UTF8 characters encountered: ") + e.what());

            json stageObj;
            stageObj["name"] = stageName;
            stageObj["unoptimizedIR"] = "non-utf8 char error";
            stageObj["optimizedIR"] = "non-utf8 char error";
            stageObj["assembly"] = "non-utf8 char error";

            json obj;
            obj["jobid"] = _jobID;
            obj["stage"] = stageObj;

            ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/plan", obj.dump());
        }
    }

    // sends results for ONE stage. I.e. the final one.
    void HistoryServerConnector::sendStageResult(int64_t stageNumber, size_t numInputRows, size_t numOutputRows,
                                                 const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &ecounts) {
        RESTInterface ri;
        json msg;

        msg["stageid"] = stageNumber;
        msg["jobid"] = _jobID;
        msg["inputRowCount"] = numInputRows;
        msg["outputRowCount"] = numOutputRows;

        size_t exceptionCount = 0;
        std::vector<json> opCounts;
        for(auto keyval : ecounts) {
            exceptionCount += keyval.second;

            json j;
            auto opid = std::get<0>(keyval.first);
            j["opid"] = opid;
            j["idx"] = getOperatorIndex(opid); // needed to join properly!
            j["count"] = keyval.second;
            auto ec = std::get<1>(keyval.first);
            j["code"] = ec;
            j["class"] = exceptionCodeToPythonClass(ec);

            opCounts.push_back(j);
        }

        msg["exceptionCounts"] = opCounts;
        msg["ecount"] = exceptionCount;
        msg["ncount"] = numOutputRows;

        ri.postJSON(base_uri(_conn.host, _conn.port) + "/api/stage/result", msg.dump());
    }

    LogicalOperator* HistoryServerConnector::getOperator(int64_t opID) {

        if(_reservoirLookup.end() == _reservoirLookup.find(opID))
            return nullptr;

        // get stage processor
        return _reservoirLookup[opID]->getOperator(opID);
    }

    int64_t HistoryServerConnector::getOperatorIndex(int64_t opID) {

        if(_reservoirLookup.end() == _reservoirLookup.find(opID))
            return -1;

        // get stage processor
        return _reservoirLookup[opID]->getOperatorIndex(opID);
    }

}