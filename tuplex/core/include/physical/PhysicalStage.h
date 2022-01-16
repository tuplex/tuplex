//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PHYSICALSTAGE_H
#define TUPLEX_PHYSICALSTAGE_H

#include "Context.h"
#include <JITCompiler.h>
#include <ee/IBackend.h>
#include "ResultSet.h"
#define EOF (-1)
#include <nlohmann/json.hpp>
#include <HistoryServerConnector.h>
#include <logical/LogicalOperator.h>

namespace tuplex {

    class IBackend;\
    class PhysicalStage;
    class PhysicalPlan;
    class LogicalPlan;
    class Context;
    class ResultSet;
    class LogicalOperator;
    class HistoryServerConnector;

    // various sinks/sources/...
    enum class EndPointMode {
        UNKNOWN = 0,
        MEMORY = 1,
        FILE = 2,
        HASHTABLE = 3
    };

    class PhysicalStage {
    private:
        // should have a TaskQueue for Multithreaded execution.
        // further sends info back to user how many tasks finished.

        PhysicalPlan* _plan; //! plan to which this stage belongs to
        std::vector<PhysicalStage*> _predecessors;
        int64_t _number;
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _ecounts; //! exception counts for this stage.
        std::vector<LogicalOperator*> _operators; //! operators belonging to stage.
    protected:
        IBackend* _backend;
        std::shared_ptr<HistoryServerConnector> _historyServer;
    public:
        void setHistoryServer(std::shared_ptr<HistoryServerConnector> hsc) { _historyServer = hsc; }
        PhysicalStage() = delete;
        PhysicalStage(PhysicalPlan *plan, IBackend* backend, int64_t number, std::vector<PhysicalStage*> predecessors=std::vector<PhysicalStage*>()) : _plan(plan), _backend(backend), _number(number), _predecessors(predecessors)   {
            // allow plan/backend to be nullptrs for dummy stage in lambda executor.
        }

        virtual ~PhysicalStage();

        std::vector<LogicalOperator*> operators() const { return _operators; }

        void setOperators(std::vector<LogicalOperator*> operators) { _operators  = operators; }

        std::vector<PhysicalStage*> predecessors() const { return _predecessors; }

        /*!
         * executes physical stage on specific context
         * @param context Context on which to execute physical plan
         */
        virtual void execute(const Context& context);

        int64_t number() const { return _number; }
        int64_t getID() const { return number(); }

        virtual EndPointMode outputMode() const { return EndPointMode::UNKNOWN; }
        virtual EndPointMode inputMode() const { return EndPointMode::UNKNOWN; }

        IBackend* backend() const { return _backend; }

        /*!
         * make this stage dependent on execution of previous stage
         * @param stage
         */
        void dependOn(PhysicalStage* stage) { dependOn(std::vector<PhysicalStage*>{stage}); }

        void dependOn(const std::vector<PhysicalStage*>& stages) {
            assert(_predecessors.empty()); _predecessors = stages;
        }

        /*!
         * return map of (operator ID, exception code) -> counts of exceptions
         * @return map
         */
        virtual std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> exceptionCounts() const {
            return _ecounts;
        }

        /*!
         * set exception counts for this stage
         * @param ecounts
         */
        void setExceptionCounts(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts) { _ecounts = ecounts; }

        /*!
         * return stage's output as resultset
         * @return ResultSet
         */
        virtual std::shared_ptr<ResultSet> resultSet() const = 0;

        /*!
         * returns PhysicalPlan associated with this stage
         * @return
         */
        const PhysicalPlan* plan() const { return _plan; }

        /*!
         * returns the context to which this stage belongs
         * @return Tuplex Context object
         */
        const Context& context() const;

        /*!
         * get json representation of this stage ( no recursive call)
         * @return
         */
        virtual nlohmann::json getJSON() const;

        /*!
         * name of the action who triggers this stage
         */
        virtual std::string actionName() const { return ""; }
    };
}
#endif //TUPLEX_PHYSICALSTAGE_H