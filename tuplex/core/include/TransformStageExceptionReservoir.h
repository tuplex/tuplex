//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TRANSFORMSTAGEEXCEPTIONRESERVOIR_H
#define TUPLEX_TRANSFORMSTAGEEXCEPTIONRESERVOIR_H

#include <physical/SampleProcessor.h>

namespace tuplex {

    class LogicalOperator;
    class Executor;
    class SampleProcessor;
    class Partition;
    class PhysicalPlan;
    class TransformStage;

    /// class to hold exception samples for WebUI, obtained via stageprocessor
    class TransformStageExceptionReservoir {
    public:
        TransformStageExceptionReservoir(const TransformStage* stage, const std::vector<std::shared_ptr<LogicalOperator>>& operators, size_t limit=5);

        bool resolverExists(int64_t opID, ExceptionCode ec) const;

        bool addExceptions(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts,
                           const std::vector<Partition*> &exceptions,
                           bool excludeAvailableResolvers);

        std::shared_ptr<LogicalOperator> getOperator(int64_t opID);
        int64_t getOperatorIndex(int64_t opID);

        /*!
         * return list of operator ids which would get updated
         * @param counts
         * @return
         */
        std::vector<int64_t> getOperatorsToUpdate(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts);

        std::string getExceptionMessageAsJSONString(const std::string& jobID, int64_t operatorID);

        /*!
         * return total exception counts for a specific operator
         * @param operatorID
         * @return  counts clustered after exception code
         */
        std::unordered_map<int64_t, size_t> getTotalOperatorCounts(int64_t operatorID);
    private:
        size_t                              _limit; //! limit of exceptions to hold per opID/ecCode combination
        std::unique_ptr<SampleProcessor>    _processor; //! sample processor belonging to this stage

        // map to store whether resolver for operator/exception code exists
        std::set<std::tuple<int64_t, ExceptionCode>> _resolverExists;

        // write needs to be protected when using multi-threading
        std::mutex _mutex;

        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _sampleCounts; // counts of samples stored.
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _totalExceptionCounts; // total counts of exceptions (determined via addExceptions)
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, ExceptionSample> _samples; // exception samples.

        /*!
         * samples exceptions depending on counts & updated internal counters
         * @param counts
         * @param exceptions
         * @param excludeAvailableResolvers
         */
        void sampleExceptions(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& counts,
                              const std::vector<Partition*> &exceptions,
                              bool excludeAvailableResolvers);



        // helper structs to hold exception messages
        struct ExceptionSummary {
            int64_t code;
            int64_t count;
            std::vector<Row> sample;
            std::string first_row_traceback;
            std::vector<std::string> sample_column_names;
        };

        std::vector<std::string> getParentsColumnNames(int64_t operatorID);

        std::vector<std::string> beautifulColumnNames(int64_t operatorID);

    };
}
#endif //TUPLEX_TRANSFORMSTAGEEXCEPTIONRESERVOIR_H