//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FULLPIPELINES_H
#define TUPLEX_FULLPIPELINES_H

#include <DataSet.h>
#include <Context.h>
#include "TestUtils.h"

namespace tuplex {
    // put in here the full pipelines as designed for benchmarking for easier debugging...
    extern DataSet& flightPipeline(Context& ctx,
            const std::string& bts_path="../resources/pipelines/flights/flights_on_time_performance_2019_01.10k-sample.csv",
            const std::string carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv",
            const std::string& airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt",
            bool cache=false,
            bool withIgnores=false);

    extern DataSet& apacheLogsPipeline(Context& ctx,
            const std::string& logs_path="../resources/pipelines/logs/logs.csv");

    extern DataSet& zillowPipeline(Context& ctx,
                                   const std::string& zillow_path="../resources/pipelines/zillow/zillow_noexc.csv",
                                   bool cache=false);

    extern DataSet& serviceRequestsPipeline(Context& ctx, const std::string& service_path="../resources/pipelines/311/311-service-requests.sample.csv");

    extern std::vector<std::string> pipelineAsStrs(DataSet& ds);
}
#endif //TUPLEX_FULLPIPELINES_H