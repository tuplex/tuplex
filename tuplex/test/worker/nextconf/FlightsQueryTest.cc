//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2023, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on  3/5/2023                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include "TestUtils.h"

namespace tuplex {
    class FlightsQuery : public tuplex::PyTest {};


    // helper functions to create the query (from python files)
    std::string extractFeatureVectorCode() {
        std::string path = "../resources/python/nextconf/flights/extractFeatures.py";
        auto code = fileToString(path);
        assert(!code.empty());
        return code;
    }

    std::string extractAssembleRowCode() {
        std::string path = "../resources/python/nextconf/flights/assembleRow.py";
        auto code = fileToString(path);
        assert(!code.empty());
        return code;
    }

    std::string extractFillDelaysCode() {
        std::string path = "../resources/python/nextconf/flights/fillDelays.py";
        auto code = fileToString(path);
        assert(!code.empty());
        return code;
    }

    std::unordered_map<std::string, std::string> lambdaSettings(bool use_hyper) {
        std::unordered_map<std::string, std::string> m;


        // shared settings
        m["input_path"] = "s3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv";

        // use stratified sampling
        m["tuplex.aws.httpThreadCount"] = std::to_string(410);
        m["tuplex.aws.maxConcurrency"] = std::to_string(410);
        m["tuplex.aws.lambdaMemory"] = "10000";
        m["tuplex.aws.lambdaThreads"] = "3";

        m["tuplex.autoUpcast"] = "True";
        m["tuplex.experimental.hyperspecialization"] = boolToString(use_hyper);
        m["tuplex.executorCount"] = std::to_string(0);
        m["tuplex.backend"] = "lambda";
        m["tuplex.webui.enable"] = "False";
        m["tuplex.driverMemory"] = "2G";
        m["tuplex.partitionSize"] = "32MB";
        m["tuplex.runTimeMemory"] = "32MB";
        m["tuplex.useLLVMOptimizer"] = "True";
        m["tuplex.optimizer.generateParser"] = "False";
        m["tuplex.optimizer.nullValueOptimization"] = "True";
        m["tuplex.optimizer.constantFoldingOptimization"] = "True";
        m["tuplex.optimizer.selectionPushdown"] = "True";
        m["tuplex.experimental.forceBadParseExceptFormat"] = "False";
        m["tuplex.resolveWithInterpreterOnly"] = "False";
        m["tuplex.inputSplitSize"] = "2GB";
        m["tuplex.experimental.opportuneCompilation"] = "True";
        m["tuplex.aws.scratchDir"] = "s3://tuplex-leonhard/scratch/flights-exp";

        // sampling settings incl.
        // stratified sampling (to make things work & faster)
        m["tuplex.sample.strataSize"] = "1024";
        m["tuplex.sample.samplesPerStrata"]="1";
        m["tuplex.sample.maxDetectionMemory"] = "32MB";
        m["tuplex.sample.maxDetectionRows"] = "30000";
        auto sampling_mode = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;
        m["sampling_mode"] = std::to_string(sampling_mode);

        // hyper vs. general specific settings
        if(use_hyper) {
            m["output_path"] = "s3://tuplex-leonhard/experiments/flights_hyper_new/hyper/";
        } else {
            m["output_path"] = "s3://tuplex-leonhard/experiments/flights_hyper_new/global/";
        }

        return m;
    }

    // use local worker test settings
    std::unordered_map<std::string, std::string> localWorkerSettings(bool use_hyper) {
        // couple changes
        auto m = lambdaSettings(use_hyper);

        // backend set to worker
        if(use_hyper)
            m["output_path"] = "./local-exp/hyper/output.csv";
        else
            m["output_path"] = "./local-exp/global/output.csv";

        m["tuplex.backend"] = "worker";
        m["input_path"] = "/hot/data/flights_all/flights*.csv";

        return m;
    }


    TEST_F(FlightsQuery, CodeNotEmpty) {
        auto udf_extractFeatures = extractFeatureVectorCode();
        ASSERT_FALSE(udf_extractFeatures.empty());

        auto udf_assembleRow = extractAssembleRowCode();
        ASSERT_FALSE(udf_assembleRow.empty());
    }

    TEST_F(FlightsQuery, FullNewFilterQuery) {
        using namespace std;

        // set input/output paths
        auto exp_settings = localWorkerSettings(true); //lambdaSettings(true);
        auto input_pattern = exp_settings["input_path"];
        auto output_path = exp_settings["output_path"];
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();
        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // dump settings
        stringToFile("context_settings.json", ctx.getOptions().toString());


        // test:
        input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_11.csv"; // <-- file that doesn't get filtered out
        input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_11.csv"; // <-- file that doesn't get filtered out

        // now perform query...
        auto& ds = ctx.csv(input_pattern, {}, option<bool>::none, option<char>::none, '"', {""}, {}, {}, sm);

        // add function that normalizes features (f-mean) / std
        // and then perform linear model for all of the variables in fill-in-delays function (adjusted)
        // now create extract vec (but only for relevant years!)
        ds.withColumn("features", UDF(extractFeatureVectorCode()))
          .map(UDF(extractFillDelaysCode()))
          .filter(UDF("lambda row: 2000 <= row['year'] <= 2005"))
          .tocsv(output_path);
    }
}



