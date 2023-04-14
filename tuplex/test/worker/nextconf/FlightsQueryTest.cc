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

    static std::unordered_map<std::string, std::string> lambdaSettings(bool use_hyper) {
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
    static std::unordered_map<std::string, std::string> localWorkerSettings(bool use_hyper) {
        // couple changes
        auto m = lambdaSettings(use_hyper);

        // backend set to worker
        if(use_hyper)
            m["output_path"] = "./local-exp/hyper/flights/output.csv";
        else
            m["output_path"] = "./local-exp/global/flights/output.csv";

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
        auto exp_settings = lambdaSettings(true);
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

    TEST_F(FlightsQuery, FullNewFilterQueryWithAggressiveTimeout) {
        using namespace std;

        // set input/output paths
        auto exp_settings = lambdaSettings(true);
        auto input_pattern = exp_settings["input_path"];
        auto output_path = exp_settings["output_path"];
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();
        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        // set aggressive timeout, so a good amount of Lambda tasks fail (timeout = 10s?)
        co.set("tuplex.aws.lambdaTimeout", "15"); // <-- this is the default setting.

        // deactivate filter promo - causes issue for filter query? -> need to fix that!
        co.set("tuplex.optimizer.filterPromotion", "false");


        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // dump settings
        stringToFile("context_settings.json", ctx.getOptions().toString());

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

    TEST_F(FlightsQuery, FullNewFilterQueryGlobal) {
        using namespace std;

        // set input/output paths
        auto exp_settings = lambdaSettings(false);
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


    TEST_F(FlightsQuery, ExpHereFullNewFilterQuery) {
        using namespace std;

        // set input/output paths
        auto exp_settings = lambdaSettings(true);
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

//
//        // test:
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_11.csv"; // <-- file that does get filtered out
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_2001_11.csv"; // <-- file that doesn't get filtered out, but does require model
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_2004_11.csv"; // <-- file that doesn't get filtered out, but doesn't require model
//
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_2003_01.csv"; // <-- this file should result in 552109 input AND output rows.
//
        input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2001_09.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";
        input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2001_01.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2001_09.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";

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

    TEST_F(FlightsQuery, NewFilterQueryLocalTest) {
        using namespace std;

        // set input/output paths
        auto exp_settings = localWorkerSettings(true);
        auto input_pattern = exp_settings["input_path"];
        auto output_path = exp_settings["output_path"];
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();
        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        // disable optimizer
        //co.set("tuplex.useLLVMOptimizer", "false");

        // disable filter promo
        // co.set("tuplex.optimizer.filterPromotion", "false");

        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // dump settings
        stringToFile("context_settings.json", ctx.getOptions().toString());

        input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2001_01.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";

        //// full input pattern (all files)
        //input_pattern = "/hot/data/flights_all/flights_on_time_performance_*.csv";

        // now perform query...
        auto& ds = ctx.csv(input_pattern, {}, option<bool>::none, option<char>::none, '"', {""}, {}, {}, sm);


        // for 2001_09 following is the error case:
        // (_Constant[i64,value=3],_Constant[i64,value=9],i64,i64,str,i64,i64,str,str,i64,str,str,i64,Option[f64],i64,Option[f64],f64) (hash=1003)
        // -> goes through with other ones, but fails here...
        // why?

        // add function that normalizes features (f-mean) / std
        // and then perform linear model for all of the variables in fill-in-delays function (adjusted)
        // now create extract vec (but only for relevant years!)
        ds.withColumn("features", UDF(extractFeatureVectorCode()))
                .map(UDF(extractFillDelaysCode()))
                .filter(UDF("lambda row: 2000 <= row['year'] <= 2005"))
                .tocsv(output_path);
    }

    TEST_F(FlightsQuery, CarrierListFindLocalTest) {
        using namespace std;

        // set input/output paths
        auto exp_settings = localWorkerSettings(true);
        auto input_pattern = exp_settings["input_path"];
        auto output_path = exp_settings["output_path"];
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();
        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        // disable optimizer
        //co.set("tuplex.useLLVMOptimizer", "false");

        // set auto upcast to false s.t. that correct list typing happens...
        co.set("tuplex.autoUpcast", "false");

        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // dump settings
        stringToFile("context_settings.json", ctx.getOptions().toString());

        input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2001_09.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";

        // now perform query...
        auto& ds = ctx.csv(input_pattern, {}, option<bool>::none, option<char>::none, '"', {""}, {}, {}, sm);

        auto extract_feature_vec_code = "def extract_feature_vector(row):\n"
                                        "    carrier_list = [None, 'EA', 'UA', 'PI', 'NK', 'PS', 'AA', 'NW', 'EV', 'B6', 'HP', 'TW', 'DL', 'OO', 'F9', 'YV',\n"
                                        "                    'TZ', 'US',\n"
                                        "                    'MQ', 'OH', 'HA', 'ML (1)', 'XE', 'G4', 'YX', 'DH', 'AS', 'KH', 'QX', 'CO', 'FL', 'VX', 'PA (1)',\n"
                                        "                    'WN', '9E']\n"
                                        "\n"
                                        "    carrier = carrier_list.index(row['OP_UNIQUE_CARRIER'])\n"
                                        "    return [carrier, 0.0, 0.0, 0.0, 0.0,  0.0, 0.0, 0.0, 0.0,  0.0, 0.0, 0.0, 0.0, None]\n";

        // add function that normalizes features (f-mean) / std
        // and then perform linear model for all of the variables in fill-in-delays function (adjusted)
        // now create extract vec (but only for relevant years!)
        ds.withColumn("features", UDF(extract_feature_vec_code))
                .map(UDF(extractFillDelaysCode()))
                .filter(UDF("lambda row: 2000 <= row['year'] <= 2005"))
                .tocsv(output_path);
    }

    TEST_F(FlightsQuery, LocalTestAll) {
        using namespace std;

        // use this input_pattern to test everything
        std::string input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2001_09.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";

        std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> configs;
        configs.push_back(make_pair(string("hyper with constant folding"), vector<pair<string,string>>({
            make_pair("output_path", "./local-exp/hyper-with-cf/output.csv"),
            make_pair("tuplex.experimental.hyperspecialization", "true"),
            make_pair("tuplex.optimizer.constantFoldingOptimization", "true")
        })));
        configs.push_back(make_pair(string("hyper without constant folding"), vector<pair<string,string>>({
                                                                                                               make_pair("output_path", "./local-exp/hyper-no-cf/output.csv"),
                                                                                                               make_pair("tuplex.experimental.hyperspecialization", "true"),
                                                                                                               make_pair("tuplex.optimizer.constantFoldingOptimization", "false")
                                                                                                       })));

        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY().toPath());

        for(const auto& config: configs) {
            cout<<"Running config: "<<config.first<<endl;
            cout<<"-----------------------------------------------"<<endl;

            // set input/output paths
            auto exp_settings = localWorkerSettings(true);
            for(auto kv : config.second)
                exp_settings[kv.first] = kv.second;
            auto output_path = exp_settings["output_path"];
            SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
            for(const auto& kv : exp_settings)
                if(startsWith(kv.first, "tuplex."))
                    co.set(kv.first, kv.second);

            // creater context according to settings
            Context ctx(co);

            // dump settings
            stringToFile("context_settings.json", ctx.getOptions().toString());

            // now perform query...
            auto& ds = ctx.csv(input_pattern, {}, option<bool>::none,
                               option<char>::none, '"', {""},
                               {}, {}, sm);

            // add function that normalizes features (f-mean) / std
            // and then perform linear model for all of the variables in fill-in-delays function (adjusted)
            // now create extract vec (but only for relevant years!)
            ds.withColumn("features", UDF(extractFeatureVectorCode()))
                    .map(UDF(extractFillDelaysCode()))
                    .filter(UDF("lambda row: 2000 <= row['year'] <= 2005"))
                    .tocsv(output_path);
        }

//        // set input/output paths
//        auto exp_settings = localWorkerSettings(true);
//        auto input_pattern = exp_settings["input_path"];
//        auto output_path = exp_settings["output_path"];
//        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
//        ContextOptions co = ContextOptions::defaults();
//        for(const auto& kv : exp_settings)
//            if(startsWith(kv.first, "tuplex."))
//                co.set(kv.first, kv.second);
//
//        // disable optimizer
//        //co.set("tuplex.useLLVMOptimizer", "false");
//
//        // creater context according to settings
//        Context ctx(co);
//
//        runtime::init(co.RUNTIME_LIBRARY().toPath());
//
//        // dump settings
//        stringToFile("context_settings.json", ctx.getOptions().toString());
//
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2001_09.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";
//
//        // full input pattern (all files)
//        input_pattern = "/hot/data/flights_all/flights_on_time_performance_*.csv";
//
//        // now perform query...
//        auto& ds = ctx.csv(input_pattern, {}, option<bool>::none, option<char>::none, '"', {""}, {}, {}, sm);
//
//
//        // for 2001_09 following is the error case:
//        // (_Constant[i64,value=3],_Constant[i64,value=9],i64,i64,str,i64,i64,str,str,i64,str,str,i64,Option[f64],i64,Option[f64],f64) (hash=1003)
//        // -> goes through with other ones, but fails here...
//        // why?
//
//        // add function that normalizes features (f-mean) / std
//        // and then perform linear model for all of the variables in fill-in-delays function (adjusted)
//        // now create extract vec (but only for relevant years!)
//        ds.withColumn("features", UDF(extractFeatureVectorCode()))
//                .map(UDF(extractFillDelaysCode()))
//                .filter(UDF("lambda row: 2000 <= row['year'] <= 2005"))
//                .tocsv(output_path);
    }

    TEST_F(FlightsQuery, LocalTestAllPythonMode) {
        using namespace std;

        // use this input_pattern to test everything
        std::string input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2001_09.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";

        std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> configs;
        configs.push_back(make_pair(string("hyper with constant folding"), vector<pair<string,string>>({
            make_pair("output_path", "./local-exp/hyper-with-cf/output.csv"),
            make_pair("tuplex.experimental.hyperspecialization", "true"),
            make_pair("tuplex.optimizer.constantFoldingOptimization", "true")
        })));
        configs.push_back(make_pair(string("hyper without constant folding"), vector<pair<string,string>>({
                                                                                                               make_pair("output_path", "./local-exp/hyper-no-cf/output.csv"),
                                                                                                               make_pair("tuplex.experimental.hyperspecialization", "true"),
                                                                                                               make_pair("tuplex.optimizer.constantFoldingOptimization", "false")
                                                                                                       })));

        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY().toPath());

        for(const auto& config: configs) {
            cout<<"Running config: "<<config.first<<endl;
            cout<<"-----------------------------------------------"<<endl;

            // set input/output paths
            auto exp_settings = localWorkerSettings(true);
            for(auto kv : config.second)
                exp_settings[kv.first] = kv.second;
            auto output_path = exp_settings["output_path"];
            SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
            for(const auto& kv : exp_settings)
                if(startsWith(kv.first, "tuplex."))
                    co.set(kv.first, kv.second);

            // use pure python mode (& make sure it works!)
            co.set("tuplex.useInterpreterOnly", "true");

            // creater context according to settings
            Context ctx(co);

            // dump settings
            stringToFile("context_settings.json", ctx.getOptions().toString());

            // now perform query...
            auto& ds = ctx.csv(input_pattern, {}, option<bool>::none,
                               option<char>::none, '"', {""},
                               {}, {}, sm);

            // add function that normalizes features (f-mean) / std
            // and then perform linear model for all of the variables in fill-in-delays function (adjusted)
            // now create extract vec (but only for relevant years!)
            ds.withColumn("features", UDF(extractFeatureVectorCode()))
                    .map(UDF(extractFillDelaysCode()))
                    .filter(UDF("lambda row: 2000 <= row['year'] <= 2005"))
                    .tocsv(output_path);
        }
    }
}



