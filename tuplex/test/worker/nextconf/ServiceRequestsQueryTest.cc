#include "TestUtils.h"


namespace tuplex {

     class ServiceRequestsQuery : public tuplex::PyTest {};

    DataSet& serviceRequestsPipeline(Context& ctx, const std::string& service_path) {
        using namespace std;


        // do this:
        // ------------
        // but low priority.


        // Prio1: exploring the dictionary optimization
        // Prio1b: add more time wise logging to Tuplex.... (i.e. compute time / IO)
        // Prio2: finishing the null-value optimization
        //        (might need more than one day, might not be worth it) // Prio2: the 311 sample query
        // Prio3: where the time is going...
        //


        // notice the null check at the beginning...
        auto fix_zip_codes_c = "def fix_zip_codes(zips):\n"
                               "    if not zips:\n"
                               "         return None\n"
                               "    # Truncate everything to length 5 \n"
                               "    s = zips[:5]\n"
                               "    \n"
                               "    # Set 00000 zip codes to nan\n"
                               "    if s == '00000':\n"
                               "         return None\n"
                               "    else:\n"
                               "         return s";

        // TODO: Need to force type hint here as string to make above code executable
        // ==> other version should be super simple and just require fix of bad records via resolve...

        // force type of column to string, Incident Zip is column #8!
        return ctx.csv(service_path, vector<string>{},
                option<bool>::none,option<char>::none, '"',
                vector<string>{"Unspecified", "NO CLUE", "NA", "N/A", "0", ""},
                unordered_map<size_t, python::Type>{{8, python::Type::makeOptionType(python::Type::STRING)}})
        .mapColumn("Incident Zip", UDF(fix_zip_codes_c))
        .selectColumns(vector<string>{"Incident Zip"})
        .unique();
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
        // m["tuplex.sample.strataSize"] = "1024";
        // m["tuplex.sample.samplesPerStrata"]="1";
        // m["tuplex.sample.maxDetectionMemory"] = "32MB";
        // m["tuplex.sample.maxDetectionRows"] = "30000";

        // use lambda settings & keep above settings original to Tuplex (cheap client-side sampling)
        m["tuplex.lambda.sample.strataSize"] = "1024";
        m["tuplex.lambda.sample.samplesPerStrata"]="1";
        m["tuplex.lambda.sample.maxDetectionMemory"] = "32MB";
        m["tuplex.lambda.sample.maxDetectionRows"] = "30000";

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

    TEST_F(ServiceRequestsQuery, Basic311Test) {
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

        // resource pattern
        input_pattern = "../resources/pipelines/311/311-service-requests.sample.csv";

        // now perform query...
        auto& ds = serviceRequestsPipeline(ctx, input_pattern);

        ds.show();
    }
}