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

// fetch github data via:
// aws s3 cp s3://tuplex-public/data/github_daily ./github_daily --recursive


namespace tuplex {
    class GithubQuery : public tuplex::PyTest {
    };

    static std::unordered_map<std::string, std::string> lambdaSettings(bool use_hyper) {
        std::unordered_map<std::string, std::string> m;


        // shared settings
        m["input_path"] = "s3://tuplex-public/data/github_daily/*.json";

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
        m["tuplex.aws.scratchDir"] = "s3://tuplex-leonhard/scratch/github-exp";

        // for github use smaller split size
        m["tuplex.inputSplitSize"] = "64MB"; // tiny tasks?

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
            m["output_path"] = "s3://tuplex-leonhard/experiments/github_hyper_new/hyper/";
        } else {
            m["output_path"] = "s3://tuplex-leonhard/experiments/github_hyper_new/global/";
        }

        return m;
    }

    // use local worker test settings
    static std::unordered_map<std::string, std::string> localWorkerSettings(bool use_hyper) {
        // couple changes
        auto m = lambdaSettings(use_hyper);

        // backend set to worker
        if(use_hyper)
            m["output_path"] = "./local-exp/hyper/github/output.csv";
        else
            m["output_path"] = "./local-exp/global/github/output.csv";

        m["tuplex.backend"] = "worker";
        m["input_path"] = "/hot/data/github_daily/*.json";

        // overwrite scratch dir (aws)
        m["tuplex.aws.scratchDir"] = "./local-exp/scratch";

        return m;
    }


    TEST_F(GithubQuery, BasicForkRequests) {
        using namespace std;

        // set input/output paths
        auto exp_settings = localWorkerSettings(true); // lambdaSettings(true);
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


        // start pipeline incl. output
        auto repo_id_code = "def extract_repo_id(row):\n"
                            "\tif 2012 <= row['year'] <= 2014:\n"
                            "\t\treturn row['repository']['id']\n"
                            "\telse:\n"
                            "\t\treturn row['repo']['id']\n";
        ctx.json(input_pattern, true, true, sm)
        .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
        .withColumn("repo_id", UDF(repo_id_code))
        .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
        .map(UDF("lambda t: (t['type'],t['repo_id'],t['year'])"))
        .tocsv(output_path);
    }

    TEST_F(GithubQuery, ForkEventsExtended) {
        using namespace std;

        // set input/output paths
        auto exp_settings = lambdaSettings(true); // localWorkerSettings(true); //
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


        // start pipeline incl. output
        auto repo_id_code = "def extract_repo_id(row):\n"
                                       "    if 2012 <= row['year'] <= 2014:\n"
                                       "        \n"
                                       "        if row['type'] == 'FollowEvent':\n"
                                       "            return row['payload']['target']['id']\n"
                                       "        \n"
                                       "        if row['type'] == 'GistEvent':\n"
                                       "            return row['payload']['id']\n"
                                       "        \n"
                                       "        repo = row.get('repository')\n"
                                       "        \n"
                                       "        if repo is None:\n"
                                       "            return None\n"
                                       "        return repo.get('id')\n"
                                       "    else:\n"
                                       "        return row['repo'].get('id')";
        ctx.json(input_pattern, true, true, sm)
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // workaround for filter promo.
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
//                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // @TODO: make this work in filter promo.
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                .tocsv(output_path);
    }

    TEST_F(GithubQuery, ForkEventsFilterPromoForEachFile) {
        using namespace std;

        python::unlockGIL();

        // set input/output paths
        auto exp_settings = localWorkerSettings(true); //
        string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";
        string output_path = "./local-exp/filter-promo/github/output";
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();

        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        bool use_hyper = true;
        co.set("tuplex.experimental.hyperspecialization", boolToString(use_hyper));
        co.set("tuplex.optimizer.filterPromotion", "true");
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        co.set("tuplex.optimizer.constantFoldingOptimization", "true");

        // make testing faster...
        co.set("tuplex.resolveWithInterpreterOnly", "true");

        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // start pipeline incl. output
        auto repo_id_code = "def extract_repo_id(row):\n"
                            "    if 2012 <= row['year'] <= 2014:\n"
                            "        \n"
                            "        if row['type'] == 'FollowEvent':\n"
                            "            return row['payload']['target']['id']\n"
                            "        \n"
                            "        if row['type'] == 'GistEvent':\n"
                            "            return row['payload']['id']\n"
                            "        \n"
                            "        # this here doesn't work, because no fancy typed row object yet\n"
                            "        # repo = row.get('repository')\n"
                            "        repo = row['repository']\n"
                            "        \n"
                            "        if repo is None:\n"
                            "            return None\n"
                            "        return repo.get('id')\n"
                            "    else:\n"
                            "        return row['repo'].get('id')";

        // // @TODO: this here should be the proper pipeline
        // ctx.json(input_pattern, true, true, sm)
        //         .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
        //         .withColumn("repo_id", UDF(repo_id_code))
        //         .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
        //         .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
        //         .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
        //         .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
        //         .tocsv(output_path);


        auto paths = glob(input_pattern);
        std::cout<<"Found "<<pluralize(paths.size(), "path")<<std::endl;

        for(const auto& path : paths) {
            std::cout<<"Checking "<<path<<std::endl;
            // single test-file here:
            //input_pattern = "../resources/hyperspecialization/github_daily/2012-10-15.json.sample";

            // fixed pipeline here b.c. canPromoteFilterCheck is incomplete yet...
            ctx.json(path, true, true, sm)
                    .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                    .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                    .withColumn("repo_id", UDF(repo_id_code))
                    .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                    .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                    .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                    .tocsv(output_path);
        }
    }

    TEST_F(GithubQuery, ForkEventsFilterPromoAllSamples) {
        using namespace std;

        // set input/output paths
        auto exp_settings = localWorkerSettings(true); //
        string input_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";
        string output_path = "./local-exp/filter-promo/github/output";
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();

        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);

        bool use_hyper = true;
        co.set("tuplex.experimental.hyperspecialization", boolToString(use_hyper));
        co.set("tuplex.optimizer.filterPromotion", "true");
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        co.set("tuplex.optimizer.constantFoldingOptimization", "true");

        // make testing faster...
        co.set("tuplex.resolveWithInterpreterOnly", "true");

        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // start pipeline incl. output
        auto repo_id_code = "def extract_repo_id(row):\n"
                            "    if 2012 <= row['year'] <= 2014:\n"
                            "        \n"
                            "        if row['type'] == 'FollowEvent':\n"
                            "            return row['payload']['target']['id']\n"
                            "        \n"
                            "        if row['type'] == 'GistEvent':\n"
                            "            return row['payload']['id']\n"
                            "        \n"
                            "        # this here doesn't work, because no fancy typed row object yet\n"
                            "        # repo = row.get('repository')\n"
                            "        repo = row['repository']\n"
                            "        \n"
                            "        if repo is None:\n"
                            "            return None\n"
                            "        return repo.get('id')\n"
                            "    else:\n"
                            "        return row['repo'].get('id')";

        // // @TODO: this here should be the proper pipeline
        // ctx.json(input_pattern, true, true, sm)
        //         .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
        //         .withColumn("repo_id", UDF(repo_id_code))
        //         .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
        //         .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
        //         .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
        //         .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
        //         .tocsv(output_path);

        // single test-file here:
        //input_pattern = "../resources/hyperspecialization/github_daily/2012-10-15.json.sample";

        // fixed pipeline here b.c. canPromoteFilterCheck is incomplete yet...
        ctx.json(input_pattern, true, true, sm)
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                .tocsv(output_path);

    }
}


