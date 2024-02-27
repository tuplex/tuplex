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
        m["tuplex.experimental.opportuneCompilation"] = "True";
        m["tuplex.aws.scratchDir"] = "s3://tuplex-leonhard/scratch/github-exp";

        // for github use smaller split size? --> need to test/check!
        m["tuplex.inputSplitSize"] = "64MB"; // tiny tasks?

        // sampling settings incl.
        // stratified sampling (to make things work & faster)
        m["tuplex.sample.strataSize"] = "1024";
        m["tuplex.sample.samplesPerStrata"]="10";
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

        // Notes: slow path compilation seems to be a problem, fix by using
        // https://en.cppreference.com/w/cpp/thread/future
        // and https://en.cppreference.com/w/cpp/thread/future to wait for compile-thread for slow code to finish?
        // if it doesn't finish in time, use interpreter only?
        // or is there a way to reduce complexity of slow code? E.g., cJSON dict instead of struct dict?
        // -> this could be faster...

        using namespace std;

        // set input/output paths
        // auto exp_settings = lambdaSettings(true);
        auto exp_settings = localWorkerSettings(true); //
        auto input_pattern = exp_settings["input_path"];
        auto output_path = exp_settings["output_path"];
        SamplingMode sm = static_cast<SamplingMode>(stoi(exp_settings["sampling_mode"]));
        ContextOptions co = ContextOptions::defaults();
        for(const auto& kv : exp_settings)
            if(startsWith(kv.first, "tuplex."))
                co.set(kv.first, kv.second);


        // --> slow path is SUPER SLOW to compile. need to improve, use this here to make testing faster.
        // make testing faster...
        // co.set("tuplex.resolveWithInterpreterOnly", "true");

        // let's iterate over a few split sizes
        // co.set("tuplex.inputSplitSize", "32M");

        co.set("tuplex.inputSplitSize", "20G");
        co.set("tuplex.experimental.worker.workerBufferSize", "12G"); // each normal, exception buffer in worker get 3G before they start spilling to disk!

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
                // .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // workaround for filter promo.
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'")) // <-- this is challenging to push down.
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

    TEST_F(GithubQuery, ForkEventsFilterPromoDebug2012) {
        using namespace std;

        // set input/output paths
        auto exp_settings = localWorkerSettings(true); //
        string input_pattern = "../resources/hyperspecialization/github_daily/2011*.json.sample,../resources/hyperspecialization/github_daily/2012*.json.sample,../resources/hyperspecialization/github_daily/2021*.json.sample";
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
        // co.set("tuplex.resolveWithInterpreterOnly", "true");

        // creater context according to settings
        Context ctx(co);

        runtime::init(co.RUNTIME_LIBRARY().toPath());

        // this here seems to work, so it's something about not getting the inference right...
        // input_pattern = "../resources/hyperspecialization/github_daily/2012*.json.sample";

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


        // setting 1: no-hyper, no filter-promo
        co.set("tuplex.experimental.hyperspecialization", boolToString(false));
        co.set("tuplex.optimizer.filterPromotion", "false"); // <-- seems to work.
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        // deactivate, does not work for struct field yet.
        // -> easy to fix though.
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.inputSplitSize", "2GB");
        co.set("tuplex.resolveWithInterpreterOnly", "false");
        // -> works, should have
        // num_input_rows                                              11012665
        // num_output_rows                                               294195
        // num_bad_rows                                                       0

        // setting 2: hyper, no filter-promo
        co.set("tuplex.experimental.hyperspecialization", boolToString(true));
        co.set("tuplex.optimizer.filterPromotion", "false"); // <-- seems to work.
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        // deactivate, does not work for struct field yet.
        // -> easy to fix though.
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.inputSplitSize", "2GB");
        co.set("tuplex.resolveWithInterpreterOnly", "false");

        // setting 3: hyper, filter-promo
        co.set("tuplex.experimental.hyperspecialization", boolToString(true));
        co.set("tuplex.optimizer.filterPromotion", "true"); // <-- seems to work.
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        // deactivate, does not work for struct field yet.
        // -> easy to fix though.
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.inputSplitSize", "2GB");
        co.set("tuplex.resolveWithInterpreterOnly", "false");
        // result of this is:
        // [2023-04-12 12:04:55.841] [worker] [info] total input row count: 11012665
        // [2023-04-12 12:04:55.841] [worker] [info] total output row count: 294195

        // // setting 4: pure python mode (for establishing a baseline)
        // co.set("tuplex.useInterpreterOnly", "true");

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

        stringToFile("extract_repo_id.py", repo_id_code);

        // check full data
        input_pattern = "/hot/data/github_daily/*.json";

        // fixed pipeline here b.c. canPromoteFilterCheck is incomplete yet...
        ctx.json(input_pattern, true, true, sm)
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .selectColumns(vector<string>{"type", "repo_id", "year", "number_of_commits"})
                .tocsv(output_path);

        // perform sanity check with pure python solution that ouput number of rows is correct.
        // std::cout<<v.size()<<std::endl;


        // long compile times for general-case are due to:
        // https://github.com/llvm/llvm-project/issues/52858
        // https://github.com/llvm/llvm-project/issues/53826
        // https://github.com/llvm/llvm-project/issues/53826
        // this here is another issue...
        // https://reviews.llvm.org/D65482
    }

    TEST_F(GithubQuery, ForkEventsFilterPromoAllSamplesExtended) {
        // @TODO: this here is the pipeline to get to work...
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


        // setting 1: no-hyper, no filter-promo
        co.set("tuplex.experimental.hyperspecialization", boolToString(false));
        co.set("tuplex.optimizer.filterPromotion", "false"); // <-- seems to work.
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        // deactivate, does not work for struct field yet.
        // -> easy to fix though.
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.inputSplitSize", "2GB");
        co.set("tuplex.resolveWithInterpreterOnly", "false");
        // -> works, should have
        // num_input_rows                                              11012665
        // num_output_rows                                               294195
        // num_bad_rows                                                       0

        // setting 2: hyper, no filter-promo
        co.set("tuplex.experimental.hyperspecialization", boolToString(true));
        co.set("tuplex.optimizer.filterPromotion", "false"); // <-- seems to work.
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        // deactivate, does not work for struct field yet.
        // -> easy to fix though.
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.inputSplitSize", "2GB");
        co.set("tuplex.resolveWithInterpreterOnly", "false");

        // setting 3: hyper, filter-promo
        co.set("tuplex.experimental.hyperspecialization", boolToString(true));
        co.set("tuplex.optimizer.filterPromotion", "true"); // <-- seems to work.
        co.set("tuplex.optimizer.nullValueOptimization", "true");
        // deactivate, does not work for struct field yet.
        // -> easy to fix though.
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.inputSplitSize", "2GB");
        co.set("tuplex.resolveWithInterpreterOnly", "false");


        co.set("tuplex.executorCount", "0"); // <-- uncomment if there are any multi-threading issues

        // result of this is:
        // [2023-04-12 12:04:55.841] [worker] [info] total input row count: 11012665
        // [2023-04-12 12:04:55.841] [worker] [info] total output row count: 294195

        // // setting 4: pure python mode (for establishing a baseline)
        // co.set("tuplex.useInterpreterOnly", "true");

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
                            "        return row['repo'].get('id')\n";

        auto extract_forkee = "def extract_forkee(row):\n"
                              "    if 2011 == row['year']:\n"
                              "        return row['actor']['login']\n"
                              "    else:\n"
                              "        return row['actor']\n";

        auto extract_forkee_url = "def extract_forkee_url(row):\n"
                                  "    if 2011 == row['year']:\n"
                                  "        return row['actor']['url']\n"
                                  "    elif row['year'] < 2015:\n"
                                  "        return row['url']\n"
                                  "    else:\n"
                                  "        return row['actor']['url']";
        auto extract_watchers = "def extract_watchers(row):\n"
                                "    # before 2012-08-06 watchers are stargazers, and no stat about watchers is available!\n"
                                "    month = int(row['created_at'].split('-')[1])\n"
                                "    day = int(row['created_at'].split('-')[1])\n"
                                "        \n"
                                "    if 2011 == row['year']:\n"
                                "        return None\n"
                                "    elif row['year'] < 2015:\n"
                                "        if row['year'] == 2012 and month * 100 + day < 806:\n"
                                "            return None\n"
                                "        else:\n"
                                "            return row['repository']['watchers']\n"
                                "    else:\n"
                                "        row['payload']['forkee']['watchers_count']";
        auto extract_stargazers = "def extract_stargazers(row):\n"
                                  "    # before 2012-08-06 watchers are stargazers, and no stat about watchers is available!\n"
                                  "    month = int(row['created_at'].split('-')[1])\n"
                                  "    day = int(row['created_at'].split('-')[1])\n"
                                  "        \n"
                                  "    if 2011 == row['year']:\n"
                                  "        return None\n"
                                  "    elif row['year'] < 2015:\n"
                                  "        if row['year'] == 2012 and month * 100 + day < 806:\n"
                                  "            return row['repository']['watchers']\n"
                                  "        else:\n"
                                  "            return row['repository'].get('stargazers') \n"
                                  "    else:\n"
                                  "            return row['payload']['forkee']['stargazers_count']";
        auto extract_forks = "def extract_forks(row):\n"
                             "    if 2011 == row['year']:\n"
                             "        return row['payload']['forkee']['forks']\n"
                             "    elif row['year'] < 2015:\n"
                             "        return row['repository']['forks']\n"
                             "    else:\n"
                             "        return row['payload']['forkee']['forks']";
        auto extract_lang = "def extract_lang(row):\n"
                            "    if 2011 == row['year']:\n"
                            "        return row['payload']['forkee']['language']\n"
                            "    elif row['year'] < 2015:\n"
                            "        # no information collected...\n"
                            "        return None\n"
                            "    else:\n"
                            "        return row['payload']['forkee']['language']";

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
        std::stringstream udf_code_stream;
        udf_code_stream<<"# code for UDFs used in github query\n";
        udf_code_stream<<repo_id_code<<"\n";
        udf_code_stream<<extract_forkee<<"\n";
        udf_code_stream<<extract_forkee_url<<"\n";
        udf_code_stream<<extract_watchers<<"\n";
        udf_code_stream<<extract_stargazers<<"\n";
        udf_code_stream<<extract_forks<<"\n";
        udf_code_stream<<extract_lang<<"\n";

        stringToFile("github_udfs.py", udf_code_stream.str());

        // // check full data
        // input_pattern = "/hot/data/github_daily/*.json";
//
//        // add functionality to debug/determine reason what went wrong?
//        // fixed pipeline here b.c. canPromoteFilterCheck is incomplete yet...
//        ctx.json(input_pattern, true, true, sm)
//                .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
//                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
//                        //.withColumn("repo_id", UDF(repo_id_code))
//                        //.withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
//                        //.withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
//                        //.withColumn("forkee", UDF(extract_forkee))
//                        //.withColumn("forkee_url", UDF(extract_forkee_url))
//                .withColumn("watchers", UDF(extract_watchers))
//                        //.withColumn("stargazers", UDF(extract_stargazers))
//                        //.withColumn("forks", UDF(extract_forks))
//                        //.withColumn("lang", UDF(extract_lang))
//                .selectColumns(vector<string>{"type", "year", "watchers"})
////                .selectColumns(vector<string>{"type", "repo_id", "year",
////                              "number_of_commits", "forkee", "forkee_url", "watchers"})
//                        // .selectColumns(vector<string>{"type", "repo_id", "year",
//                        //                              "number_of_commits", "lang", "forkee", "forkee_url",
//                        //                              "watchers",
//                        //                              "stargazers", "forks"})
//                .tocsv(output_path);


// #error "investigate forkee url and what is going wrong here. Maybe create separate example."

        // problematic isolated pipeline
        ctx.json(input_pattern, true, true, sm)
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("forkee_url", UDF(extract_forkee_url)) // <-- problematic, need to investigate & fix
                .selectColumns(vector<string>{"type", "year", "forkee_url"})
                .tocsv(output_path);

        return;

        // full pipeline (fix func by func)
        ctx.json(input_pattern, true, true, sm)
                .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                .withColumn("repo_id", UDF(repo_id_code))
                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                .withColumn("forkee", UDF(extract_forkee))
                .withColumn("forkee_url", UDF(extract_forkee_url)) // <-- problematic, need to investigate & fix
//                .withColumn("watchers", UDF(extract_watchers))
//                .withColumn("stargazers", UDF(extract_stargazers))
//                .withColumn("forks", UDF(extract_forks))
//                .withColumn("lang", UDF(extract_lang))
//                .selectColumns(vector<string>{"type", "repo_id", "year",
//                                                     "number_of_commits", "lang", "forkee", "forkee_url",
//                                                     "watchers",
//                                                     "stargazers", "forks"})
                .selectColumns(vector<string>{"type", "year", "repo_id", "number_of_commits", "forkee", "forkee_url"})
                .tocsv(output_path);

//        // full pipeline (fix func by func)
//        ctx.json(input_pattern, true, true, sm)
//                .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
//                .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
//                .withColumn("repo_id", UDF(repo_id_code))
//                .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
//                .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
//                .withColumn("forkee", UDF(extract_forkee))
//                .withColumn("forkee_url", UDF(extract_forkee_url))
//                .withColumn("watchers", UDF(extract_watchers))
//                .withColumn("stargazers", UDF(extract_stargazers))
//                .withColumn("forks", UDF(extract_forks))
//                .withColumn("lang", UDF(extract_lang))
//                .selectColumns(vector<string>{"type", "repo_id", "year",
//                                              "number_of_commits", "lang", "forkee", "forkee_url",
//                                              "watchers",
//                                              "stargazers", "forks"})
//                .tocsv(output_path);
    }

    TEST_F(GithubQuery, ExtractWatchersTest) {

        // the order seems here wrong.
        // -> introduce Row type to solve all of these issues better?
        // row type could come in handy also for groupby.

        auto input_row_type_str = "(Option[str],Option[Struct[(str,'avatar_url'=>str),(str,'display_login'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)]],Option[str],Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'comment'=>Struct[(str,'author_association'=>str),(str,'body'=>str),(str,'commit_id'=>str),(str,'created_at'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'line'=>null),(str,'node_id'=>str),(str,'path'=>null),(str,'position'=>null),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'distinct'=>bool),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'distinct_size'=>i64),(str,'forkee'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'name'=>str),(str,'full_name'=>str),(str,'private'=>bool),(str,'owner'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'html_url'=>str),(str,'description'=>str),(str,'fork'=>bool),(str,'url'=>str),(str,'forks_url'=>str),(str,'keys_url'=>str),(str,'collaborators_url'=>str),(str,'teams_url'=>str),(str,'hooks_url'=>str),(str,'issue_events_url'=>str),(str,'events_url'=>str),(str,'assignees_url'=>str),(str,'branches_url'=>str),(str,'tags_url'=>str),(str,'blobs_url'=>str),(str,'git_tags_url'=>str),(str,'git_refs_url'=>str),(str,'trees_url'=>str),(str,'statuses_url'=>str),(str,'languages_url'=>str),(str,'stargazers_url'=>str),(str,'contributors_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'commits_url'=>str),(str,'git_commits_url'=>str),(str,'comments_url'=>str),(str,'issue_comment_url'=>str),(str,'contents_url'=>str),(str,'compare_url'=>str),(str,'merges_url'=>str),(str,'archive_url'=>str),(str,'downloads_url'=>str),(str,'issues_url'=>str),(str,'pulls_url'=>str),(str,'milestones_url'=>str),(str,'notifications_url'=>str),(str,'labels_url'=>str),(str,'releases_url'=>str),(str,'deployments_url'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'pushed_at'=>str),(str,'git_url'=>str),(str,'ssh_url'=>str),(str,'clone_url'=>str),(str,'svn_url'=>str),(str,'homepage'=>str),(str,'size'=>i64),(str,'stargazers_count'=>i64),(str,'watchers_count'=>i64),(str,'language'=>null),(str,'has_issues'=>bool),(str,'has_projects'=>bool),(str,'has_downloads'=>bool),(str,'has_wiki'=>bool),(str,'has_pages'=>bool),(str,'forks_count'=>i64),(str,'mirror_url'=>null),(str,'archived'=>bool),(str,'disabled'=>bool),(str,'open_issues_count'=>i64),(str,'license'=>null),(str,'allow_forking'=>bool),(str,'is_template'=>bool),(str,'topics'=>[]),(str,'visibility'=>str),(str,'forks'=>i64),(str,'open_issues'=>i64),(str,'watchers'=>i64),(str,'default_branch'=>str),(str,'public'=>bool)]),(str,'head'=>str),(str,'issue'=>Struct[(str,'assignee'=>null),(str,'body'=>str),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'created_at'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels'=>List[Struct[(str,'name'->str),(str,'url'->str),(str,'color'->str)]]),(str,'milestone'=>Option[Struct[(str,'number'->i64),(str,'created_at'->str),(str,'due_on'->str),(str,'title'->str),(str,'creator'->Struct[(str,'gravatar_id'->str),(str,'avatar_url'->str),(str,'url'->str),(str,'id'->i64),(str,'login'->str)]),(str,'url'->str),(str,'open_issues'->i64),(str,'closed_issues'->i64),(str,'description'->str),(str,'state'->str)]]),(str,'number'=>i64),(str,'pull_request'=>Struct[(str,'diff_url'=>null),(str,'patch_url'=>null),(str,'html_url'=>null)]),(str,'state'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'gravatar_id'=>str),(str,'avatar_url'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)])]),(str,'legacy'=>Struct[(str,'comment_id'=>i64),(str,'head'=>str),(str,'issue_id'=>i64),(str,'push_id'=>i64),(str,'ref'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64)]),(str,'push_id'=>i64),(str,'ref'=>str),(str,'size'=>i64)]],Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],i64,i64,List[Struct[(str,'sha'->str),(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'url'->str),(str,'message'->str)]],i64,str)\n";
        auto input_row_type = python::Type::decode(input_row_type_str);

        std::vector<std::string> input_columns({"type", "public", "actor", "created_at", "payload", "id", "repo", "org", "year", "repo_id"});

        // input row type:  ((Option[str],Option[Struct[(str,'avatar_url'=>str),(str,'display_login'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)]],Option[str],Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'email'->str),(str,'name'->str)]),(str,'distinct'=>bool),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'description'=>str),(str,'distinct_size'=>i64),(str,'forkee'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'name'=>str),(str,'full_name'=>str),(str,'private'=>bool),(str,'owner'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'html_url'=>str),(str,'description'=>str),(str,'fork'=>bool),(str,'url'=>str),(str,'forks_url'=>str),(str,'keys_url'=>str),(str,'collaborators_url'=>str),(str,'teams_url'=>str),(str,'hooks_url'=>str),(str,'issue_events_url'=>str),(str,'events_url'=>str),(str,'assignees_url'=>str),(str,'branches_url'=>str),(str,'tags_url'=>str),(str,'blobs_url'=>str),(str,'git_tags_url'=>str),(str,'git_refs_url'=>str),(str,'trees_url'=>str),(str,'statuses_url'=>str),(str,'languages_url'=>str),(str,'stargazers_url'=>str),(str,'contributors_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'commits_url'=>str),(str,'git_commits_url'=>str),(str,'comments_url'=>str),(str,'issue_comment_url'=>str),(str,'contents_url'=>str),(str,'compare_url'=>str),(str,'merges_url'=>str),(str,'archive_url'=>str),(str,'downloads_url'=>str),(str,'issues_url'=>str),(str,'pulls_url'=>str),(str,'milestones_url'=>str),(str,'notifications_url'=>str),(str,'labels_url'=>str),(str,'releases_url'=>str),(str,'deployments_url'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'pushed_at'=>str),(str,'git_url'=>str),(str,'ssh_url'=>str),(str,'clone_url'=>str),(str,'svn_url'=>str),(str,'homepage'=>str),(str,'size'=>i64),(str,'stargazers_count'=>i64),(str,'watchers_count'=>i64),(str,'language'=>null),(str,'has_issues'=>bool),(str,'has_projects'=>bool),(str,'has_downloads'=>bool),(str,'has_wiki'=>bool),(str,'has_pages'=>bool),(str,'forks_count'=>i64),(str,'mirror_url'=>null),(str,'archived'=>bool),(str,'disabled'=>bool),(str,'open_issues_count'=>i64),(str,'license'=>null),(str,'allow_forking'=>bool),(str,'is_template'=>bool),(str,'topics'=>[]),(str,'visibility'=>str),(str,'forks'=>i64),(str,'open_issues'=>i64),(str,'watchers'=>i64),(str,'default_branch'=>str),(str,'public'=>bool)]),(str,'head'=>str),(str,'legacy'=>Struct[(str,'head'=>str),(str,'size'=>i64),(str,'push_id'=>i64),(str,'shas'=>List[List[str]]),(str,'ref'=>str)]),(str,'master_branch'=>str),(str,'number'=>i64),(str,'pull_request'=>Struct[(str,'url'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'html_url'=>str),(str,'diff_url'=>str),(str,'patch_url'=>str),(str,'issue_url'=>str),(str,'number'=>i64),(str,'state'=>str),(str,'locked'=>bool),(str,'title'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'body'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'closed_at'=>null),(str,'merged_at'=>null),(str,'merge_commit_sha'=>null),(str,'assignee'=>null),(str,'assignees'=>[]),(str,'requested_reviewers'=>[]),(str,'requested_teams'=>[]),(str,'labels'=>[]),(str,'milestone'=>null),(str,'draft'=>bool),(str,'commits_url'=>str),(str,'review_comments_url'=>str),(str,'review_comment_url'=>str),(str,'comments_url'=>str),(str,'statuses_url'=>str),(str,'head'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'sha'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'repo'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'name'=>str),(str,'full_name'=>str),(str,'private'=>bool),(str,'owner'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'html_url'=>str),(str,'description'=>str),(str,'fork'=>bool),(str,'url'=>str),(str,'forks_url'=>str),(str,'keys_url'=>str),(str,'collaborators_url'=>str),(str,'teams_url'=>str),(str,'hooks_url'=>str),(str,'issue_events_url'=>str),(str,'events_url'=>str),(str,'assignees_url'=>str),(str,'branches_url'=>str),(str,'tags_url'=>str),(str,'blobs_url'=>str),(str,'git_tags_url'=>str),(str,'git_refs_url'=>str),(str,'trees_url'=>str),(str,'statuses_url'=>str),(str,'languages_url'=>str),(str,'stargazers_url'=>str),(str,'contributors_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'commits_url'=>str),(str,'git_commits_url'=>str),(str,'comments_url'=>str),(str,'issue_comment_url'=>str),(str,'contents_url'=>str),(str,'compare_url'=>str),(str,'merges_url'=>str),(str,'archive_url'=>str),(str,'downloads_url'=>str),(str,'issues_url'=>str),(str,'pulls_url'=>str),(str,'milestones_url'=>str),(str,'notifications_url'=>str),(str,'labels_url'=>str),(str,'releases_url'=>str),(str,'deployments_url'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'pushed_at'=>str),(str,'git_url'=>str),(str,'ssh_url'=>str),(str,'clone_url'=>str),(str,'svn_url'=>str),(str,'homepage'=>null),(str,'size'=>i64),(str,'stargazers_count'=>i64),(str,'watchers_count'=>i64),(str,'language'=>str),(str,'has_issues'=>bool),(str,'has_projects'=>bool),(str,'has_downloads'=>bool),(str,'has_wiki'=>bool),(str,'has_pages'=>bool),(str,'forks_count'=>i64),(str,'mirror_url'=>null),(str,'archived'=>bool),(str,'disabled'=>bool),(str,'open_issues_count'=>i64),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'allow_forking'=>bool),(str,'is_template'=>bool),(str,'topics'=>[]),(str,'visibility'=>str),(str,'forks'=>i64),(str,'open_issues'=>i64),(str,'watchers'=>i64),(str,'default_branch'=>str)])]),(str,'base'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'sha'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'repo'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'name'=>str),(str,'full_name'=>str),(str,'private'=>bool),(str,'owner'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'html_url'=>str),(str,'description'=>str),(str,'fork'=>bool),(str,'url'=>str),(str,'forks_url'=>str),(str,'keys_url'=>str),(str,'collaborators_url'=>str),(str,'teams_url'=>str),(str,'hooks_url'=>str),(str,'issue_events_url'=>str),(str,'events_url'=>str),(str,'assignees_url'=>str),(str,'branches_url'=>str),(str,'tags_url'=>str),(str,'blobs_url'=>str),(str,'git_tags_url'=>str),(str,'git_refs_url'=>str),(str,'trees_url'=>str),(str,'statuses_url'=>str),(str,'languages_url'=>str),(str,'stargazers_url'=>str),(str,'contributors_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'commits_url'=>str),(str,'git_commits_url'=>str),(str,'comments_url'=>str),(str,'issue_comment_url'=>str),(str,'contents_url'=>str),(str,'compare_url'=>str),(str,'merges_url'=>str),(str,'archive_url'=>str),(str,'downloads_url'=>str),(str,'issues_url'=>str),(str,'pulls_url'=>str),(str,'milestones_url'=>str),(str,'notifications_url'=>str),(str,'labels_url'=>str),(str,'releases_url'=>str),(str,'deployments_url'=>str),(str,'created_at'=>str),(str,'updated_at'=>str),(str,'pushed_at'=>str),(str,'git_url'=>str),(str,'ssh_url'=>str),(str,'clone_url'=>str),(str,'svn_url'=>str),(str,'homepage'=>null),(str,'size'=>i64),(str,'stargazers_count'=>i64),(str,'watchers_count'=>i64),(str,'language'=>str),(str,'has_issues'=>bool),(str,'has_projects'=>bool),(str,'has_downloads'=>bool),(str,'has_wiki'=>bool),(str,'has_pages'=>bool),(str,'forks_count'=>i64),(str,'mirror_url'=>null),(str,'archived'=>bool),(str,'disabled'=>bool),(str,'open_issues_count'=>i64),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'allow_forking'=>bool),(str,'is_template'=>bool),(str,'topics'=>[]),(str,'visibility'=>str),(str,'forks'=>i64),(str,'open_issues'=>i64),(str,'watchers'=>i64),(str,'default_branch'=>str)])]),(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'issue'=>Struct[(str,'href'=>str)]),(str,'comments'=>Struct[(str,'href'=>str)]),(str,'review_comments'=>Struct[(str,'href'=>str)]),(str,'review_comment'=>Struct[(str,'href'=>str)]),(str,'commits'=>Struct[(str,'href'=>str)]),(str,'statuses'=>Struct[(str,'href'=>str)])]),(str,'author_association'=>str),(str,'auto_merge'=>null),(str,'active_lock_reason'=>null),(str,'merged'=>bool),(str,'mergeable'=>null),(str,'rebaseable'=>null),(str,'mergeable_state'=>str),(str,'merged_by'=>null),(str,'comments'=>i64),(str,'review_comments'=>i64),(str,'maintainer_can_modify'=>bool),(str,'commits'=>i64),(str,'additions'=>i64),(str,'deletions'=>i64),(str,'changed_files'=>i64)]),(str,'push_id'=>i64),(str,'ref'=>str),(str,'ref_type'=>str),(str,'size'=>i64)]],Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],i64,i64,null,i64,str))
        //input columns:   [type, public, actor, created_at, payload, id, repo, org, year, repo_id, commits, number_of_commits, forkee]

        ASSERT_EQ(input_row_type.parameters().size(), input_columns.size());

        // list types here for convenience
        std::stringstream ss;
        for(unsigned i = 0; i < input_row_type.parameters().size(); ++i) {
            ss<<input_columns[i]<<": "<<input_row_type.parameters()[i].desc()<<std::endl;
        }

        std::cout<<"column info:\n"+ss.str()<<std::endl;

        auto extract_watchers = "def extract_watchers(row):\n"
                                "    # before 2012-08-06 watchers are stargazers, and no stat about watchers is available!\n"
                                "    month = int(row['created_at'].split('-')[1])\n"
                                "    day = int(row['created_at'].split('-')[1])\n"
                                "        \n"
                                "    if 2011 == row['year']:\n"
                                "        return None\n"
                                "    elif row['year'] < 2015:\n"
                                "        if row['year'] == 2012 and month * 100 + day < 806:\n"
                                "            return None\n"
                                "        else:\n"
                                "            return row['repository']['watchers']\n"

                                  "    else:\n"
                                "        row['payload']['forkee']['watchers_count']";

        // compile function
        UDF udf(extract_watchers);


        EXPECT_NE(input_row_type, python::Type::UNKNOWN);
        udf.rewriteDictAccessInAST(input_columns);
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, input_row_type), false, true);
        std::cerr<<udf.getCompileErrorsAsStr()<<std::endl;
        EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(null)");
    }

    TEST_F(GithubQuery, AccessedColumnsRepoIdFunctionTest) {
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

        // parse

        std::string input_row_type_desc = "Row['type'->Option[str],'public'->Option[bool],'actor'->Option[Struct[(str,'avatar_url'=>str),(str,'display_login'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)]],'created_at'->Option[str],'payload'->Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'comment'=>Struct[(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'pull_request'=>Struct[(str,'href'=>str)])]),(str,'author_association'=>str),(str,'body'=>str),(str,'commit_id'=>str),(str,'created_at'=>str),(str,'diff_hunk'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'line'=>i64),(str,'node_id'=>str),(str,'original_commit_id'=>str),(str,'original_line'=>i64),(str,'original_position'=>i64),(str,'original_start_line'=>null),(str,'path'=>str),(str,'performed_via_github_app'=>null),(str,'position'=>i64),(str,'pull_request_review_id'=>i64),(str,'pull_request_url'=>str),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'side'=>str),(str,'start_line'=>null),(str,'start_side'=>null),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'distinct'=>bool),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'description'=>Option[str]),(str,'distinct_size'=>i64),(str,'download'=>Struct[(str,'name'=>str),(str,'created_at'=>str),(str,'size'=>i64),(str,'content_type'=>str),(str,'url'=>str),(str,'download_count'=>i64),(str,'id'=>i64),(str,'description'=>str),(str,'html_url'=>str)]),(str,'forkee'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>str),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'public'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'gist'=>Struct[(str,'comments'=>i64),(str,'created_at'=>str),(str,'description'=>str),(str,'files'=>{}),(str,'git_pull_url'=>str),(str,'git_push_url'=>str),(str,'html_url'=>str),(str,'id'=>str),(str,'public'=>bool),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)])]),(str,'head'=>str),(str,'issue'=>Struct[(str,'active_lock_reason'=>null),(str,'assignee'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'author_association'=>str),(str,'body'=>str),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'created_at'=>str),(str,'events_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels'=>List[Struct[(str,'color'->str),(str,'default'=>bool),(str,'description'=>Option[str]),(str,'id'=>i64),(str,'name'->str),(str,'node_id'=>str),(str,'url'->str)]]),(str,'labels_url'=>str),(str,'locked'=>bool),(str,'milestone'=>Option[Struct[(str,'closed_at'=>null),(str,'closed_issues'->i64),(str,'created_at'->str),(str,'creator'->Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]),(str,'description'->Option[str]),(str,'due_on'->Option[str]),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels_url'=>str),(str,'node_id'=>str),(str,'number'->i64),(str,'open_issues'->i64),(str,'state'->str),(str,'title'->str),(str,'updated_at'=>str),(str,'url'->str)]]),(str,'node_id'=>str),(str,'number'=>i64),(str,'performed_via_github_app'=>Option[Struct[(str,'id'->i64),(str,'slug'->str),(str,'node_id'->str),(str,'owner'->Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]),(str,'name'->str),(str,'description'->str),(str,'external_url'->str),(str,'html_url'->str),(str,'created_at'->str),(str,'updated_at'->str),(str,'permissions'->Struct[(str,'issues'->str),(str,'metadata'->str)]),(str,'events'->[])]]),(str,'pull_request'=>Struct[(str,'diff_url'=>Option[str]),(str,'html_url'=>Option[str]),(str,'patch_url'=>Option[str]),(str,'url'=>str)]),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'repository_url'=>str),(str,'state'=>str),(str,'timeline_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'legacy'=>Struct[(str,'action'=>str),(str,'comment_id'=>i64),(str,'desc'=>str),(str,'head'=>str),(str,'id'=>i64),(str,'issue'=>i64),(str,'issue_id'=>i64),(str,'name'=>str),(str,'number'=>i64),(str,'push_id'=>i64),(str,'ref'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'url'=>str)]),(str,'master_branch'=>str),(str,'member'=>Struct[(str,'gravatar_id'=>str),(str,'avatar_url'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)]),(str,'number'=>i64),(str,'pages'=>List[Struct[(str,'action'->str),(str,'html_url'=>str),(str,'page_name'->str),(str,'sha'->str),(str,'summary'->null),(str,'title'->str)]]),(str,'pull_request'=>Struct[(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'issue'=>Struct[(str,'href'=>str)]),(str,'comments'=>Struct[(str,'href'=>str)]),(str,'review_comments'=>Struct[(str,'href'=>str)]),(str,'review_comment'=>Struct[(str,'href'=>str)]),(str,'commits'=>Struct[(str,'href'=>str)]),(str,'statuses'=>Struct[(str,'href'=>str)])]),(str,'active_lock_reason'=>null),(str,'additions'=>i64),(str,'assignee'=>Option[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'author_association'=>str),(str,'auto_merge'=>null),(str,'base'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Option[Struct[(str,'key'->str),(str,'name'->str),(str,'spdx_id'->str),(str,'url'->str),(str,'node_id'->str)]]),(str,'master_branch'=>Option[str]),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'body'=>Option[str]),(str,'changed_files'=>i64),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'commits'=>i64),(str,'commits_url'=>str),(str,'created_at'=>str),(str,'deletions'=>i64),(str,'diff_url'=>str),(str,'draft'=>bool),(str,'head'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Option[Struct[(str,'key'->str),(str,'name'->str),(str,'spdx_id'->str),(str,'url'->str),(str,'node_id'->str)]]),(str,'master_branch'=>Option[str]),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'labels'=>[]),(str,'locked'=>bool),(str,'maintainer_can_modify'=>bool),(str,'merge_commit_sha'=>Option[str]),(str,'mergeable'=>Option[bool]),(str,'mergeable_state'=>str),(str,'merged'=>bool),(str,'merged_at'=>Option[str]),(str,'merged_by'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'milestone'=>null),(str,'node_id'=>str),(str,'number'=>i64),(str,'patch_url'=>str),(str,'rebaseable'=>null),(str,'requested_reviewers'=>[]),(str,'requested_teams'=>[]),(str,'review_comment_url'=>str),(str,'review_comments'=>i64),(str,'review_comments_url'=>str),(str,'state'=>str),(str,'statuses_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'push_id'=>i64),(str,'pusher_type'=>str),(str,'ref'=>Option[str]),(str,'ref_type'=>str),(str,'size'=>i64)]],'id'->Option[str],'repo'->Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],'org'->Option[Struct[(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'id'->i64),(str,'login'->str),(str,'url'->str)]],'year'->i64]";



    }

    TEST(GithubQueryRequest, DebugSingleRequest) {
        using namespace tuplex;

        // debug single problematic request
        std::string path = "/home/leonhards/projects/tuplex-public/benchmarks/nextconf/hyperspecialization/github/request_1.json";

        path = "/home/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_200.json";
        path = "/home/leonhards/projects/tuplex-public/benchmarks/nextconf/hyperspecialization/flights/request_200.json";

        auto json_message = fileToString(path);
        ASSERT_FALSE(json_message.empty());

        Logger::init();

        python::initInterpreter();
        python::unlockGIL();

        // start worker within same process to easier debug...
        auto app = std::make_unique<WorkerApp>(WorkerSettings());
        app->processJSONMessage(json_message);
        app->shutdown();

        python::lockGIL();
        python::closeInterpreter();
    }

    TEST(GithubQueryRequest, DebugMultiRequests) {
        using namespace tuplex;

        // debug single problematic request
        std::string path = "/home/leonhards/projects/tuplex-public/benchmarks/nextconf/hyperspecialization/github/request_1.json";

        path = "/home/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_200.json";

        auto json_message = fileToString(path);
        ASSERT_FALSE(json_message.empty());

        Logger::init();

        python::initInterpreter();
        python::unlockGIL();

        // start worker within same process to easier debug...
        auto app = std::make_unique<WorkerApp>(WorkerSettings());
        for(unsigned i = 0; i < 410; ++i) {
            path = "/home/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_" + std::to_string(i) + ".json";
            auto json_message = fileToString(path);

            std::cout<<">>> current RSS "<<sizeToMemString(getCurrentRSS())<<" peak RSS "<<sizeToMemString(getPeakRSS())<<" before message "<<(i)<<std::endl;
            app->processJSONMessage(json_message);
            std::cout<<">>> current RSS "<<sizeToMemString(getCurrentRSS())<<" peak RSS "<<sizeToMemString(getPeakRSS())<<" after message "<<(i)<<std::endl;
        }


        app->shutdown();

        python::lockGIL();
        python::closeInterpreter();
    }

}


