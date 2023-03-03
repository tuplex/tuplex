//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include <gtest/gtest.h>
#include <unistd.h>
#include <Utils.h>
#include <Timer.h>
#include <physical/codegen/StageBuilder.h>
#include <logical/FileInputOperator.h>
#include <logical/FileOutputOperator.h>
#include <logical/MapOperator.h>
#include <google/protobuf/util/json_util.h>
#include <ee/worker/WorkerApp.h>
#include <ee/aws/LambdaWorkerApp.h>

#include <boost/filesystem.hpp>

#ifdef BUILD_WITH_AWS
#include <ee/aws/AWSLambdaBackend.h>
#endif

#ifndef S3_TEST_BUCKET
// define dummy to compile
#ifdef SKIP_AWS_TESTS
#define S3_TEST_BUCKET "tuplex-test"
#endif

#warning "need S3 Test bucket to run these tests"
#endif


//   constexpr double nan = std::numeric_limits<double>::quiet_NaN();

#include <CSVUtils.h>
#include <procinfo.h>
#include <FileHelperUtils.h>
#include <physical/codegen/StagePlanner.h>
#include <logical/LogicalOptimizer.h>

#include "PerfEvent.hpp"
#include "logical/LogicalPlan.h"
#include "physical/execution/csvmonkey.h"

// dummy so linking works
namespace tuplex {
    ContainerInfo getThisContainerInfo() {
        ContainerInfo info;
        return info;
    }
}

tuplex::TransformStage* create_github_stage(const std::string& test_path, const std::string& test_output_path,
                                            bool enable_nvo, const tuplex::ContextOptions& co,
                                            bool hyper_mode,
                                            const tuplex::SamplingMode& sm = tuplex::DEFAULT_SAMPLING_MODE) {
    using namespace tuplex;
    using namespace std;

    bool enable_cf = false;
    codegen::StageBuilderConfiguration conf;
    conf.constantFoldingOptimization = enable_cf;
    codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, enable_cf, false);

    auto jsonop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromJSON(test_path, true, true, co, sm));

    auto repo_id_code = "def extract_repo_id(row):\n"
                        "\tif 2012 <= row['year'] <= 2014:\n"
                        "\t\treturn row['repository']['id']\n"
                        "\telse:\n"
                        "\t\treturn row['repo']['id']\n";

    auto wop1 = std::shared_ptr<LogicalOperator>(new WithColumnOperator(jsonop, jsonop->columns(), "year", UDF("lambda x: int(x['created_at'].split('-')[0])")));
    auto wop2 = std::shared_ptr<LogicalOperator>(new WithColumnOperator(wop1, wop1->columns(), "repo_id", UDF(repo_id_code)));
    auto filter_op = std::shared_ptr<LogicalOperator>(new FilterOperator(wop2, UDF("lambda x: x['type'] == 'ForkEvent'"), wop2->columns()));
    auto mop = new MapOperator(filter_op, UDF("lambda t: (t['type'],t['repo_id'],t['year'])"), filter_op->columns());
    mop->setOutputColumns(std::vector<std::string>{"type", "repo_id", "year"});
    auto sop = std::shared_ptr<LogicalOperator>(mop);
    auto fop = std::make_shared<FileOutputOperator>(sop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());

    // optimize operator chain
    auto options = ContextOptions::defaults();
    auto opt = std::make_unique<LogicalOptimizer>(options);
    auto opt_action = opt->optimize(fop, true);

    // create operator chain
    std::vector<std::shared_ptr<LogicalOperator>> ops;
    auto node = opt_action;
    while(node) {
        ops.push_back(node);
        node = node->parent();
    }
    std::reverse(ops.begin(), ops.end());

    // now add to builder
    for(auto op : ops) {
        if(op->type() == LogicalOperatorType::FILEINPUT)
            builder.addFileInput(std::dynamic_pointer_cast<FileInputOperator>(op));
        else if(op->type() == LogicalOperatorType::FILEOUTPUT)
            builder.addFileOutput(std::dynamic_pointer_cast<FileOutputOperator>(op));
        else
            builder.addOperator(op);
    }

    // // can add also manually ops from before (no logical opt performed!)
    // builder.addFileInput(jsonop);
    // builder.addOperator(wop1);
    // builder.addOperator(wop2);
    // builder.addOperator(filter_op);
    // builder.addOperator(sop);
    // builder.addFileOutput(fop);

    if(hyper_mode)
        // hyper mode!
        return builder.encodeForSpecialization(nullptr, nullptr, true, false, true);
    else
        return builder.build();
}

std::string find_worker() {
    using namespace tuplex;

    // find worker executable (tuplex-worker)
    static const std::string exec_name = "tuplex-worker";

    // check current working dir
    auto exec_dir = dir_from_pid(pid_from_self());

    auto path = exec_dir + "/" + exec_name;

    if(!fileExists(path))
        throw std::runtime_error("Could not find worker under " + path);

    return eliminateSeparatorRuns(path);
}


/*!
 * runs command and returns stdout?
 * @param cmd
 * @return stdout
 */
std::string runCommand(const std::string& cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 128, pipe.get()) != nullptr)
            result += buffer.data();
    }
    return result;
}
namespace tuplex {
    std::string transformStageToReqMessage(const TransformStage* tstage, const std::string& inputURI, const size_t& inputSize, const std::string& output_uri, bool interpreterOnly,
                                           size_t numThreads = 1, const std::string& spillURI="spill_folder", bool verboseLogging=false) {
        messages::InvocationRequest req;

        double nc_threshold = 0.9;

        size_t buf_spill_size = 512 * 1024; // for testing, set spill size to 512KB!

        // use higher buf, to avoid spilling
        buf_spill_size = 128 * 1024 * 1024; // 128MB

        req.set_type(messages::MessageType::MT_TRANSFORM);
        assert(tstage);
        auto pb_stage = tstage->to_protobuf();

        pb_stage->set_bitcode(tstage->fastPathBitCode());
        req.set_allocated_stage(pb_stage.release());

        // add input file to request
        req.add_inputuris(inputURI);
        req.add_inputsizes(inputSize);

        // output uri of job? => final one? parts?
        req.set_baseoutputuri(output_uri);

        req.set_verboselogging(verboseLogging);

        auto ws = std::make_unique<messages::WorkerSettings>();
        ws->set_numthreads(numThreads);
        ws->set_normalbuffersize(buf_spill_size);
        ws->set_exceptionbuffersize(buf_spill_size);
        ws->set_spillrooturi(spillURI);
        ws->set_useinterpreteronly(interpreterOnly);
        ws->set_normalcasethreshold(nc_threshold);

        // do not use optimizer?
        auto& m = *ws->mutable_other();
        m["tuplex.useLLVMOptimizer"] = "true";
        m["tuplex.optimizer.constantFoldingOptimization"] = "true";

        req.set_allocated_settings(ws.release());

        // transfrom to json
        std::string json_buf;
        google::protobuf::util::MessageToJsonString(req, &json_buf);
        return json_buf;
    }
}

std::string findAndReplaceAll(const std::string& str, const std::string& toSearch, const std::string& replaceStr){
    std::string data = str;
    // Get the first occurrence
    size_t pos = data.find(toSearch);

    // Repeat till end is reached
    while( pos != std::string::npos) {
        // Replace this occurrence of Sub String
        data.replace(pos, toSearch.size(), replaceStr);
        // Get the next occurrence from the current position
        pos =data.find(toSearch, pos + replaceStr.size());
    }
    return data;
}

std::string shellEscape(const std::string& str) {
    if(str.empty())
        return "\"\"";

    // replace all '
    // return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
    return "'" + findAndReplaceAll(str, "'", "'\"'\"'") + "'";
}

void createRefPipeline(const std::string& test_input_path,
                       const std::string& test_output_path,
                       const std::string& scratch_dir) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    try {
        auto co = ContextOptions::defaults();
        co.set("tuplex.executorCount", "4");
        co.set("tuplex.partitionSize", "512KB");
        co.set("tuplex.executorMemory", "32MB");
        co.set("tuplex.useLLVMOptimizer", "true");
        co.set("tuplex.allowUndefinedBehavior", "false");
        co.set("tuplex.webui.enable", "false");
        co.set("tuplex.scratchDir", scratch_dir);
        co.set("tuplex.optimizer.selectionPushdown", "false"); // same setting as below??
        Context c(co);

        // from:
        //  auto csvop = FileInputOperator::fromCsv(test_path.toString(), co,
        //                                       option<bool>(true),
        //                                               option<char>(','), option<char>('"'),
        //            {}, {}, {}, {});
        //    auto mapop = new MapOperator(csvop, UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}"), csvop->columns());
        //    auto fop = new FileOutputOperator(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, {});
        c.csv(test_input_path).map(UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}", "")).tocsv(test_output_path);
    } catch (...) {
        FAIL();
    }
    python::lockGIL();
    python::closeInterpreter();
}

// @TODO: should either have constructable outputURI or baseURI + partNo!

TEST(BasicInvocation, PurePythonMode) {
    using namespace std;
    using namespace tuplex;

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // test config here:
    // local csv test file!
    auto test_path = URI("file://../resources/flights_on_time_performance_2019_01.sample.csv");
    // local for quicker dev
    test_path = URI("../../../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv");
    auto test_output_path = URI("file://output.txt");
    auto spillURI = std::string("spill_folder");

    size_t num_threads = 4;

    // invoke helper with --help
    std::string work_dir = cur_dir;

    // Step 1: create ref pipeline using direct Tuplex invocation
    auto test_ref_path = testName + "_ref.csv";
    createRefPipeline(test_path.toString(), test_ref_path, scratchDir);

    // load ref file from parts!
    auto ref_files = glob(current_working_directory() + "/" + testName + "_ref*part*csv");
    std::sort(ref_files.begin(), ref_files.end());

    // merge into single large string
    std::string ref_content = "";
    for(unsigned i = 0; i < ref_files.size(); ++i) {
        if(i > 0) {
            auto file_content = fileToString(ref_files[i]);
            // skip header for these parts
            auto idx = file_content.find("\n");
            ref_content += file_content.substr(idx + 1);
        } else {
            ref_content = fileToString(ref_files[0]);
        }
    }
    stringToFile("ref_content.txt", ref_content);
    ref_content = fileToString("ref_content.txt");

    // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
    ASSERT_TRUE(test_path.exists());
    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();
    auto enable_nvo = false; // test later with true! --> important for everything to work properly together!
    co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
    co.set("tuplex.useInterpreterOnly", "true");

    // // disable llvm optimizers?
     co.set("tuplex.useLLVMOptimizer", "false");

     codegen::StageBuilderConfiguration conf(co);
    codegen::StageBuilder builder(0, true, conf); //;(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                            option<bool>(true),
                                            option<char>(','), option<char>('"'),
                                            {""}, {}, {}, {}, DEFAULT_SAMPLING_MODE));
    auto mapop = std::make_shared<MapOperator>(csvop, UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}"), csvop->columns());
    auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());
    builder.addFileInput(csvop);
    builder.addOperator(mapop);
    builder.addFileOutput(fop);

    auto tstage = builder.build();

    // transform to message
    vfs = VirtualFileSystem::fromURI(test_path);
    uint64_t input_file_size = 0;
    vfs.file_size(test_path, input_file_size);
    auto json_message = transformStageToReqMessage(tstage, URI(test_path).toPath(),
                                                   input_file_size, test_output_path.toString(),
                                                   true,
                                                   num_threads,
                                                   spillURI);

    // save to file
    auto msg_file = URI("test_message.json");
    stringToFile(msg_file, json_message);

    python::lockGIL();
    python::closeInterpreter();

    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(WorkerSettings());
    app->processJSONMessage(json_message);
    app->shutdown();

    // fetch output file and check contents...
    auto file_content = fileToString(test_output_path.toString() + ".csv");

    // check files are 1:1 the same!
    EXPECT_EQ(file_content.size(), ref_content.size());

    // because order may be different (unless specified), split into lines & sort and compare
    auto res_lines = splitToLines(file_content);
    auto ref_lines = splitToLines(ref_content);
    std::sort(res_lines.begin(), res_lines.end());
    std::sort(ref_lines.begin(), ref_lines.end());
    ASSERT_EQ(res_lines.size(), ref_lines.size());
    for(unsigned i = 0; i < std::min(res_lines.size(), ref_lines.size()); ++i) {
        EXPECT_EQ(res_lines[i], ref_lines[i]);
    }

    // test again, but this time invoking the worker with a message as separate process.

    // invoke worker with that message
    Timer timer;
    auto cmd = worker_path + " -m " + msg_file.toPath();
    auto res_stdout = runCommand(cmd);
    auto worker_invocation_duration = timer.time();
    cout<<res_stdout<<endl;
    cout<<"Invoking worker took: "<<worker_invocation_duration<<"s"<<endl;

    // check result contents
    cout<<"Checking worker results..."<<endl;
    file_content = fileToString(test_output_path.toString() + ".csv");

    // check files are 1:1 the same!
    ASSERT_EQ(file_content.size(), ref_content.size());
    res_lines = splitToLines(file_content);
    std::sort(res_lines.begin(), res_lines.end());
    EXPECT_EQ(res_lines.size(), ref_lines.size());
    for(unsigned i = 0; i < std::min(res_lines.size(), ref_lines.size()); ++i) {
        EXPECT_EQ(res_lines[i], ref_lines[i]);
    }
    cout<<"Invoked worker check done."<<endl;
    cout<<"Test done."<<endl;
}

TEST(BasicInvocation, HashOutput) {
    using namespace std;
    using namespace tuplex;

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // test config here:
    // local csv test file!
    auto test_path = URI("file://../resources/flights_on_time_performance_2019_01.sample.csv");
    auto test_output_path = URI("file://output.ht");
    auto spillURI = std::string("spill_folder");

    // local for quicker dev
    test_path = URI("file:///Users/leonhards/data/flights/flights_on_time_performance_2009_01.csv");

    size_t num_threads = 1;

    // invoke helper with --help
    std::string work_dir = cur_dir;

    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();
    auto enable_nvo = false; // test later with true! --> important for everything to work properly together!
    co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
    co.set("tuplex.useInterpreterOnly", "false");
    codegen::StageBuilderConfiguration conf(co);
    codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                            option<bool>(true),
                                            option<char>(','), option<char>('"'),
                                            {""}, {}, {}, {}, DEFAULT_SAMPLING_MODE));
    auto mapop = std::make_shared<MapOperator>(csvop, UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}"), csvop->columns());
    auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());
    builder.addFileInput(csvop);
    builder.addOperator(mapop);

    auto key_type = mapop->getOutputSchema().getRowType().parameters()[0];
    auto bucket_type = mapop->getOutputSchema().getRowType().parameters()[1];
    builder.addHashTableOutput(nullptr, mapop->getOutputSchema(), true, false, {0}, key_type, bucket_type);


#warning "given pipeline is with i64 keys. -> issue. want to specialize stuff properly!"

    auto tstage = builder.build();

    // transform to message
    vfs = VirtualFileSystem::fromURI(test_path);
    uint64_t input_file_size = 0;
    vfs.file_size(test_path, input_file_size);
    auto json_message = transformStageToReqMessage(tstage, URI(test_path).toPath(),
                                                   input_file_size, test_output_path.toString(),
                                                   co.PURE_PYTHON_MODE(),
                                                   num_threads,
                                                   spillURI);

    // save to file
    auto msg_file = URI("test_message.json");
    stringToFile(msg_file, json_message);

    python::lockGIL();
    python::closeInterpreter();

    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(WorkerSettings());
    app->processJSONMessage(json_message);
    app->shutdown();
}

int64_t csv_dummy_row_functor(void* userData, int64_t num_cells, char **cells, int64_t* cell_sizes) {
    auto num = reinterpret_cast<size_t*>(userData);
    if(num) {
        *num = *num + 1;
    }
    return 0;
}

inline std::string flights_code() {
    auto udf_code = "def fill_in_delays(row):\n"
                    "    # want to fill in data for missing carrier_delay, weather delay etc.\n"
                    "    # only need to do that prior to 2003/06\n"
                    "    \n"
                    "    year = row['YEAR']\n"
                    "    month = row['MONTH']\n"
                    "    arr_delay = row['ARR_DELAY']\n"
                    "    \n"
                    "    if year == 2003 and month < 6 or year < 2003:\n"
                    "        # fill in delay breakdown using model and complex logic\n"
                    "        if arr_delay is None:\n"
                    "            # stays None, because flight arrived early\n"
                    "            # if diverted though, need to add everything to div_arr_delay\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': None,\n"
                    "                    'carrier_delay' : None,\n"
                    "                    'weather_delay': None,\n"
                    "                    'nas_delay' : None,\n"
                    "                    'security_delay': None,\n"
                    "                    'late_aircraft_delay' : None}\n"
                    "        elif arr_delay < 0.:\n"
                    "            # stays None, because flight arrived early\n"
                    "            # if diverted though, need to add everything to div_arr_delay\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : None,\n"
                    "                    'weather_delay': None,\n"
                    "                    'nas_delay' : None,\n"
                    "                    'security_delay': None,\n"
                    "                    'late_aircraft_delay' : None}\n"
                    "        elif arr_delay < 5.:\n"
                    "            # it's an ontime flight, just attribute any delay to the carrier\n"
                    "            carrier_delay = arr_delay\n"
                    "            # set the rest to 0\n"
                    "            # ....\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : carrier_delay,\n"
                    "                    'weather_delay': None,\n"
                    "                    'nas_delay' : None,\n"
                    "                    'security_delay': None,\n"
                    "                    'late_aircraft_delay' : None}\n"
                    "        else:\n"
                    "            # use model to determine everything and set into (join with weather data?)\n"
                    "            # i.e., extract here a couple additional columns & use them for features etc.!\n"
                    "            crs_dep_time = float(row['CRS_DEP_TIME'])\n"
                    "            crs_arr_time = float(row['CRS_ARR_TIME'])\n"
                    "            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])\n"
                    "            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time\n"
                    "            weather_delay = 2000 + 0.09 * carrier_delay * (carrier_delay - 10.0)\n"
                    "            nas_delay = 3600 * crs_dep_time / 10.0\n"
                    "            security_delay = 7200 / crs_dep_time\n"
                    "            late_aircraft_delay = (20 + crs_arr_time) / (1.0 + crs_dep_time)\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : carrier_delay,\n"
                    "                    'weather_delay': weather_delay,\n"
                    "                    'nas_delay' : nas_delay,\n"
                    "                    'security_delay': security_delay,\n"
                    "                    'late_aircraft_delay' : late_aircraft_delay}\n"
                    "    else:\n"
                    "        # just return it as is\n"
                    "        return {'year' : year, 'month' : month,\n"
                    "                'day' : row['DAY_OF_MONTH'],\n"
                    "                'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                'dest': row['DEST_AIRPORT_ID'],\n"
                    "                'distance' : row['DISTANCE'],\n"
                    "                'dep_delay' : row['DEP_DELAY'],\n"
                    "                'arr_delay': row['ARR_DELAY'],\n"
                    "                'carrier_delay' : row['CARRIER_DELAY'],\n"
                    "                'weather_delay':row['WEATHER_DELAY'],\n"
                    "                'nas_delay' : row['NAS_DELAY'],\n"
                    "                'security_delay': row['SECURITY_DELAY'],\n"
                    "                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}";
    return udf_code;
}

TEST(BasicInvocation, MultiAWSInit) {
    using namespace tuplex;

    Timer timer;
    initAWS();
    shutdownAWS();
    std::cout<<"aws test 1: "<<timer.time()<<std::endl;
    initAWS();
    shutdownAWS();
    std::cout<<"aws test 2: "<<timer.time()<<std::endl;
}

class BasicInvocationF : public ::testing::Test {
protected:
    PyThreadState *saveState;
    std::stringstream logStream;

    void SetUp() override {
        // reset global static variables, i.e. whether to use UDF compilation or not!
        tuplex::UDF::enableCompilation();

        // init logger to write to both stream as well as stdout
        // ==> searching the stream can be used to validate err Messages
        Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>()});

        python::initInterpreter();
        // release GIL
        python::unlockGIL();
    }

    void TearDown() override {
        python::lockGIL();
        // important to get GIL for this
        python::closeInterpreter();

        // release runtime memory
        tuplex::runtime::releaseRunTimeMemory();

        // remove all loggers ==> note: this crashed because of multiple threads not being done yet...
        // call only AFTER all threads/threadpool is terminated from Context/LocalBackend/LocalEngine...
        Logger::instance().reset();

        tuplex::UDF::enableCompilation(); // reset

        // check whether exceptions work, LLVM9 has a bug which screws up C++ exception handling in ORCv2 APIs
        try {
            throw std::exception();
        } catch(...) {
            std::cout << "test done." << std::endl;
        }
    }
};


TEST(BasicInvocation, StrataCSVSampling) {
    using namespace tuplex;
    using namespace std;

    string input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_10.csv";

    auto data = fileToString(input_pattern);

    Timer timer;
    auto rows = csv_parseRowsStratified(data.c_str(), data.size(), 110, 0, ',', '"', {""}, 10, 128, 1, 0, {0});
    cout<<"stratified sampling of "<<rows.size()<<" rows took "<<timer.time()<<" s"<<endl;
}

TEST(BasicInvocation, FlightsConstantSamplingTest) {
    using namespace tuplex;
    using namespace std;

    string input_pattern = "/hot/data/flights_all/*.csv";

    // change here for single file
    // input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_10.csv";

    auto vfs = VirtualFileSystem::fromURI(input_pattern);
    auto paths = vfs.globAll(input_pattern);

    std::cout<<"Found "<<pluralize(paths.size(), "file")<<" for pattern "<<input_pattern<<std::endl;
    ASSERT_FALSE(paths.empty());

    auto sm = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;
    size_t sample_limit = 10000;

    auto co = ContextOptions::defaults();
    co.set("tuplex.sample.maxDetectionMemory", "256KB"); // result 1 setting
    co.set("tuplex.sample.maxDetectionMemory", "1MB"); // result 2 setting
    co.set("tuplex.sample.maxDetectionMemory", "16MB"); // result 3 setting + 30k rows
    co.set("tuplex.sample.maxDetectionMemory", "32MB"); // result 4 setting + 30k rows --> maybe use 1024/1 for sampling? --> this works. use log(...) rule for sample size.

    // set strata
    co.set("tuplex.sample.strataSize", "1024");
    co.set("tuplex.sample.samplesPerStrata", "1");
    sample_limit = 30000;

    std::set<string> required_cols({"YEAR", "MONTH", "DAY_OF_MONTH", "OP_UNIQUE_CARRIER",
                                    "OP_CARRIER_FL_NUM", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
                                    "CRS_DEP_TIME", "DEP_DELAY", "CRS_ARR_TIME", "ARR_DELAY",
                                    "CRS_ELAPSED_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY",
                                    "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"});

    // there are only a couple columns to be ok with being marked as constant. Change sampling etc. else!
    std::set<string> ok_cols({"YEAR", "MONTH", "CARRIER_DELAY", "WEATHER_DELAY",
                                    "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"});

    std::vector<tuple<string,string>> const_results;
    std::set<string> bad_paths;

    // now go through each file & sample & see what checks are generated
    std::shared_ptr<FileInputOperator> fop(FileInputOperator::fromCsv(input_pattern, co, option<bool>::none, ',', '"', {""}, {}, {}, {}, sm));
    for(const auto& path : paths) {

        cout<<"-- resampling file "<<path.toPath()<<endl;

        // perform now only sampling & ConstantDetection
        uint64_t path_size = 0;
        vfs.file_size(path, path_size);
        fop->setInputFiles({path}, {path_size}, true, sample_limit, true, 128, 1, -1);
        auto sample = fop->getSample(sample_limit);

        codegen::DetectionStats ds;
        ds.detect(sample);

        std::stringstream ss;
        ss<<"Identified "<<pluralize(ds.constant_column_indices().size(), "column")<<" to be constant: "<<ds.constant_column_indices()<<endl;

        // print out which rows are considered constant (and with which values!)
        // --> note that these checks are done POST initial pushdown.
        // i.e. use output columns. (previously: input columns)
        std::string const_str;
        for(auto idx : ds.constant_column_indices()) {
            string column_name;
            if(!fop->columns().empty())
                column_name = fop->columns()[idx];
            if(required_cols.find(column_name) != required_cols.end()) {
                ss<<" - "<<column_name<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
                const_str += column_name + "=" + ds.constant_row.get(idx).desc() + " ";

                // not ok?
                if(ok_cols.find(column_name) == ok_cols.end())
                    bad_paths.insert(path.basename());
            }
        }

        const_results.push_back(std::make_tuple(path.basename(), const_str));

        cout<<ss.str()<<endl;
    }

    // print out summary
    for(auto res : const_results) {
        cout<<"path: "<<std::get<0>(res)<<"   "<<std::get<1>(res)<<endl;
    }
    cout<<endl;
    std::vector<string> sorted_bad_paths(bad_paths.begin(), bad_paths.end());
    sort(sorted_bad_paths.begin(), sorted_bad_paths.end());
    for(auto path : sorted_bad_paths) {
        cout<<"bad path: "<<path<<endl;
    }

    if(!sorted_bad_paths.empty())
        cout<<endl<<sorted_bad_paths.size()<<" paths have undesired behavior"<<endl;


    // result 1: with 256KB memory, 10k rows max
    // bad path: flights_on_time_performance_1988_12.csv
    //bad path: flights_on_time_performance_1989_04.csv
    //bad path: flights_on_time_performance_1991_05.csv
    //bad path: flights_on_time_performance_1991_06.csv
    //bad path: flights_on_time_performance_1991_09.csv
    //bad path: flights_on_time_performance_1992_08.csv
    //bad path: flights_on_time_performance_1993_09.csv
    //bad path: flights_on_time_performance_1994_02.csv
    //bad path: flights_on_time_performance_1994_05.csv
    //bad path: flights_on_time_performance_1994_08.csv
    //bad path: flights_on_time_performance_2003_10.csv
    //bad path: flights_on_time_performance_2007_08.csv
    //bad path: flights_on_time_performance_2010_02.csv
    //bad path: flights_on_time_performance_2012_05.csv
    //bad path: flights_on_time_performance_2017_04.csv
    //bad path: flights_on_time_performance_2017_05.csv
    //bad path: flights_on_time_performance_2017_08.csv
    //bad path: flights_on_time_performance_2018_04.csv
    //bad path: flights_on_time_performance_2018_09.csv
    //bad path: flights_on_time_performance_2020_11.csv
    //bad path: flights_on_time_performance_2021_03.csv
    //bad path: flights_on_time_performance_2021_07.csv
    //bad path: flights_on_time_performance_2021_10.csv
    //
    //23 paths have undesired behavior

    // result 2: with 1MB memory, 10k rows max
    // bad path: flights_on_time_performance_1988_12.csv
    //bad path: flights_on_time_performance_1991_09.csv
    //bad path: flights_on_time_performance_1993_09.csv
    //bad path: flights_on_time_performance_1994_05.csv
    //bad path: flights_on_time_performance_1994_08.csv
    //bad path: flights_on_time_performance_2003_10.csv
    //bad path: flights_on_time_performance_2007_08.csv
    //bad path: flights_on_time_performance_2012_05.csv
    //bad path: flights_on_time_performance_2017_04.csv
    //bad path: flights_on_time_performance_2017_05.csv
    //bad path: flights_on_time_performance_2018_09.csv
    //bad path: flights_on_time_performance_2021_03.csv
    //bad path: flights_on_time_performance_2021_07.csv
    //bad path: flights_on_time_performance_2021_10.csv
    //
    //14 paths have undesired behavior

    // result 3: with 4MB memory, 20k rows max
}

TEST(BasicInvocation, FlightsConstantFilterFold) {
    using namespace tuplex;
    using namespace std;

    cout<<">> starting flights (hyper) test"<<endl;

    string input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";

    input_pattern = "../resources/hyperspecialization/flights_all/*.csv.sample";

    // test pattern
    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1999_05.csv";

    // can use local files to test as well!
    input_pattern = "/hot/data/flights_all/flights_on_time_performance_1999_05.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_07.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2000_03.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2000_07.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2021_03.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2001_08.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2000_02.csv";
//
//    // wrong column detected to be constant
//    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2018_04.csv";

    std::cout<<"Testing over pattern: "<<input_pattern<<std::endl;

    bool resolve_with_interpreter_only = false;

    // local worker mode for easier debugging
    ContextOptions co = ContextOptions::defaults();


    // AWS Lambda settings
     co.set("tuplex.backend", "lambda");
     co.set("tuplex.aws.scratchDir", "s3://tuplex-leonhard/scratch/flights-exp");
    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_02.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";


    // alone, this here works.
    // use worker to detect issue
    co.set("tuplex.backend", "worker");
    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_02.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";

    input_pattern = "/hot/data/flights_all/flights_on_time_performance_1987_10.csv,/hot/data/flights_all/flights_on_time_performance_2000_02.csv,/hot/data/flights_all/flights_on_time_performance_2021_11.csv";
    // --> 2000 file produces exceptions - why??? -> only in conjunction with the other files... - bad general case path?

    // bad sampling:




    // activate optimizations
    co.set("tuplex.optimizer.selectionPushdown", "true");
    co.set("tuplex.optimizer.filterPushdown", "true");
    co.set("tuplex.optimizer.constantFoldingOptimization", "true"); // run with constant folding on/off
    co.set("tuplex.filterPromotion", "true");
    co.set("tuplex.optimizer.nullValueOptimization", "true");
    co.set("tuplex.experimental.hyperspecialization", "false"); // first check that THIS is correct.
    co.set("tuplex.experimental.s3PreCacheSize", "1G");
    co.set("tuplex.inputSplitSize", "2GB");
    co.set("tuplex.resolveWithInterpreterOnly", boolToString(resolve_with_interpreter_only));

    // tweak sample size here to fix everything
    co.set("tuplex.sample.maxDetectionRows", "10000");
    co.set("tuplex.sample.maxDetectionMemory", "1MB");

    co.set("tuplex.sample.maxDetectionMemory", "256KB");

    auto co_hyper = co;
    co_hyper.set("tuplex.experimental.hyperspecialization", "true");

    // force bad parse format?
    co_hyper.set("tuplex.experimental.forceBadParseExceptFormat", "true"); // <-- this could explain the exceptions?
    co_hyper.set("tuplex.useLLVMOptimizer", "true");
    co_hyper.set("tuplex.autoUpcast", "true");

    // use sampling mode first/last file, first/last rows
    SamplingMode sm = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;

    // init runtime
    auto rc_runtime = runtime::init(co.RUNTIME_LIBRARY().toPath());
    ASSERT_TRUE(rc_runtime);


    double hyper_time = 0;
    double nohyper_time = 0;
//    // non-hyper vs. hyper (there needs to be a runtime difference.
//    {
//        python::initInterpreter();
//        python::unlockGIL();
//        Context ctx(co);
//        auto udf_code = flights_code();
//
//        Timer timer;
//        ctx.csv(input_pattern, {}, option<bool>::none,
//                option<char>::none, '"', {""}, {}, {}, sm)
//                .map(UDF(udf_code))
//                .filter(UDF("lambda x: 2001 <= x['YEAR'] <= 2004"))
//                .tocsv("nohyper_local_worker_output.csv");
//        nohyper_time = timer.time();
//        std::cout<<"no-hyper mode: "<<nohyper_time<<std::endl;
//    }

    {
        // bug: workerapp shutsdown interpreter -> needs to be fixed!
        python::initInterpreter();
        python::unlockGIL();
        Context ctx(co_hyper);
        auto udf_code = flights_code();

        // ctx.csv(input_pattern, sampling_mode=sm).map(fill_in_delays).filter(lambda x: 2000 <= x['year'] <= 2005).tocsv(s3_output_path)

        Timer timer;
        ctx.csv(input_pattern, {}, option<bool>::none,
                option<char>::none, '"', {""}, {}, {}, sm)
                .map(UDF(udf_code))
                .filter(UDF("lambda x: 2000 <= x['year'] <= 2005"))
                .tocsv("hyper_local_worker_output.csv");
        hyper_time = timer.time();
        std::cout<<"hyper mode: "<<hyper_time<<std::endl;
        std::cout<<"context options:\n";//<<ctx.getOptions().toString()<<std::endl;
        auto options = ctx.getOptions();
        nlohmann::json j;
        for(auto kv : options.store()) {
            j[kv.first] = kv.second;
        }
        std::cout<<j.dump(2)<<std::endl;

        std::cout<<"UDF::\n\n"<<udf_code<<std::endl;
    }

    // final:
    std::cout<<"result:::"<<std::endl;
    std::cout<<"   hyper: "<<hyper_time<<std::endl;
    std::cout<<"no hyper: "<<nohyper_time<<std::endl;


//    ctx.csv(input_pattern, {}, option<bool>::none,
//            option<char>::none, '"', {""}, {}, {}, sm).map(UDF(udf_code)).tocsv("local_worker_output.csv");

    // // let's use a simple pipeline to make sure everything works
    // ctx.csv(input_pattern).selectColumns(std::vector<std::string>{"YEAR"}).tocsv("year_extract.csv");

    //python::lockGIL();
    //python::closeInterpreter();
}

TEST(BasicInvocation, ConstantFilterFold) {
    using namespace tuplex;
    using namespace std;

    // local worker mode for easier debugging
    ContextOptions co = ContextOptions::defaults();
    co.set("tuplex.backend", "worker");

    // activate optimizations
    co.set("tuplex.optimizer.selectionPushdown", "true");
    co.set("tuplex.optimizer.filterPushdown", "true");
    co.set("tuplex.optimizer.constantFoldingOptimization", "true"); // run with constant folding on/off
    co.set("tuplex.filterPromotion", "true");
    co.set("tuplex.optimizer.nullValueOptimization", "true");
    co.set("tuplex.experimental.hyperspecialization", "false"); // first check that THIS is correct.
    co.set("tuplex.experimental.s3PreCacheSize", "1G");
    co.set("tuplex.inputSplitSize", "2GB");

    auto co_hyper = co;
    co_hyper.set("tuplex.experimental.hyperspecialization", "true");


    // use sampling mode first/last file, first/last rows
    SamplingMode sm = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;

    // init runtime
    auto rc_runtime = runtime::init(co.RUNTIME_LIBRARY().toPath());
    ASSERT_TRUE(rc_runtime);

    string input_pattern = "test.csv";
    // create test file using random data
    size_t N = 1000000;
    stringstream ss;
    ss<<"A,B\n";
    for(unsigned i = 0; i < N; ++i) {
        ss<<"42,"<<rand()% 100<<"\n";
    }
    stringToFile(input_pattern, ss.str());

    double hyper_time = 0;
    {
        python::initInterpreter();
        python::unlockGIL();
        Context ctx(co_hyper);
        auto udf_code = flights_code();

        // i.e., idea is: does filter always evaluate to true? or does it always evaluate to false?
        // -> if false based on check -> can remove in normal-path all subsequent operators.
        // -> if true based on check -> can remove filter, and use all subsequent operators.

        Timer timer;
        ctx.csv(input_pattern, {}, option<bool>::none,
                option<char>::none, '"', {""}, {}, {}, sm)
                .filter(UDF("lambda x: x['A'] == 0")) // <-- this is an always false condition based on constant sample
                .tocsv("output.csv");
        hyper_time = timer.time();
        std::cout<<"hyper mode: "<<hyper_time<<std::endl;
    }
}

TEST(BasicInvocation, FlightHyperSingleFileWithFilter) {
    // ctx.csv(input_pattern, sampling_mode=sm).map(fill_in_delays).filter(lambda x: 2000 <= x['year'] <= 2005).tocsv(s3_output_path)

    using namespace tuplex;
    using namespace std;

    cout<<">> starting flights (hyper) test"<<endl;

    string input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";
    bool resolve_with_interpreter_only = false;

    // local worker mode for easier debugging
    ContextOptions co = ContextOptions::defaults();
    co.set("tuplex.backend", "worker");
    co.set("tuplex.backend", "lambda");

    // activate optimizations
    co.set("tuplex.optimizer.selectionPushdown", "true");
    co.set("tuplex.optimizer.filterPushdown", "true");
    co.set("tuplex.optimizer.constantFoldingOptimization", "true"); // run with constant folding on/off
    co.set("tuplex.filterPromotion", "true");
    co.set("tuplex.optimizer.nullValueOptimization", "true");
    co.set("tuplex.experimental.hyperspecialization", "false"); // first check that THIS is correct.
    co.set("tuplex.experimental.s3PreCacheSize", "1G");
    co.set("tuplex.inputSplitSize", "2GB");
    co.set("tuplex.resolveWithInterpreterOnly", boolToString(resolve_with_interpreter_only));

    auto co_hyper = co;
    co_hyper.set("tuplex.experimental.hyperspecialization", "true");

    // use sampling mode first/last file, first/last rows
    SamplingMode sm = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;

    // init runtime
    auto rc_runtime = runtime::init(co.RUNTIME_LIBRARY().toPath());
    ASSERT_TRUE(rc_runtime);

    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1999_05.csv";

    {
        // bug: workerapp shutsdown interpreter -> needs to be fixed!
        python::initInterpreter();
        python::unlockGIL();
        Context ctx(co_hyper);
        auto udf_code = flights_code();

        Timer timer;
        ctx.csv(input_pattern, {}, option<bool>::none,
                option<char>::none, '"', {""}, {}, {}, sm)
                .map(UDF(udf_code))
                .filter(UDF("lambda x: 2000 <= x['year'] <= 2005"))
                .tocsv("hyper_local_worker_output.csv");
        double hyper_time = timer.time();
        std::cout<<"hyper mode: "<<hyper_time<<std::endl;
    }
}

// proper test for flight processing
TEST(BasicInvocation, ProperFlightsTest) {
    using namespace tuplex;
    using namespace std;

    cout<<">> starting flights (hyper) test"<<endl;

    string input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv";

    //input_pattern = "s3://tuplex-public/data/flights_on_time_performance_2009_01.10k.csv"; //"../resources/hyperspecialization/flights/*.sample";
    // get full data stats
    // input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv";

//    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1995_11.csv,../resources/hyperspecialization/flights/*.sample";
//
//    // test, constant folding should only detect year and month.
//    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv:0-247414256";

    // s3://tuplex-public/data/flights_all/flights_on_time_performance_1995_11.csv produces in hyper mode exceptions.
    // -> why? => splitsize should be 2G

    // // glob files
    // auto files = VirtualFileSystem::globAll(input_pattern);
    // vector<size_t> uri_sizes;
    // cout<<"-- Found "<<pluralize(files.size(), "file")<<endl;
    // for(const auto& uri: files) {
    //     size_t uri_size = 0;
    //     auto vfs = VirtualFileSystem::fromURI(uri);
    //     vfs.file_size(uri, reinterpret_cast<uint64_t&>(uri_size));
    //     uri_sizes.push_back(uri_size);
    //     cout<<"  - "<<uri.toString()<<" ("<<uri_size<<", "<<sizeToMemString(uri_size)<<")"<<endl;
    // }
    //
    // // reference row counts obtained from wc -l
    // // this includes the header row.
    // std::unordered_map<string, size_t> ref_row_counts{{"flights_on_time_performance_1987_10.csv", 448621},
    //                                                    {"flights_on_time_performance_2000_10.csv", 485762},
    //                                                    {"flights_on_time_performance_2021_11.csv", 547560}};
    //
    // // before parsing, make sure runtime memory is initialized
    // // init runtime memory
    // runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());
    //
    // // check number of lines/rows in each file
    // for(const auto& uri : files) {
    //     size_t number_of_rows = 0;
    //     auto reader = make_unique<CSVReader>(&number_of_rows, csv_dummy_row_functor, true, 0, nullptr, 110, ',');
    //     reader->read(uri);
    //     cout<<"  - "<<uri.toString()<<" number of rows: "<<number_of_rows<<" rows read: "<<reader->inputRowCount()<<endl;
    //     EXPECT_EQ(ref_row_counts[uri.basename()], number_of_rows);
    // }

    bool resolve_with_interpreter_only = false;

    // local worker mode for easier debugging
    ContextOptions co = ContextOptions::defaults();
    co.set("tuplex.backend", "worker");

    // activate optimizations
    co.set("tuplex.optimizer.selectionPushdown", "true");
    co.set("tuplex.optimizer.filterPushdown", "true");
    co.set("tuplex.optimizer.constantFoldingOptimization", "true"); // run with constant folding on/off
    co.set("tuplex.filterPromotion", "true");
    co.set("tuplex.optimizer.nullValueOptimization", "true");
    co.set("tuplex.experimental.hyperspecialization", "false"); // first check that THIS is correct.
    co.set("tuplex.experimental.s3PreCacheSize", "1G");
    co.set("tuplex.inputSplitSize", "2GB");
    co.set("tuplex.resolveWithInterpreterOnly", boolToString(resolve_with_interpreter_only));

    auto co_hyper = co;
    co_hyper.set("tuplex.experimental.hyperspecialization", "true");


    // use sampling mode first/last file, first/last rows
    SamplingMode sm = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;

    // init runtime
    auto rc_runtime = runtime::init(co.RUNTIME_LIBRARY().toPath());
    ASSERT_TRUE(rc_runtime);


    double hyper_time = 0;
    double nohyper_time = 0;
    // non-hyper vs. hyper (there needs to be a runtime difference.
    {
        python::initInterpreter();
        python::unlockGIL();
        Context ctx(co);
        auto udf_code = flights_code();

        Timer timer;
        ctx.csv(input_pattern, {}, option<bool>::none,
                option<char>::none, '"', {""}, {}, {}, sm)
            .map(UDF(udf_code))
            //.map(UDF("lambda x: {'year':x['YEAR'], 'nas':x['NAS_DELAY']}"))
            .tocsv("nohyper_local_worker_output.csv");
        nohyper_time = timer.time();
        std::cout<<"no-hyper mode: "<<nohyper_time<<std::endl;
    }

    {
        // bug: workerapp shutsdown interpreter -> needs to be fixed!
        python::initInterpreter();
        python::unlockGIL();
        Context ctx(co_hyper);
        auto udf_code = flights_code();

        Timer timer;
        ctx.csv(input_pattern, {}, option<bool>::none,
                option<char>::none, '"', {""}, {}, {}, sm)
                .map(UDF(udf_code))
                //.map(UDF("lambda x: {'year':x['YEAR'], 'nas':x['NAS_DELAY']}"))
                .tocsv("hyper_local_worker_output.csv");
        hyper_time = timer.time();
        std::cout<<"hyper mode: "<<hyper_time<<std::endl;
    }

    // final:
    std::cout<<"result:::"<<std::endl;
    std::cout<<"   hyper: "<<hyper_time<<std::endl;
    std::cout<<"no hyper: "<<nohyper_time<<std::endl;


//    ctx.csv(input_pattern, {}, option<bool>::none,
//            option<char>::none, '"', {""}, {}, {}, sm).map(UDF(udf_code)).tocsv("local_worker_output.csv");

    // // let's use a simple pipeline to make sure everything works
    // ctx.csv(input_pattern).selectColumns(std::vector<std::string>{"YEAR"}).tocsv("year_extract.csv");

    python::lockGIL();
    python::closeInterpreter();
}


TEST(BasicInvocation, SingleMessageDebug) {
    using namespace tuplex;
    using namespace std;

    // need to init AWS SDK...
#ifdef BUILD_WITH_AWS
    {
        // init AWS SDK to get access to S3 filesystem
        auto& logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }
#endif


    // start worker within same process to easier debug...
    auto ws = WorkerSettings();
    auto app = make_unique<WorkerApp>(ws);

    // problematic for hyoer: s3://tuplex-public/data/github_daily/2020-10-15.json:1610612736-1879048192

    string test_input_uri = "s3://tuplex-public/data/github_daily/2020-10-15.json:3758096384-4026531840";
    test_input_uri = "s3://tuplex-public/data/github_daily/2020-10-15.json:5637144576-5905580032";

    test_input_uri = "s3://tuplex-public/data/github_daily/2013-10-15.json:268435456-533607666";

    // auto message_path = "/home/leonhard/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_1.json";

    auto message_path = "/Users/leonhards/projects/tuplex-public/tuplex/cmake-build-debug/dist/bin/request_16.json";

#ifndef __APPLE__
#ifdef BUILD_WITH_CEREAL
    message_path = "/home/leonhard/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_1521.json";
#else
    message_path = "/home/leonhard/projects/tuplex-public/tuplex/cmake-build-debug/dist/bin/request_1.json";
#endif
#else
#ifdef BUILD_WITH_CEREAL
    message_path = "/Users/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_1086.json";
#else
    message_path = "/Users/leonhards/projects/tuplex-public/tuplex/cmake-build-debug/dist/bin/request_1.json";
#endif
#endif


    // debug bad request here
    message_path = "/home/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/request_0.json";

    auto message = fileToString(URI(message_path));

    // check individual messages that they work
    auto rc = app->processJSONMessage(message); // <-- second file is the critical one where something goes wrong...
    EXPECT_EQ(rc, WORKER_OK);
}

TEST(BasicInvocation, GithubProcessing) {
    using namespace std;
    using namespace tuplex;

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"-- GITHUB workload testing --"<<endl;
    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // test config here:
    // local csv test file!
    auto test_path = URI("file://../../../resources/hyperspecialization/github_daily/*.json.sample");
    auto test_output_path = URI("file://output.txt");
    auto spillURI = std::string("spill_folder");

    auto hyper_mode = true;

//    // S3 paths?
//    test_path = URI("s3://tuplex-public/data/flights_on_time_performance_2009_01.csv");
//    test_output_path = URI("file://output_s3.txt");
//
//    // local for quicker dev
//    test_path = URI("file:///Users/leonhards/data/flights/flights_on_time_performance_2009_01.csv");
//
//    // test using S3
//    test_path = URI("s3://tuplex-public/data/flights_on_time_performance_2009_01.csv");
//    test_output_path = URI(std::string("s3://") + S3_TEST_BUCKET + "/output/output_s3.txt");
//    scratchDir = URI(std::string("s3://") + S3_TEST_BUCKET + "/scratch/").toString();
//    spillURI = URI(std::string("s3://") + S3_TEST_BUCKET + "/scratch/spill_folder/").toString();

    size_t num_threads = 4;
    num_threads = 2;

    // need to init AWS SDK...
#ifdef BUILD_WITH_AWS
    {
        // init AWS SDK to get access to S3 filesystem
        auto& logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }
#endif

    // invoke helper with --help
    std::string work_dir = cur_dir;

    tuplex::Timer timer;
    auto res_stdout = runCommand(worker_path + " --help");
    auto worker_invocation_duration = timer.time();
    cout<<res_stdout<<endl;
    cout<<"Invoking worker took: "<<worker_invocation_duration<<"s"<<endl;

    // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();


    auto enable_nvo = false; // test later with true! --> important for everything to work properly together!
    co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
    auto tstage = create_github_stage(test_path.toString(), test_output_path.toString(), enable_nvo, co, hyper_mode);

    // update test path
    auto paths = glob(test_path.toPath());

    // s3 test: glob for actual data to debug failures (prob. missing rtfreeall)
    // string s3_pattern = "s3://tuplex-public/data/github_daily/*.json";
    // auto uris = VirtualFileSystem::globAll(s3_pattern);
    // paths.clear();
    // for(auto uri : uris) {
    //     std::cout<<"Found S3 uri: "<<uri.toString()<<std::endl;
    //     paths.push_back(uri.toString());
    // }



//    paths.clear();
//    URI s3_test_uri("s3://tuplex-public/data/github_daily/2013-10-15.json"); // this is off...
//    paths.push_back("s3://tuplex-public/data/github_daily/2011-10-15.json");
//    paths.push_back(s3_test_uri.toString());


    // // local test files for linux
    // paths.clear();
    // paths.push_back("/home/leonhard/Downloads/2011-10-15.json");
    // paths.push_back("/home/leonhard/Downloads/2013-10-15.json");

    // use 2011 and 2013 but determine pipeline based on 2011
    // pattern = "s3://tuplex-public/data/github_daily/2011*.json,s3://tuplex-public/data/github_daily/2013*.json";

    std::vector<std::string> messages;
    unsigned pos = 0;
    bool use_interpreter_only = false;
    num_threads = 2; // single threaded. // 'tuplex.aws.lambdaThreads': '2'
    for(const auto& path : paths) {
        // transform to message
        vfs = VirtualFileSystem::fromURI(path);
        uint64_t input_file_size = 0;
        vfs.file_size(path, input_file_size);
        auto json_message = transformStageToReqMessage(tstage, URI(path).toPath(),
                                                       input_file_size, "output_" + std::to_string(++pos) + ".csv",
                                                       use_interpreter_only,
                                                       num_threads,
                                                       spillURI);

        // save to file
        auto msg_file = URI("test_message.json");
        stringToFile(msg_file, json_message);
        messages.push_back(json_message);
    }

    python::lockGIL();
    python::closeInterpreter();



    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(WorkerSettings());

    // problematic for hyoer: s3://tuplex-public/data/github_daily/2020-10-15.json:1610612736-1879048192

    string test_input_uri = "s3://tuplex-public/data/github_daily/2020-10-15.json:3758096384-4026531840";
    test_input_uri = "s3://tuplex-public/data/github_daily/2020-10-15.json:5637144576-5905580032";

    test_input_uri = "s3://tuplex-public/data/github_daily/2013-10-15.json:268435456-533607666";

    //// reverse
    //std::reverse(messages.begin(), messages.end());

    // check all messages
     for(const auto& message : messages) {
        cout<<"Processing message..."<<endl;
        app->processJSONMessage(message);

        // get info (paths and exceptions)
        auto json_info = app->jsonStats();
        cout<<json_info<<endl;
     }

    app->shutdown();

    cout<<"-- test done."<<endl;
}

namespace tuplex {
    void aws_init() {
        // need to init AWS SDK...
    #ifdef BUILD_WITH_AWS
        {
            // init AWS SDK to get access to S3 filesystem
            auto& logger = Logger::instance().logger("aws");
            auto aws_credentials = AWSCredentials::get();
            auto options = ContextOptions::defaults();
            Timer timer;
            bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
            logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
        }
    #endif
    }
}


TEST(BasicInvocation, CSVResamplingTest) {
    using namespace std;
    using namespace tuplex;

    // test using S3
    auto test_path = URI("s3://tuplex-public/data/flights_on_time_performance_2009_01.csv");

    aws_init();

    tuplex::Timer timer;

    // check why resampling//csv parsinbg takes so long
    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();

    // sampling size, increase to make it more dominant
    co.set("tuplex.sample.maxDetectionMemory", "4MB");
    co.set("tuplex.sample.maxDetectionRows", "100000");

    auto enable_nvo = false; // test later with true! --> important for everything to work properly together!
    co.set("tuplex.optimizer.nullValueOptimization", enable_nvo ? "true" : "false");
    codegen::StageBuilderConfiguration conf(co);
    codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                       option<bool>(true),
                                               option<char>(','), option<char>('"'),
            {""}, {}, {}, {}, DEFAULT_SAMPLING_MODE));

    // now resampling test, why is it so slow?
    std::vector<URI> uris;
    std::vector<size_t> uri_sizes;

    uris.push_back(URI("s3://tuplex-public/data/flights_on_time_performance_2010_01.csv"));
    for(auto& uri : uris) {
        auto vfs = VirtualFileSystem::fromURI(uri);
        size_t uri_size = 0;
        vfs.file_size(uri, reinterpret_cast<uint64_t&>(uri_size));
        uri_sizes.push_back(uri_size);
    }
    timer.reset();
    csvop->setInputFiles(uris, uri_sizes, true);
    std::cout<<"resampling took: "<<timer.time()<<"s"<<std::endl;
}

TEST(BasicInvocation, Worker) {
    using namespace std;
    using namespace tuplex;

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // test config here:
    // local csv test file!
    auto test_path = URI("file://../resources/flights_on_time_performance_2019_01.sample.csv");
    auto test_output_path = URI("file://output.txt");
    auto spillURI = std::string("spill_folder");

    // S3 paths?
    test_path = URI("s3://tuplex-public/data/flights_on_time_performance_2009_01.csv");
    test_output_path = URI("file://output_s3.txt");

    // local for quicker dev
    test_path = URI("file:///Users/leonhards/data/flights/flights_on_time_performance_2009_01.csv");

    // test using S3
    test_path = URI("s3://tuplex-public/data/flights_on_time_performance_2009_01.csv");
    test_output_path = URI(std::string("s3://") + S3_TEST_BUCKET + "/output/output_s3.txt");
    scratchDir = URI(std::string("s3://") + S3_TEST_BUCKET + "/scratch/").toString();
    spillURI = URI(std::string("s3://") + S3_TEST_BUCKET + "/scratch/spill_folder/").toString();

    size_t num_threads = 4;
    num_threads = 4;

    // need to init AWS SDK...
#ifdef BUILD_WITH_AWS
    {
        // init AWS SDK to get access to S3 filesystem
        auto& logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }
#endif

    // invoke helper with --help
    std::string work_dir = cur_dir;

    tuplex::Timer timer;
    auto res_stdout = runCommand(worker_path + " --help");
    auto worker_invocation_duration = timer.time();
    cout<<res_stdout<<endl;
    cout<<"Invoking worker took: "<<worker_invocation_duration<<"s"<<endl;

    // Step 1: create ref pipeline using direct Tuplex invocation
    auto test_ref_path = testName + "_ref.csv";
    createRefPipeline(test_path.toString(), test_ref_path, scratchDir);

    // load ref file from parts!
    auto ref_files = glob(current_working_directory() + "/" + testName + "_ref*part*csv");
    std::sort(ref_files.begin(), ref_files.end());

    // merge into single large string
    std::string ref_content = "";
    for(unsigned i = 0; i < ref_files.size(); ++i) {
        if(i > 0) {
            auto file_content = fileToString(ref_files[i]);
            // skip header for these parts
            auto idx = file_content.find("\n");
            ref_content += file_content.substr(idx + 1);
        } else {
            ref_content = fileToString(ref_files[0]);
        }
    }
    stringToFile("ref_content.txt", ref_content);
    ref_content = fileToString("ref_content.txt");

    // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
    ASSERT_TRUE(test_path.exists());
    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();
    auto enable_nvo = false; // test later with true! --> important for everything to work properly together!
    co.set("tuplex.optimizer.nullValueOptimization", enable_nvo ? "true" : "false");
    codegen::StageBuilderConfiguration conf(co);
    codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                       option<bool>(true),
                                               option<char>(','), option<char>('"'),
            {""}, {}, {}, {}, DEFAULT_SAMPLING_MODE));
    auto mapop = std::make_shared<MapOperator>(csvop, UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}"), csvop->columns());
    auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());
    builder.addFileInput(csvop);
    builder.addOperator(mapop);
    builder.addFileOutput(fop);

    auto tstage = builder.build();

    // transform to message
    vfs = VirtualFileSystem::fromURI(test_path);
    uint64_t input_file_size = 0;
    vfs.file_size(test_path, input_file_size);
    auto json_message = transformStageToReqMessage(tstage, URI(test_path).toPath(),
                                                   input_file_size, test_output_path.toString(),
                                                   false,
                                                   num_threads,
                                                   spillURI);

    // save to file
    auto msg_file = URI("test_message.json");
    stringToFile(msg_file, json_message);

    python::lockGIL();
    python::closeInterpreter();

    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(WorkerSettings());
    app->processJSONMessage(json_message);
    app->shutdown();

#ifdef BUILD_WITH_AWS
    // reinit SDK, b.c. app shutdown also closes AWS SDK.
    initAWS();
#endif

    // fetch output file and check contents...
    auto file_content = fileToString(test_output_path);

    // check files are 1:1 the same!
    EXPECT_EQ(file_content.size(), ref_content.size());

    // because order may be different (unless specified), split into lines & sort and compare
    auto res_lines = splitToLines(file_content);
    auto ref_lines = splitToLines(ref_content);
    std::sort(res_lines.begin(), res_lines.end());
    std::sort(ref_lines.begin(), ref_lines.end());
    ASSERT_EQ(res_lines.size(), ref_lines.size());
    for(unsigned i = 0; i < std::min(res_lines.size(), ref_lines.size()); ++i) {
        EXPECT_EQ(res_lines[i], ref_lines[i]);
    }

    // test again, but this time invoking the worker with a message as separate process.

    // invoke worker with that message
    timer.reset();
    auto cmd = worker_path + " -m " + msg_file.toPath();
    res_stdout = runCommand(cmd);
    worker_invocation_duration = timer.time();
    cout<<res_stdout<<endl;
    cout<<"Invoking worker took: "<<worker_invocation_duration<<"s"<<endl;

    // check result contents
    cout<<"Checking worker results..."<<endl;
    file_content = fileToString(test_output_path);

    // check files are 1:1 the same!
    ASSERT_EQ(file_content.size(), ref_content.size());
    res_lines = splitToLines(file_content);
    std::sort(res_lines.begin(), res_lines.end());
    EXPECT_EQ(res_lines.size(), ref_lines.size());
    for(unsigned i = 0; i < std::min(res_lines.size(), ref_lines.size()); ++i) {
        EXPECT_EQ(res_lines[i], ref_lines[i]);
    }
    cout<<"Invoked worker check done."<<endl;

    cout<<"Test done."<<endl;
}

TEST(BasicInvocation, FileSplitting) {
    // test file splitting function (into parts)
    using namespace tuplex;

    std::vector<URI> uris;
    std::vector<size_t> sizes;
    std::vector<std::vector<FilePart>> res;


    // 0. avoiding tiny parts
    res = splitIntoEqualParts(2, {"test.csv"}, {8000}, 4096);
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].size(), 1);
    EXPECT_EQ(res[1].size(), 0);

    res = splitIntoEqualParts(2, {"test.csv"}, {32256}, 4096);
    ASSERT_EQ(res.size(), 2);

    // 1. equally sized files -> each thread should get same number of files (i.e. here 2)
    for(int i = 0; i < 14; ++i) {
        uris.push_back("file" + std::to_string(i) + ".csv");
        sizes.push_back(2048);
    }
    res = splitIntoEqualParts(7, uris, sizes, 0);
    ASSERT_EQ(res.size(), 7);
    for(int i = 0; i < res.size(); ++i) {
        const auto& v = res[i];

        // each thread should have two full files
        ASSERT_EQ(v.size(), 2);
        EXPECT_EQ(v[0].rangeEnd, 0);
        EXPECT_EQ(v[1].rangeEnd, 0);

        for(auto el : v) {
            std::cout<<"Thread "<<i<<": "<<el.uri.toPath()<<" start: "<<el.rangeStart<<" end: "<<el.rangeEnd<<std::endl;
        }
    }

    // split one large file into parts...
    res = splitIntoEqualParts(7, {"test.csv"}, {10000}, 0);
    ASSERT_EQ(res.size(), 7);

    res = splitIntoEqualParts(5, {"test.csv"}, {10000}, 0);
    ASSERT_EQ(res.size(), 5);
}

// faulty test, skip
//TEST(BasicInvocation, SelfInvoke) {
//    using namespace tuplex;
//
//    // test selfInvoke function...
//
//    auto cred = AWSCredentials::get();
//    NetworkSettings ns;
//
//    initAWS(cred, ns);
//
//    auto ids = selfInvoke("tuplex-lambda-runner", 4, 1500, 75, cred, ns);
//
//    shutdownAWS();
//}
namespace tuplex {
    TransformStage* create_flights_stage(const std::string& test_path,
                                         const std::string& test_output_path,
                                         bool build_for_hyper,
                                         const tuplex::SamplingMode& mode = tuplex::DEFAULT_SAMPLING_MODE) {

        using namespace tuplex;
        using namespace std;

        auto udf_code = "def fill_in_delays(row):\n"
                        "    # want to fill in data for missing carrier_delay, weather delay etc.\n"
                        "    # only need to do that prior to 2003/06\n"
                        "    \n"
                        "    year = row['YEAR']\n"
                        "    month = row['MONTH']\n"
                        "    arr_delay = row['ARR_DELAY']\n"
                        "    \n"
                        "    if year == 2003 and month < 6 or year < 2003:\n"
                        "        # fill in delay breakdown using model and complex logic\n"
                        "        if arr_delay is None:\n"
                        "            # stays None, because flight arrived early\n"
                        "            # if diverted though, need to add everything to div_arr_delay\n"
                        "            return {'year' : year, 'month' : month,\n"
                        "                    'day' : row['DAY_OF_MONTH'],\n"
                        "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                        "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                        "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                        "                    'dest': row['DEST_AIRPORT_ID'],\n"
                        "                    'distance' : row['DISTANCE'],\n"
                        "                    'dep_delay' : row['DEP_DELAY'],\n"
                        "                    'arr_delay': None,\n"
                        "                    'carrier_delay' : None,\n"
                        "                    'weather_delay': None,\n"
                        "                    'nas_delay' : None,\n"
                        "                    'security_delay': None,\n"
                        "                    'late_aircraft_delay' : None}\n"
                        "        elif arr_delay < 0.:\n"
                        "            # stays None, because flight arrived early\n"
                        "            # if diverted though, need to add everything to div_arr_delay\n"
                        "            return {'year' : year, 'month' : month,\n"
                        "                    'day' : row['DAY_OF_MONTH'],\n"
                        "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                        "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                        "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                        "                    'dest': row['DEST_AIRPORT_ID'],\n"
                        "                    'distance' : row['DISTANCE'],\n"
                        "                    'dep_delay' : row['DEP_DELAY'],\n"
                        "                    'arr_delay': row['ARR_DELAY'],\n"
                        "                    'carrier_delay' : None,\n"
                        "                    'weather_delay': None,\n"
                        "                    'nas_delay' : None,\n"
                        "                    'security_delay': None,\n"
                        "                    'late_aircraft_delay' : None}\n"
                        "        elif arr_delay < 5.:\n"
                        "            # it's an ontime flight, just attribute any delay to the carrier\n"
                        "            carrier_delay = arr_delay\n"
                        "            # set the rest to 0\n"
                        "            # ....\n"
                        "            return {'year' : year, 'month' : month,\n"
                        "                    'day' : row['DAY_OF_MONTH'],\n"
                        "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                        "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                        "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                        "                    'dest': row['DEST_AIRPORT_ID'],\n"
                        "                    'distance' : row['DISTANCE'],\n"
                        "                    'dep_delay' : row['DEP_DELAY'],\n"
                        "                    'arr_delay': row['ARR_DELAY'],\n"
                        "                    'carrier_delay' : carrier_delay,\n"
                        "                    'weather_delay': None,\n"
                        "                    'nas_delay' : None,\n"
                        "                    'security_delay': None,\n"
                        "                    'late_aircraft_delay' : None}\n"
                        "        else:\n"
                        "            # use model to determine everything and set into (join with weather data?)\n"
                        "            # i.e., extract here a couple additional columns & use them for features etc.!\n"
                        "            crs_dep_time = float(row['CRS_DEP_TIME'])\n"
                        "            crs_arr_time = float(row['CRS_ARR_TIME'])\n"
                        "            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])\n"
                        "            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time\n"
                        "            weather_delay = 2000 + 0.09 * carrier_delay * (carrier_delay - 10.0)\n"
                        "            nas_delay = 3600 * crs_dep_time / 10.0\n"
                        "            security_delay = 7200 / crs_dep_time\n"
                        "            late_aircraft_delay = (20 + 1.0) / (2.0 + crs_dep_time)\n"
                        "            return {'year' : year, 'month' : month,\n"
                        "                    'day' : row['DAY_OF_MONTH'],\n"
                        "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                        "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                        "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                        "                    'dest': row['DEST_AIRPORT_ID'],\n"
                        "                    'distance' : row['DISTANCE'],\n"
                        "                    'dep_delay' : row['DEP_DELAY'],\n"
                        "                    'arr_delay': row['ARR_DELAY'],\n"
                        "                    'carrier_delay' : carrier_delay,\n"
                        "                    'weather_delay': weather_delay,\n"
                        "                    'nas_delay' : nas_delay,\n"
                        "                    'security_delay': security_delay,\n"
                        "                    'late_aircraft_delay' : late_aircraft_delay}\n"
                        "    else:\n"
                        "        # just return it as is\n"
                        "        return {'year' : year, 'month' : month,\n"
                        "                'day' : row['DAY_OF_MONTH'],\n"
                        "                'carrier': row['OP_UNIQUE_CARRIER'],\n"
                        "                'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                        "                'origin': row['ORIGIN_AIRPORT_ID'],\n"
                        "                'dest': row['DEST_AIRPORT_ID'],\n"
                        "                'distance' : row['DISTANCE'],\n"
                        "                'dep_delay' : row['DEP_DELAY'],\n"
                        "                'arr_delay': row['ARR_DELAY'],\n"
                        "                'carrier_delay' : row['CARRIER_DELAY'],\n"
                        "                'weather_delay':row['WEATHER_DELAY'],\n"
                        "                'nas_delay' : row['NAS_DELAY'],\n"
                        "                'security_delay': row['SECURITY_DELAY'],\n"
                        "                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}";


        // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
        // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
        TransformStage *tstage = nullptr;

        // try {
        ContextOptions co = ContextOptions::defaults();
        auto enable_nvo = true; // test later with true! --> important for everything to work properly together!
        co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
        co.set("tuplex.optimizer.constantFoldingOptimization", "true");
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.sample.maxDetectionMemory", "32KB");
        codegen::StageBuilderConfiguration conf(co);
        codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
        auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path, co,
                                                                                   option<bool>::none,
                                                                                   option<char>::none, option<char>::none,
                                                                                   std::vector<std::string>({""}), {}, {}, {}, mode));
        auto mapop = std::make_shared<MapOperator>(csvop, UDF(udf_code), csvop->columns());
        auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());


        // optimize operators (projection pushdown!)
        auto logical_opt = std::make_unique<LogicalOptimizer>(co);
        auto opt_action = logical_opt->optimize(fop, true);

        // create operator chain
        std::vector<std::shared_ptr<LogicalOperator>> ops;
        auto node = opt_action;
        while(node) {
            ops.push_back(node);
            node = node->parent();
        }
        std::reverse(ops.begin(), ops.end());

        // now add to builder
        for(auto op : ops) {
            if(op->type() == LogicalOperatorType::FILEINPUT)
                builder.addFileInput(std::dynamic_pointer_cast<FileInputOperator>(op));
            else if(op->type() == LogicalOperatorType::FILEOUTPUT)
                builder.addFileOutput(std::dynamic_pointer_cast<FileOutputOperator>(op));
            else
                builder.addOperator(op);
        }

        if(!build_for_hyper)
            tstage = builder.build();
        else
            tstage = builder.encodeForSpecialization(nullptr, nullptr, true, false, true);

        return tstage;
    }
}

tuplex::TransformStage* create_flights_pipeline(const std::string& test_path, const std::string& test_output_path, bool build_for_hyper, const tuplex::SamplingMode& mode = tuplex::DEFAULT_SAMPLING_MODE) {

    using namespace tuplex;
    using namespace std;
//
//
//    auto udf_code = "def fill_in_delays(row):\n"
//                    "    # want to fill in data for missing carrier_delay, weather delay etc.\n"
//                    "    # only need to do that prior to 2003/06\n"
//                    "    \n"
//                    "    year = row['YEAR']\n"
//                    "    month = row['MONTH']\n"
//                    "    \n"
//                    "    if year == 2003 and month < 6:\n"
//                    "         return {'x': row['DEP_DELAY']}\n"
//                    "    else:\n"
//                    "         return {'x': row['ARR_DELAY']}\n";

    auto udf_code = "def fill_in_delays(row):\n"
                    "    # want to fill in data for missing carrier_delay, weather delay etc.\n"
                    "    # only need to do that prior to 2003/06\n"
                    "    \n"
                    "    year = row['YEAR']\n"
                    "    month = row['MONTH']\n"
                    "    arr_delay = row['ARR_DELAY']\n"
                    "    \n"
                    "    if year == 2003 and month < 6 or year < 2003:\n"
                    "        # fill in delay breakdown using model and complex logic\n"
                    "        if arr_delay is None:\n"
                    "            # stays None, because flight arrived early\n"
                    "            # if diverted though, need to add everything to div_arr_delay\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': None,\n"
                    "                    'carrier_delay' : None,\n"
                    "                    'weather_delay': None,\n"
                    "                    'nas_delay' : None,\n"
                    "                    'security_delay': None,\n"
                    "                    'late_aircraft_delay' : None}\n"
                    "        elif arr_delay < 0.:\n"
                    "            # stays None, because flight arrived early\n"
                    "            # if diverted though, need to add everything to div_arr_delay\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : None,\n"
                    "                    'weather_delay': None,\n"
                    "                    'nas_delay' : None,\n"
                    "                    'security_delay': None,\n"
                    "                    'late_aircraft_delay' : None}\n"
                    "        elif arr_delay < 5.:\n"
                    "            # it's an ontime flight, just attribute any delay to the carrier\n"
                    "            carrier_delay = arr_delay\n"
                    "            # set the rest to 0\n"
                    "            # ....\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : carrier_delay,\n"
                    "                    'weather_delay': None,\n"
                    "                    'nas_delay' : None,\n"
                    "                    'security_delay': None,\n"
                    "                    'late_aircraft_delay' : None}\n"
                    "        else:\n"
                    "            # use model to determine everything and set into (join with weather data?)\n"
                    "            # i.e., extract here a couple additional columns & use them for features etc.!\n"
                    "            crs_dep_time = float(row['CRS_DEP_TIME'])\n"
                    "            crs_arr_time = float(row['CRS_ARR_TIME'])\n"
                    "            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])\n"
                    "            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time\n"
                    "            weather_delay = 2000 + 0.09 * carrier_delay * (carrier_delay - 10.0)\n"
                    "            nas_delay = 3600 * crs_dep_time / 10.0\n"
                    "            security_delay = 7200 / crs_dep_time\n"
                    "            late_aircraft_delay = (20 + 1.0) / (2.0 + crs_dep_time)\n"
                    "            return {'year' : year, 'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                    'dest': row['DEST_AIRPORT_ID'],\n"
                    "                    'distance' : row['DISTANCE'],\n"
                    "                    'dep_delay' : row['DEP_DELAY'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : carrier_delay,\n"
                    "                    'weather_delay': weather_delay,\n"
                    "                    'nas_delay' : nas_delay,\n"
                    "                    'security_delay': security_delay,\n"
                    "                    'late_aircraft_delay' : late_aircraft_delay}\n"
                    "    else:\n"
                    "        # just return it as is\n"
                    "        return {'year' : year, 'month' : month,\n"
                    "                'day' : row['DAY_OF_MONTH'],\n"
                    "                'carrier': row['OP_UNIQUE_CARRIER'],\n"
                    "                'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                    "                'origin': row['ORIGIN_AIRPORT_ID'],\n"
                    "                'dest': row['DEST_AIRPORT_ID'],\n"
                    "                'distance' : row['DISTANCE'],\n"
                    "                'dep_delay' : row['DEP_DELAY'],\n"
                    "                'arr_delay': row['ARR_DELAY'],\n"
                    "                'carrier_delay' : row['CARRIER_DELAY'],\n"
                    "                'weather_delay':row['WEATHER_DELAY'],\n"
                    "                'nas_delay' : row['NAS_DELAY'],\n"
                    "                'security_delay': row['SECURITY_DELAY'],\n"
                    "                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}";


        // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
    TransformStage *tstage = nullptr;
    python::initInterpreter();
    python::unlockGIL();
    // try {
        ContextOptions co = ContextOptions::defaults();
        auto enable_nvo = true; // test later with true! --> important for everything to work properly together!
        co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
        co.set("tuplex.optimizer.constantFoldingOptimization", "true");
        co.set("tuplex.optimizer.constantFoldingOptimization", "false");
        co.set("tuplex.sample.maxDetectionMemory", "32KB");
        codegen::StageBuilderConfiguration conf(co);
        codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
        auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path, co,
                                                                                   option<bool>::none,
                                                                                   option<char>::none, option<char>::none,
                                                                                   std::vector<std::string>({""}), {}, {}, {}, mode));
        auto mapop = std::make_shared<MapOperator>(csvop, UDF(udf_code), csvop->columns());
        auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());


        // optimize operators (projection pushdown!)
        auto logical_opt = std::make_unique<LogicalOptimizer>(co);
        auto v = std::vector<std::shared_ptr<LogicalOperator>>{csvop, mapop, fop};
        auto opt_ops = logical_opt->optimize(v, true);

        // TODO: logical opt here?
        // => somehow on Lambda weird stuff happens...

//        // new: using separate logical optimizer class
//        auto opt = std::make_unique<LogicalOptimizer>(context.getOptions());
//        fop =

        builder.addFileInput(csvop);
        builder.addOperator(mapop);
        builder.addFileOutput(fop);
        if(!build_for_hyper)
            tstage = builder.build();
        else
            tstage = builder.encodeForSpecialization(nullptr, nullptr, true, false, true);
//    } catch(const std::exception& e) {
//        std::cerr<<"Exception occurred: "<<e.what()<<std::endl;
//        python::lockGIL();
//        python::closeInterpreter();
//        return nullptr;
//    } catch(...) {
//        std::cerr<<"Exception occurred! Failure"<<std::endl;
//        python::lockGIL();
//        python::closeInterpreter();
//        return nullptr;
//    }

    python::lockGIL();
    python::closeInterpreter();
    return tstage;
}

namespace tuplex {
    DataSet& github_pipeline(Context& ctx, const std::string& pattern) {
        // in order to extract repo -> for 2012 till 2014 incl. repo key is called repository!
        // => unknown key will trigger badparseinput exception.

        auto repo_id_code = "def extract_repo_id(row):\n"
                            "\tif 2012 <= row['year'] <= 2014:\n"
                            "\t\treturn row['repository']['id']\n"
                            "\telse:\n"
                            "\t\treturn row['repo']['id']\n";

        return ctx.json(pattern)
                  .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                  .withColumn("repo_id", UDF(repo_id_code))
                  .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                  .selectColumns(std::vector<std::string>({"type", "repo_id", "year"}));
    }

    void init_aws_for_this_test() {
        // need to init AWS SDK...
#ifdef BUILD_WITH_AWS
        {
            // init AWS SDK to get access to S3 filesystem
            auto& logger = Logger::instance().logger("aws");
            auto aws_credentials = AWSCredentials::get();
            auto options = ContextOptions::defaults();
            Timer timer;
            bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
            logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
        }
#endif
    }

    std::string message_for_path(const std::string& json_message,
                                 const URI& input_uri,
                                 const URI& output_uri,
                                 size_t buffer_size = memStringToSize("10G")) {

        // modify input/output path
        // transform to message


        // parse message & update
        try {
            auto j = nlohmann::json::parse(json_message);

            // query file size b.c. message needs this (reuse basically)
            auto vfs = VirtualFileSystem::fromURI(input_uri);
            uint64_t input_file_size = 0;
            vfs.file_size(input_uri, input_file_size);

            auto input_uri_arr = nlohmann::json::array();
            input_uri_arr.push_back(input_uri.toPath());
            auto input_sizes_arr = nlohmann::json::array();
            input_sizes_arr.push_back(input_file_size);
            j["inputURIS"] = input_uri_arr;
            j["inputSizes"] = input_sizes_arr;
            j["baseOutputURI"] = output_uri.toPath();

            // update memory size in settings
            j["settings"]["normalBufferSize"] = buffer_size;
            j["settings"]["exceptionBufferSize"] = buffer_size;

            return j.dump();
        } catch(const nlohmann::json::exception& e) {
            std::cerr<<"parsing JSON resulted in exception "<<e.what();
            throw e;
        }

        return json_message;
    }

    std::string input_uri_from_message(const std::string& json_message) {
        try {
            auto j = nlohmann::json::parse(json_message);

            return j["inputURIS"][0].get<std::string>();

        } catch(const nlohmann::json::exception& e) {
            std::cerr<<"parsing JSON resulted in exception "<<e.what();
            throw e;
        }

        return "";
    }

    std::string detailed_bitcode_stats(const std::string& bitCode) {
        if(bitCode.empty())
            return "";

        std::stringstream  ss;
        bool include_detailed_counts = true;

        llvm::LLVMContext context;
        auto mod = codegen::bitCodeToModule(context, bitCode);

        char name[] = "inst count";
        InstructionCounts inst_count(*name);
        inst_count.runOnModule(*mod);
        ss<<inst_count.formattedStats(include_detailed_counts);
        return ss.str();
    }

    std::string timings_to_str(const std::vector<std::tuple<std::string, std::string, unsigned, double, std::string>>& timings) {
        std::stringstream ss;
        ss<<"mode,path,run,time,stats\n";
        for(auto t : timings) {
            ss<<std::get<0>(t)<<","<<std::get<1>(t)<<","<<std::get<2>(t)<<","<<std::get<3>(t)<<","<<quote(std::get<4>(t))<<"\n";
        }
        return ss.str();
    }

    std::string get_hostname() {
#ifndef HOST_NAME_MAX
    #ifdef MAXHOSTNAMELEN
        #define HOST_NAME_MAX MAXHOSTNAMELEN
    #else
        #define HOST_NAME_MAX 256
    #endif
#endif
        char hostname[HOST_NAME_MAX];
        gethostname(hostname, HOST_NAME_MAX);
        return hostname;
    }
}

TEST(BasicInvocation, GithubSensititivity) {
    // goal of this experiment is to use first row/last row/first file/last file
    // to get the global schema and THEN hyperspecialize for each file to get a sense of
    // a speedup factor due to hyperspecialization and plot it out.

    using namespace std;
    using namespace tuplex;

    static const string github_test_pattern = "../resources/hyperspecialization/github_daily/*.json.sample";

    // config params
    unsigned NUM_RUNS = 4;
    string data_root = github_test_pattern;

    auto hostname = get_hostname();

    // bbsn00 (HACK)
    if("bbsn00" == hostname)
        data_root = "/hot/data/github_daily/*.json";

    unsigned NUM_THREADS = 0; // single thread
    auto sampling_mode = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;
    // to be sure, use ALL_FILES sampling mode
    sampling_mode = sampling_mode | SamplingMode::ALL_FILES; // <-- global should have the right schema.
    bool enable_nvo = true;
    string memory = "32G";

    string local_work_dir = "./github_sensitivity_experiment";

    // worker settings (from config params)
    auto ws = WorkerSettings();
    ws.numThreads = NUM_THREADS;
    ws.spillRootURI = URI(local_work_dir + "/local_worker_spill/");
    ws.useConstantFolding = true;
    ws.useFilterPromotion = true;
    ws.useCompiledGeneralPath = true;
    ws.useInterpreterOnly = false;
    ws.useOptimizer = true;
    ws.normalBufferSize = memStringToSize(memory);
    ws.exceptionBufferSize = memStringToSize(memory);

    // get paths via glob
    auto vfs = VirtualFileSystem::fromURI(data_root);
    auto paths = vfs.glob(data_root);

    cout<<"Found "<<pluralize(paths.size(), "path")<<" from pattern "<<data_root<<endl;
    ASSERT_FALSE(paths.empty());

    init_aws_for_this_test();

    // create a hyper and non-hyper version
    auto input_pattern = data_root;
    ContextOptions options = ContextOptions::defaults();
    python::initInterpreter();
    python::unlockGIL();

    auto tstage_global = create_github_stage(input_pattern, "dummy", enable_nvo, options, false, sampling_mode);
    auto tstage_hyper = create_github_stage(input_pattern, "dummy", enable_nvo, options, true, sampling_mode);

    // Run optimizer over code (to reduce number of basic blocks)
    cout<<"Running LLVM optimizer to reduce complexity"<<endl;
    {

        LLVMOptimizer opt;
        cout<<"GLOBAL LLVM stats:"<<endl;
        cout<<"Detailed module stats before opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_global->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_global->slowPathBitCode())
            <<endl;
        Timer timer;
        tstage_global->optimizeBitCode(opt);
        cout<<"optimized global stage LLVM IR in "<<timer.time()<<"s"<<endl;
        cout<<"Detailed module stats after opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_global->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_global->slowPathBitCode())
            <<endl;
    }

    {
        cout<<"HYPER LLVM stats:"<<endl;
        cout<<"Detailed module stats before opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_hyper->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_hyper->slowPathBitCode())
            <<endl;
        Timer timer;
        LLVMOptimizer opt;
        tstage_hyper->optimizeBitCode(opt);
        cout<<"optimized hyper stage LLVM IR in "<<timer.time()<<"s"<<endl;
        cout<<"Detailed module stats after opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_hyper->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_hyper->slowPathBitCode())
            <<endl;
    }


    // now transform to JSON messages to pass to worker app
    auto global_json_message = transformStageToReqMessage(tstage_global, "input_dummy",
                                                          0, "output_dummy",
                                                          ws.useInterpreterOnly,
                                                          NUM_THREADS,
                                                          ws.spillRootURI.toPath());
    auto hyper_json_message = transformStageToReqMessage(tstage_hyper, "input_dummy",
                                                          0, "output_dummy",
                                                          ws.useInterpreterOnly,
                                                          NUM_THREADS,
                                                          ws.spillRootURI.toPath());
    python::lockGIL();
    python::closeInterpreter();

    // @TODO:
    // -> use single-threaded to mask effects but invoke using worker app
    // local WorkerApp
    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(ws);

    // process messages (first hyper)
    vector<string> hyper_messages;
    vector<string> global_messages;
    unsigned pos = 0;
    std::sort(paths.begin(), paths.end(), [](const URI& a, const URI& b) {
        auto path_a = a.toPath();
        auto path_b = b.toPath();
        return lexicographical_compare(path_a.begin(), path_a.end(), path_b.begin(), path_b.end());
    });

    for(const auto& path : paths) {
        string hyper_output_uri = local_work_dir + "/hyper/" + path.basename();
        hyper_messages.push_back(message_for_path(hyper_json_message, path, hyper_output_uri));

        string global_output_uri = local_work_dir + "/general/" + path.basename();
        global_messages.push_back(message_for_path(global_json_message, path, global_output_uri));

        pos++;
    }

    vector<tuple<string, string, unsigned, double, string>> timings;
    string output_path = local_work_dir + "/results.csv";

    for(unsigned run = 0; run < NUM_RUNS; ++run) {
        // process hyper messages
        cout<<"HYPER processing::"<<endl;
        Timer timer;
        for(const auto& json_message : hyper_messages) {

            auto input_path = input_uri_from_message(json_message);
            Timer msg_timer;
            app->processJSONMessage(json_message);
            timings.emplace_back("hyper", input_path, run, msg_timer.time(), app->jsonStats());

            // save intermediate to file
            stringToFile(URI(output_path), timings_to_str(timings));
            cout<<"-- processed file"<<endl;
        }
        cout<<"Hyper took "<<timer.time()<<"s for "<<pluralize(hyper_messages.size(), "message")<<endl;

        cout<<"GLOBAL processing::"<<endl;
        timer.reset();
        for(const auto& json_message : global_messages) {
            auto input_path = input_uri_from_message(json_message);
            Timer msg_timer;
            app->processJSONMessage(json_message);
            timings.emplace_back("global", input_path, run, msg_timer.time(), app->jsonStats());

            // save intermediate to file
            stringToFile(URI(output_path), timings_to_str(timings));
            cout<<"-- processed file"<<endl;
        }
        cout<<"Global took "<<timer.time()<<"s for "<<pluralize(global_messages.size(), "message")<<endl;
    }

    //
    app->shutdown();

    // print results as CSV
    cout<<"CSV results::\n"<<endl;
    cout<<timings_to_str(timings)<<endl;
    // later: use multi-threading as well and see how speed up changes.


    // each output line should be:


    #warning "implement this with github query!"

    #warning "Further todo: implement updated flights query (prob. linear regression on facors should do, could else use XGB as fitted"
}

TEST(BasicInvocation, FlightsSensititivity) {
    // goal of this experiment is to use first row/last row/first file/last file
    // to get the global schema and THEN hyperspecialize for each file to get a sense of
    // a speedup factor due to hyperspecialization and plot it out.

    using namespace std;
    using namespace tuplex;

    static const string flights_test_pattern = "../resources/hyperspecialization/flights_all/*.csv.sample";

    // config params
    unsigned NUM_RUNS = 4;
    string data_root = flights_test_pattern;

    auto hostname = get_hostname();

    // bbsn00 (HACK)
    if("bbsn00" == hostname)
        data_root = "/hot/data/flights_all/*.csv";

    unsigned NUM_THREADS = 0; // single thread
    auto sampling_mode = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE;
    // to be sure, use ALL_FILES sampling mode
    sampling_mode = sampling_mode | SamplingMode::ALL_FILES | SamplingMode::SINGLETHREADED; // <-- global should have the right schema.
    bool enable_nvo = true;
    string memory = "32G";

    string local_work_dir = "./flights_sensitivity_experiment";

    // worker settings (from config params)
    auto ws = WorkerSettings();
    ws.numThreads = NUM_THREADS;
    ws.spillRootURI = URI(local_work_dir + "/local_worker_spill/");
    ws.useConstantFolding = true;
    ws.useFilterPromotion = true;
    ws.useCompiledGeneralPath = true;
    ws.useInterpreterOnly = false;
    ws.useOptimizer = true;
    ws.normalBufferSize = memStringToSize(memory);
    ws.exceptionBufferSize = memStringToSize(memory);

    // get paths via glob
    auto vfs = VirtualFileSystem::fromURI(data_root);
    auto paths = vfs.glob(data_root);

    cout<<"Found "<<pluralize(paths.size(), "path")<<" from pattern "<<data_root<<endl;
    ASSERT_FALSE(paths.empty());

    init_aws_for_this_test();

    // create a hyper and non-hyper version
    auto input_pattern = data_root;
    ContextOptions options = ContextOptions::defaults();
    python::initInterpreter();
    python::unlockGIL();
    auto tstage_global = create_flights_stage(input_pattern, "dummy", false, sampling_mode);
    auto tstage_hyper = create_flights_stage(input_pattern, "dummy", true, sampling_mode);

    // Run optimizer over code (to reduce number of basic blocks)
    cout<<"Running LLVM optimizer to reduce complexity"<<endl;
    {

        LLVMOptimizer opt;
        cout<<"GLOBAL LLVM stats:"<<endl;
        cout<<"Detailed module stats before opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_global->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_global->slowPathBitCode())
            <<endl;
        Timer timer;
        tstage_global->optimizeBitCode(opt);
        cout<<"optimized global stage LLVM IR in "<<timer.time()<<"s"<<endl;
        cout<<"Detailed module stats after opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_global->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_global->slowPathBitCode())
            <<endl;
    }

    {
        cout<<"HYPER LLVM stats:"<<endl;
        cout<<"Detailed module stats before opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_hyper->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_hyper->slowPathBitCode())
            <<endl;
        Timer timer;
        LLVMOptimizer opt;
        tstage_hyper->optimizeBitCode(opt);
        cout<<"optimized hyper stage LLVM IR in "<<timer.time()<<"s"<<endl;
        cout<<"Detailed module stats after opt:\n- fast path:\n"
            <<detailed_bitcode_stats(tstage_hyper->fastPathBitCode())
            <<"\n"
            <<"- slow path:\n"<<detailed_bitcode_stats(tstage_hyper->slowPathBitCode())
            <<endl;
    }


    // now transform to JSON messages to pass to worker app
    auto global_json_message = transformStageToReqMessage(tstage_global, "input_dummy",
                                                          0, "output_dummy",
                                                          ws.useInterpreterOnly,
                                                          NUM_THREADS,
                                                          ws.spillRootURI.toPath());
    auto hyper_json_message = transformStageToReqMessage(tstage_hyper, "input_dummy",
                                                          0, "output_dummy",
                                                          ws.useInterpreterOnly,
                                                          NUM_THREADS,
                                                          ws.spillRootURI.toPath());
    python::lockGIL();
    python::closeInterpreter();

    // @TODO:
    // -> use single-threaded to mask effects but invoke using worker app
    // local WorkerApp
    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(ws);

    // process messages (first hyper)
    vector<string> hyper_messages;
    vector<string> global_messages;
    unsigned pos = 0;
    std::sort(paths.begin(), paths.end(), [](const URI& a, const URI& b) {
        auto path_a = a.toPath();
        auto path_b = b.toPath();
        return lexicographical_compare(path_a.begin(), path_a.end(), path_b.begin(), path_b.end());
    });

    for(const auto& path : paths) {
        string hyper_output_uri = local_work_dir + "/hyper/" + path.basename();
        hyper_messages.push_back(message_for_path(hyper_json_message, path, hyper_output_uri));

        string global_output_uri = local_work_dir + "/general/" + path.basename();
        global_messages.push_back(message_for_path(global_json_message, path, global_output_uri));

        pos++;
    }

    vector<tuple<string, string, unsigned, double, string>> timings;
    string output_path = local_work_dir + "/results.csv";

    for(unsigned run = 0; run < NUM_RUNS; ++run) {
        // process hyper messages
        cout<<"HYPER processing::"<<endl;
        Timer timer;
        for(const auto& json_message : hyper_messages) {

            auto input_path = input_uri_from_message(json_message);
            Timer msg_timer;
            app->processJSONMessage(json_message);
            timings.emplace_back("hyper", input_path, run, msg_timer.time(), app->jsonStats());

            // save intermediate to file
            stringToFile(URI(output_path), timings_to_str(timings));
            cout<<"-- processed file"<<endl;
        }
        cout<<"Hyper took "<<timer.time()<<"s for "<<pluralize(hyper_messages.size(), "message")<<endl;

        cout<<"GLOBAL processing::"<<endl;
        timer.reset();
        for(const auto& json_message : global_messages) {
            auto input_path = input_uri_from_message(json_message);
            Timer msg_timer;
            app->processJSONMessage(json_message);
            timings.emplace_back("global", input_path, run, msg_timer.time(), app->jsonStats());

            // save intermediate to file
            stringToFile(URI(output_path), timings_to_str(timings));
            cout<<"-- processed file"<<endl;
        }
        cout<<"Global took "<<timer.time()<<"s for "<<pluralize(global_messages.size(), "message")<<endl;
    }

    //
    app->shutdown();

    // print results as CSV
    cout<<"CSV results::\n"<<endl;
    cout<<timings_to_str(timings)<<endl;
}

TEST(BasicInvocation, FlightsTestSpecialization) {
    using namespace std;
    using namespace tuplex;

    auto test_path = "../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv,../resources/hyperspecialization/2021/flights_on_time_performance_2021_11.sample.csv,";
    auto test_output_path = "./general_processing/";
    int num_threads = 1;
    auto spillURI = std::string("spill_folder");
    bool use_hyper = false;
    use_hyper = true;
    auto tstage = create_flights_pipeline(test_path, test_output_path, use_hyper);

    URI test_uri("../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv");
    //URI test_uri("../resources/hyperspecialization/2021/flights_on_time_performance_2021_11.sample.csv");
    auto vfs = VirtualFileSystem::fromURI(test_uri);
    size_t test_uri_size = 0;
    vfs.file_size(test_uri, reinterpret_cast<uint64_t &>(test_uri_size));
    hyperspecialize(tstage, test_uri, test_uri_size, 200, 1, 1, codegen::StageBuilderConfiguration());
}


TEST(BasicInvocation, FlightsHyper) {
    using namespace std;
    using namespace tuplex;


    // need to init AWS SDK...
#ifdef BUILD_WITH_AWS
    {
        // init AWS SDK to get access to S3 filesystem
        auto& logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }
#endif

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // for testing purposes, store here the root path to the flights data (simple ifdef)
#ifdef __APPLE__
    // leonhards macbook
    string flights_root = "/Users/leonhards/Downloads/flights/";
    //string flights_root = "/Users/leonhards/Downloads/flights_small/";

    string driver_memory = "2G";
#else
    // BBSN00
    string flights_root = "/hot/data/flights_all/";

    string driver_memory = "32G";
    string executor_memory = "10G";
    string num_executors = "0";
    //num_executors = "16";
#endif

    // --- use this for final PR ---
    // For testing purposes: resources/hyperspecialization/2003/*.csv holds two mini samples where wrong sampling triggers too many exceptions in general case mode
    string input_pattern = cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv," + cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_12.csv";
    // --- end use this for final PR ---

//    // !!! use compatible files for inference when issuing queries, else there'll be errors.
//    input_pattern = flights_root + "flights_on_time_performance_2003_01.csv," + flights_root + "flights_on_time_performance_2003_12.csv";
//
//
    input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_2002_01.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003_11.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004_12.csv";

    auto test_path = input_pattern;
    auto test_output_path = "./general_processing/";
    int num_threads = 1;
    auto spillURI = std::string("spill_folder");
    bool use_hyper = false;
    use_hyper = true;
    auto tstage = create_flights_pipeline(test_path, test_output_path, use_hyper);

    // transform to message
    // create message only for first file!
//    auto input_uri = URI(cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_08.csv");
    auto input_uri = URI(cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv");

    input_uri = URI("s3://tuplex-public/data/flights_all/flights_on_time_performance_2001_12.csv");

//    input_uri = URI(flights_root + "flights_on_time_performance_2003_08.csv");
//    input_uri = URI(flights_root + "flights_on_time_performance_2003_01.csv");
    auto output_uri = URI(test_output_path + (use_hyper ? string("output_hyper.csv") : string("output_general.csv")));

    // test files:
    // other: s3://tuplex-public/data/flights_all/flights_on_time_performance_2017_06.csv:134217728-201326592
    // input_uri = URI("s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_08.csv:134217728-201326592");


    auto file = tuplex::VirtualFileSystem::open_file(output_uri, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);
    file.reset();

    vfs = VirtualFileSystem::fromURI(input_uri);
    uint64_t input_file_size = 0;
    vfs.file_size(input_uri, input_file_size);
    num_threads = 0;
    auto json_message = transformStageToReqMessage(tstage, input_uri.toPath(),
                                                   input_file_size, output_uri.toString(),
                                                   false,
                                                   num_threads,
                                                   spillURI);

    // local WorkerApp
    // start worker within same process to easier debug...
    auto app = make_unique<WorkerApp>(WorkerSettings());
    app->processJSONMessage(json_message);
    app->shutdown();

//    // check size of ref file
//    uint64_t output_file_size = 0;
//    vfs.file_size("file://./general_processing/output_hyper.csv.csv", output_file_size);
//    cout<<"Output size of hyper file is: "<<output_file_size<<endl;
//    vfs.file_size("file://./general_processing/output_general.csv.csv", output_file_size);
//    cout<<"Output size of general file is: "<<output_file_size<<endl;

//    // Output size of file is: 46046908 (general, no hyper)
    // 46046908

    cout<<"Test done."<<endl;
}

TEST(BasicInvocation, BasicFS) {
    using namespace tuplex;
    using namespace std;

    auto path = URI("./general_processing/testfile.csv");

    auto file = VirtualFileSystem::fromURI(path).open_file(path, VirtualFileMode::VFS_OVERWRITE);

    int64_t test_i = 42;
    file->write(&test_i, sizeof(int64_t));
    file->write(&test_i, sizeof(int64_t));

    size_t buf_size = 49762375;
    uint8_t* buf = new uint8_t[buf_size];
    for(int i = 0; i < buf_size; ++i)
	    buf[i] = i % 32;
    file->write(buf, buf_size);
    file->close();

    delete [] buf;
}


std::string basename(const std::string& s) {
    return s.substr(s.rfind('/') + 1);
}
namespace tuplex {

    bool checkFiles(const std::string root_path, const std::string basename) {
        Timer timer;
        //std::string root_path = "tests/BasicInvocationTestAllFlightFiles";
        std::string general_path = root_path + "/general_processing/" + basename;// flights_on_time_performance_2003_01.csv.csv";
        std::string hyper_path = root_path + "/hyper_processing/" + basename; // flights_on_time_performance_2003_01.csv.csv";

        // load both files as rows and check length
        auto general_data = fileToString(general_path);
        // ASSERT_TRUE(!general_data.empty());
        if(general_data.empty())
            return false;
        auto general_rows = parseRows(general_data.c_str(), general_data.c_str() + general_data.length(), {""});

        auto hyper_data = fileToString(hyper_path);
        if(hyper_data.empty())
            return false;
        // ASSERT_TRUE(!hyper_data.empty());
        auto hyper_rows = parseRows(hyper_data.c_str(), hyper_data.c_str() + hyper_data.length(), {""});
        std::cout<<"file has "<<general_rows.size()<<" general rows, "<<hyper_rows.size()<<" hyper rows"<<std::endl;

        //EXPECT_EQ(general_rows.size(), hyper_rows.size());
        if(general_rows.size() != hyper_rows.size()) {
            std::cout<<"different number of rows (general: "<<general_rows.size()<<", hyper: "<<hyper_rows.size()<<")"<<std::endl;
            return false;
        }

        bool rows_identical = true;
        if(general_rows.size() == hyper_rows.size()) {
            // compare individual rows (after sorting them!)
            std::cout<<"sorting general rows..."<<std::endl;
            std::sort(general_rows.begin(), general_rows.end());
            std::cout<<"sorting hyper rows..."<<std::endl;
            std::sort(hyper_rows.begin(), hyper_rows.end());
            std::cout<<"comparing rows 1:1..."<<std::endl;

            // go through rows and compare them one-by-one
            for(unsigned i = 0; i < general_rows.size(); ++i) {
                //EXPECT_EQ(general_rows[i], hyper_rows[i]);
                if(!(general_rows[i] == hyper_rows[i])) {
                    std::cout<<"row i="<<i<<" not equal"<<std::endl;
                    std::cout<<"hyper:   "<<hyper_rows[i].toPythonString()<<std::endl;
                    std::cout<<"general: "<<general_rows[i].toPythonString()<<std::endl;
                    std::cout<<"----"<<std::endl;
                    rows_identical = false;
                }
            }
        }

        std::cout<<"Validation took "<<timer.time()<<"s"<<std::endl;
        return rows_identical;
    }

    int checkHyperSpecialization(const URI& input_uri, TransformStage* tstage_hyper, TransformStage* tstage_general, int num_threads, const URI& spillURI) {
        using namespace std;
        int rc = 0;
        auto vfs = VirtualFileSystem::fromURI(input_uri);
        uint64_t input_file_size = 0;
        vfs.file_size(input_uri, input_file_size);

        std::unique_ptr<WorkerApp> app;
        std::string json_message_hyper;
        std::string json_message_general;
        URI output_uri;

        // prep stages
        if(tstage_hyper) {
            output_uri = tstage_hyper->outputURI().toString() + "/" + basename(input_uri.toString());
            json_message_hyper = transformStageToReqMessage(tstage_hyper, input_uri.toPath(),
                                                            input_file_size, output_uri.toString(),
                                                            false, // this here causes an error!!!
                                                            num_threads,
                                                            spillURI.toString());
        }


        if(tstage_general) {
            output_uri = tstage_general->outputURI().toString() + "/" + basename(input_uri.toString());
            json_message_general = transformStageToReqMessage(tstage_general, input_uri.toPath(),
                                                                   input_file_size, output_uri.toString(),
                                                                   false,
                                                                   num_threads,
                                                                   spillURI.toString());

            // print slowpath to file
            llvm::LLVMContext llvm_ctx;
            auto mod = codegen::bitCodeToModule(llvm_ctx, tstage_general->slowPathBitCode());
            stringToFile("general_slowpath.txt", codegen::moduleToString(*mod.get()));
            // codegen::annotateModuleWithInstructionPrint(*mod.get());
            // debug, overwrite slowpath with newly annotated module!
            tstage_general->slowPathBitCode() = codegen::moduleToBitCodeString(*mod.get());
        }



        // local WorkerApp
        // start worker within same process to easier debug...

        if(tstage_hyper) {
            std::cout<<" --- Hyper processing --- "<<std::endl;
            app = make_unique<WorkerApp>(WorkerSettings());
            app->processJSONMessage(json_message_hyper);
            app->shutdown();
            rc |= 0x1;
        }

        if(tstage_general) {
            std::cout<<" --- General processing --- "<<std::endl;
            app = make_unique<WorkerApp>(WorkerSettings());
            app->processJSONMessage(json_message_general);
            app->shutdown();
            rc |= 0x2;
        }

        if(tstage_hyper && tstage_general) {
            // now call verify function
            auto str = input_uri.toString();
            std::cout<<"input uri: "<<input_uri<<std::endl;
            auto flights_root = str.substr(0, str.find("/flights_on_time"));
            auto s_basename = output_uri.toString();
            std::cout<<"basename: "<<s_basename<<std::endl;
            s_basename = s_basename.substr(s_basename.rfind('/')+1) + ".csv";
            auto output_root = ".";//"tests/BasicInvocationTestAllFlightFiles";
            if(checkFiles(output_root, s_basename))
                rc |= 0x4;
        }

        return rc;
    }
}

TEST(BasicInvocation, RowSort) {

    using namespace tuplex;
    using namespace std;

    vector<Row> v{Row(), Row(10, 20), Row(5, 10)};

    std::sort(v.begin(), v.end());

    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "()");
    EXPECT_EQ(v[1].toPythonString(), "(5,10)");
    EXPECT_EQ(v[2].toPythonString(), "(10,20)");


    std::string general_path = "./tests/BasicInvocationTestAllFlightFiles/general_processing/flights_on_time_performance_2021_11.sample.csv.csv";// flights_on_time_performance_2003_01.csv.csv";

    // load both files as rows and check length
    auto general_data = fileToString(general_path);

    auto general_rows = parseRows(general_data.c_str(), general_data.c_str() + general_data.length(), {""});
    std::sort(general_rows.begin(), general_rows.end());
}

TEST(BasicInvocation, FSWrite) {
    using namespace tuplex;

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    //URI uri("file://./hyper_processing//flights_on_time_performance_2003_01.csv.csv");
    URI uri("./hyper_processing//flights_on_time_performance_2003_01.csv.csv");
    auto file = vfs.open_file(uri, VirtualFileMode::VFS_WRITE);
    ASSERT_TRUE(file);
    file->close();
}

TEST(BasicInvocation, VerifyOutput) {
    using namespace tuplex;

    std::string root_path = "tests/BasicInvocationTestAllFlightFiles";
    std::string general_path = root_path + "/general_processing/flights_on_time_performance_2003_01.csv.csv";
    std::string hyper_path = root_path + "/hyper_processing/flights_on_time_performance_2003_01.csv.csv";

    // load both files as rows and check length
    auto general_data = fileToString(general_path);
    ASSERT_TRUE(!general_data.empty());
    auto general_rows = parseRows(general_data.c_str(), general_data.c_str() + general_data.length(), {""});

    auto hyper_data = fileToString(hyper_path);
    ASSERT_TRUE(!hyper_data.empty());
    auto hyper_rows = parseRows(hyper_data.c_str(), hyper_data.c_str() + hyper_data.length(), {""});
    std::cout<<"file has "<<general_rows.size()<<" general rows, "<<hyper_rows.size()<<" hyper rows"<<std::endl;

    EXPECT_EQ(general_rows.size(), hyper_rows.size());

    if(general_rows.size() == hyper_rows.size()) {
        // compare individual rows (after sorting them!)
        std::cout<<"sorting general rows..."<<std::endl;
        std::sort(general_rows.begin(), general_rows.end());
        std::cout<<"sorting hyper rows..."<<std::endl;
        std::sort(hyper_rows.begin(), hyper_rows.end());
        std::cout<<"comparing rows 1:1..."<<std::endl;

        // go through rows and compare them one-by-one
        for(unsigned i = 0; i < general_rows.size(); ++i) {
            //EXPECT_EQ(general_rows[i], hyper_rows[i]);
            if(!(general_rows[i] == hyper_rows[i])) {
               std::cout<<"row i="<<i<<" not equal"<<std::endl;
               std::cout<<"hyper:   "<<hyper_rows[i].toPythonString()<<std::endl;
               std::cout<<"general: "<<general_rows[i].toPythonString()<<std::endl;
               std::cout<<"----"<<std::endl;
            }
        }
    }
}

TEST(BasicInvocation, DetectNllAndZeroes) {
    using namespace tuplex;
    using namespace std;

    vector<Row> rows{Row(0.0), Row(0.0), Row(Field::null())};

    codegen::DetectionStats ds;
    ds.detect(rows);

    auto indices = ds.constant_column_indices();
    EXPECT_EQ(indices.size(), 0);
}

namespace tuplex {
    std::shared_ptr<LogicalOperator> sampleFiles(const std::string& pattern, const SamplingMode& mode) {
        python::initInterpreter();
        python::unlockGIL();
        // try {
        ContextOptions co = ContextOptions::defaults();
        auto enable_nvo = true; // test later with true! --> important for everything to work properly together!
        co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
        codegen::StageBuilderConfiguration conf(co);
        codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
        auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(pattern, co,
                                                                                   option<bool>(true),
                                                                                   option<char>(','), option<char>('"'),
                                                                                   {""}, {}, {}, {}, mode));
        python::lockGIL();
        python::closeInterpreter();
        return csvop;
    }
}

TEST(BasicInvocation, FileSampling) {
    using namespace std;
    using namespace tuplex;

    // for testing purposes, store here the root path to the flights data (simple ifdef)
#ifdef __APPLE__
    // leonhards macbook
    string flights_root = "/Users/leonhards/Downloads/flights/";
    //string flights_root = "/Users/leonhards/Downloads/flights_small/";
    string driver_memory = "2G";
#else
    // BBSN00
    string flights_root = "/hot/data/flights_all/";
    string driver_memory = "32G";
    string executor_memory = "10G";
    string num_executors = "0";
    //num_executors = "16";
#endif

    // find all flight files in root
    auto vfs = VirtualFileSystem::fromURI(URI(flights_root));
    auto paths = vfs.globAll(flights_root + "/flights_on_time*.csv");
    std::sort(paths.begin(), paths.end(), [](const URI& a, const URI& b) {
        auto a_str = a.toString();
        auto b_str = b.toString();
        return lexicographical_compare(a_str.begin(), a_str.end(), b_str.begin(), b_str.end());
    });
    cout<<"Found "<<paths.size()<<" CSV files.";
    cout<<basename(paths.front().toString())<<" ... "<<basename(paths.back().toString())<<endl;

    // sample all files & record time!
    Timer timer;
    auto op = sampleFiles(flights_root + "/flights_on_time*.csv", DEFAULT_SAMPLING_MODE);
    ASSERT_TRUE(op);
    auto sample = op->getSample();
    auto default_duration = timer.time();
    ASSERT_GT(default_duration, 0.0);


    size_t sample_size = 10000; // 10k samples.

    // no go through modes
    vector<pair<string, SamplingMode>> v{make_pair("first file", SamplingMode::FIRST_ROWS | SamplingMode::FIRST_FILE),
                                 make_pair("all files, first rows", SamplingMode::ALL_FILES | SamplingMode::FIRST_ROWS),
                                 make_pair("all files, first and last rows", SamplingMode::ALL_FILES | SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS),
                                 make_pair("all files, first/last/random", SamplingMode::ALL_FILES | SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::RANDOM_ROWS)};

    std::stringstream ss;
    ss<<"Sampling times:\n";
    for(const auto& t : v) {
        auto name = t.first;
        auto mode = t.second;
        timer.reset();
        op = sampleFiles(flights_root + "/flights_on_time*.csv", mode);
        ASSERT_TRUE(op);
        sample = op->getSample(sample_size);
        auto duration = timer.time();
        ss<<name<<": "<<duration<<"s\n";
        cout<<"done with mode "<<name<<" "<<pluralize(sample.size(), "row")<<endl;
    }
    cout<<endl;
    cout<<ss.str()<<endl;
}

TEST(BasicInvocation, TestAllFlightFiles) {
    using namespace std;
    using namespace tuplex;

    // get runtime lib path
    auto rtlib_path = ContextOptions::defaults().RUNTIME_LIBRARY().toString();

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"Starting exhaustive Flights hyperspecializaiton test:\n======="<<endl;
    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // for testing purposes, store here the root path to the flights data (simple ifdef)
#ifdef __APPLE__
    // leonhards macbook
    string flights_root = "/Users/leonhards/Downloads/flights/";
    //string flights_root = "/Users/leonhards/Downloads/flights_small/";
    string driver_memory = "2G";
#else
    // BBSN00
    string flights_root = "/hot/data/flights_all/";
    string driver_memory = "32G";
    string executor_memory = "10G";
    string num_executors = "0";
    //num_executors = "16";
#endif

    // --- use this for final PR ---
    // For testing purposes: resources/hyperspecialization/2003/*.csv holds two mini samples where wrong sampling triggers too many exceptions in general case mode
    string input_pattern = cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv," + cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_12.csv";
    // --- end use this for final PR ---

    // find all flight files in root
    vfs = VirtualFileSystem::fromURI(URI(flights_root));
    auto paths = vfs.globAll(flights_root + "/flights_on_time*.csv");
    std::sort(paths.begin(), paths.end(), [](const URI& a, const URI& b) {
        auto a_str = a.toString();
        auto b_str = b.toString();
       return lexicographical_compare(a_str.begin(), a_str.end(), b_str.begin(), b_str.end());
    });
    cout<<"Found "<<paths.size()<<" CSV files.";
    cout<<basename(paths.front().toString())<<" ... "<<basename(paths.back().toString())<<endl;

    // create the two pipeline versions...
        // !!! use compatible files for inference when issuing queries, else there'll be errors.

//    paths = {URI(flights_root + "flights_on_time_performance_2000_01.csv"), URI(flights_root + "flights_on_time_performance_2021_11.csv")};
    paths = {URI(flights_root + "flights_on_time_performance_2002_01.csv"), URI(flights_root + "flights_on_time_performance_2004_12.csv")};

    input_pattern = paths.front().toString() + "," + paths.back().toString();
    auto test_output_path = "./general_processing/";
    int num_threads = 1;
    auto spillURI = std::string("spill_folder");
    auto tstage_hyper = nullptr;//create_flights_pipeline(input_pattern, "./hyper_processing/", true);
    auto tstage_general = create_flights_pipeline(input_pattern, "./general_processing/", false, SamplingMode::FIRST_ROWS | SamplingMode::FIRST_FILE);

    // // test: 2013_03 fails -> fixed
    // 2010_01 fails

    // this file here fails completely => issue is the processing using opt[str] assumptions on the delay breakdown.
    //paths = {URI(flights_root + "/flights_on_time_performance_2021_11.csv")};
//
//    paths = {URI(cwd_path.string() + "/../resources/hyperspecialization/2021/flights_on_time_performance_2021_11.sample.csv")};
//    paths = {URI(flights_root + "flights_on_time_performance_2003_01.csv"), URI(flights_root + "flights_on_time_performance_2003_12.csv")};
    paths = {URI(flights_root + "flights_on_time_performance_2000_02.csv")};
    paths = {URI(flights_root + "flights_on_time_performance_2021_11.csv")};


    paths = {URI(flights_root + "flights_on_time_performance_2008_02.csv")}; // this seems to fail in python mode??

    paths = {URI(flights_root + "flights_on_time_performance_1997_04.csv")}; // this seems to fail in python mode??

    // @TODO: create from all files a diverse set!

    // this fails with hyper and 32KB sampling memory
    paths = {URI(flights_root + "flights_on_time_performance_2000_10.csv")}; // this seems to fail in python mode??

    paths = {URI(flights_root + "flights_on_time_performance_2004_12.csv")};

    std::reverse(paths.begin(), paths.end());

    for(const auto& path : paths) {
        Timer timer;
        cout<<"Testing "<<basename(path.toString())<<"...";
        cout.flush();
        int rc = checkHyperSpecialization(path, tstage_hyper, tstage_general, num_threads, spillURI);
        std::string hyper_ok = rc & 0x1 ? "OK" : "FAILED";
        std::string general_ok = rc & 0x2 ? "OK" : "FAILED";
        std::string validation_ok = rc & 0x4 ? "OK" : "FAILED";
        cout<<"  hyper: "<<hyper_ok<<" general: "<<general_ok<<" validation: "<<validation_ok<<" took: "<<timer.time()<<"s"<<endl;
    }

//    // !!! use compatible files for inference when issuing queries, else there'll be errors.
//    input_pattern = flights_root + "flights_on_time_performance_2003_01.csv," + flights_root + "flights_on_time_performance_2003_12.csv";
//
//
//    auto test_path = input_pattern;
//    auto test_output_path = "./general_processing/";
//    int num_threads = 1;
//    auto spillURI = std::string("spill_folder");
//    bool use_hyper = false;
//    auto tstage = create_flights_pipeline(test_path, test_output_path, use_hyper);
//
//    // transform to message
//    // create message only for first file!
////    auto input_uri = URI(cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_08.csv");
//    auto input_uri = URI(cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv");
//    input_uri = URI(flights_root + "flights_on_time_performance_2003_08.csv");
////    input_uri = URI(flights_root + "flights_on_time_performance_2003_01.csv");
//    auto output_uri = URI(test_output_path + (use_hyper ? string("output_hyper.csv") : string("output_general.csv")));
//
//    auto file = tuplex::VirtualFileSystem::open_file(output_uri, VirtualFileMode::VFS_OVERWRITE);
//    ASSERT_TRUE(file);
//    file.reset();
//
//    vfs = VirtualFileSystem::fromURI(input_uri);
//    uint64_t input_file_size = 0;
//    vfs.file_size(input_uri, input_file_size);
//    auto json_message = transformStageToReqMessage(tstage, input_uri.toPath(),
//                                                   input_file_size, output_uri.toString(),
//                                                   false,
//                                                   num_threads,
//                                                   spillURI);
//
//    // local WorkerApp
//    // start worker within same process to easier debug...
//    auto app = make_unique<WorkerApp>(WorkerSettings());
//    app->processJSONMessage(json_message);
//    app->shutdown();
//
//    // check size of ref file
//    uint64_t output_file_size = 0;
//    vfs.file_size("file://./general_processing/output_hyper.csv.csv", output_file_size);
//    cout<<"Output size of hyper file is: "<<output_file_size<<endl;
//    vfs.file_size("file://./general_processing/output_general.csv.csv", output_file_size);
//    cout<<"Output size of general file is: "<<output_file_size<<endl;
//
//    // Output size of file is: 46046908 (general, no hyper)
//    // 46046908

    cout<<"Test done."<<endl;
}

TEST(BasicInvocation, MicroConstantFoldedAggregation) {
    using namespace std;
    using namespace tuplex;

    std::cout<<"Micro Benchmark: Replacing hashmap aggregateByKey with direct aggregate for constant key:"<<std::endl;

    size_t N = 1000000;
    int64_t* data = new int64_t[N * 3];
    for(unsigned i = 0; i < N; ++i) {
        data[3 * i + 0] = 2012;
        data[3 * i + 1] = 06;
        data[3 * i + 2] = 1;
    }

    size_t N_runs = 10;
    std::cout<<"data generated."<<std::endl;
    cout<<"Benchmarking with N="<<N<<", runs="<<N_runs<<endl;
    std::vector<std::tuple<size_t, size_t, std::string, double>> timings;

    for(unsigned run = 0; run < N_runs; ++run) {
        Timer timer;
        int64_t count = 0;
        std::vector<std::tuple<int64_t, int64_t, int64_t>> bad_rows;

        // simple agg, time this here
        for(unsigned i = 0; i < N; ++i) {
            if(data[3 * i + 0] != 2012 || data[3 * i + 1] != 6)
                bad_rows.emplace_back(make_tuple(data[3 * i + 0], data[3 * i + 1], data[3 * i + 2]));
            else
                count += data[3 * i + 2];
        }

        // now compute over bad rows
        // (ignore for now, data matches...)

        double t = timer.time();
        timings.push_back(make_tuple(N, run, "constant-folded",t));
        std::cout<<"run: "<<run<<" specialized code took: "<<t<<"s res: "<<count<<" expected: "<<N<<std::endl;
    }


    for(unsigned run = 0; run < N_runs; ++run) {
        Timer timer;
        int64_t count = 0;
        auto hm = hashmap_new();

        // simple agg, time this here
        int64_t value;
        int64_t key[2];
        for(unsigned i = 0; i < N; ++i) {
            uint8_t *bucket = nullptr;
            key[0] = data[3 * i + 0];
            key[1] = data[3 * i + 1];
            hashmap_get(hm, reinterpret_cast<const char*>(key), 2 * sizeof(int64_t), (void **)(&bucket));
            if(!bucket) {
                assert(i == 0);
                bucket = new uint8_t[8];
                *(int64_t*)bucket = 0;
            }

            // update bucket with current value
            *(int64_t*)bucket += data[3 * i + 2];
            hashmap_put(hm, reinterpret_cast<const char*>(key), 2 * sizeof(int64_t), bucket);
        }

        // get count from hashmap
        uint8_t *bucket = nullptr;
        key[0] = 2012;
        key[1] = 6;
        hashmap_get(hm, reinterpret_cast<const char*>(key), 2 * sizeof(int64_t), (void **)(&bucket));
        assert(bucket);
        count = *(int64_t*)bucket;
        double t = timer.time();
        timings.push_back(make_tuple(N, run, "hashmap",t));
        std::cout<<"run: "<<run<<" with hashmap took: "<<t<<"s res: "<<count<<" expected: "<<N<<std::endl;
    }

    // print as CSV table
    std::stringstream ss;
    ss<<"N,run,mode,time_in_s\n";
    for(auto t : timings) {
        ss<<std::get<0>(t)<<","<<std::get<1>(t)<<","<<std::get<2>(t)<<","<<std::get<3>(t)<<"\n";
    }

    cout<<"CSV results:\n----\n"<<ss.str()<<endl;
}

TEST(BasicInvocation, FlightConstAggBenchmark) {
    // row['ARR_DELAY']
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    // TODO: next steps:
    // -> run over FULL 1987 file.
    // contrast timing for constant-keyed hashing vs. non-constant-keyed hashing for full 1987 file

    // check timing info for ALL files to get sense

    // expand pipeline, clean up.

    // put onto lambda.

    string input_pattern = "../resources/hyperspecialization/flights/flights_on_time_performance_1987_10.csv.sample";

    // a larger file
    input_pattern = "/hot/data/flights_all/flights_on_time_performance_2002_01.csv";

    // check the smallest and the largest file
    std::vector<std::string> paths({"/hot/data/flights_all/flights_on_time_performance_2020_06.csv",   // smallest file
                                    "/hot/data/flights_all/flights_on_time_performance_2019_07.csv"}); // largest file

    unsigned N_runs = 10;

    // std::vector<std::tuple<std::string, double, double>> timings;
    std::vector<std::tuple<std::string, std::string, unsigned, double>> timings;
    double cf_time = 0.0, nocf_time = 0.0;

    for(auto path : paths) {

        input_pattern = path;
        cout<<"Running benchmark (N_runs="<<N_runs<<") on "<<input_pattern<<endl;

        for(unsigned i = 0; i < N_runs; ++i) {
            // no constant folding for global version
            {
                ContextOptions co = ContextOptions::defaults();
                co.set("tuplex.executorCount", "0");
                co.set("tuplex.sample.maxDetectionRows", "20");
                // activate constant-folding for hashing optimization!
                co.set("tuplex.optimizer.constantFoldingOptimization", "false");
                Context ctx(co);

                //PerfEvent e;
                //e.startCounters();

                Timer timer;
                ctx.csv(input_pattern)
                        .selectColumns(std::vector<std::string>({"YEAR", "MONTH", "ARR_DELAY"}))
                        .aggregateByKey(UDF("lambda a, b: a + b"),
                                        UDF("lambda a, row: a + row['ARR_DELAY']"),
                                        Row(0), std::vector<std::string>({"YEAR", "MONTH"}))
                        .show();
                nocf_time = timer.time();

                //e.stopCounters();
                //e.printReport(std::cout, 1); // use n as scale factor
                std::cout<<"Run "<<(i+1)<<"/"<<N_runs<<" took "<<nocf_time<<"s (without constant-folding)"<<std::endl;
                timings.emplace_back(input_pattern, "without-constant-folding", i, nocf_time);
            }
        }
        // use https://github.com/viktorleis/perfevent


        for(unsigned i = 0; i < N_runs; ++i) {
            // single file w. constant-folding!
            {
                ContextOptions co = ContextOptions::defaults();
                co.set("tuplex.executorCount", "0");
                co.set("tuplex.sample.maxDetectionRows", "20");

                // activate constant-folding for hashing optimization!
                co.set("tuplex.optimizer.constantFoldingOptimization", "true");
                Context ctx(co);

                //PerfEvent e;
                //e.startCounters();


                Timer timer;
                ctx.csv(input_pattern)
                        .selectColumns(std::vector<std::string>({"YEAR", "MONTH", "ARR_DELAY"}))
                        .aggregateByKey(UDF("lambda a, b: a + b"),
                                        UDF("lambda a, row: a + row['ARR_DELAY']"),
                                        Row(0), std::vector<std::string>({"YEAR", "MONTH"}))
                        .show();
                cf_time = timer.time();

                //e.stopCounters();
                //e.printReport(std::cout, 1); // use n as scale factor
                std::cout<<"Run "<<(i+1)<<"/"<<N_runs<<" took "<<cf_time<<"s (with constant-folding)"<<std::endl;
                timings.emplace_back(input_pattern, "with-constant-folding", i, cf_time);
            }
        }
    }

    python::lockGIL();
    python::closeInterpreter();

    // print result table
    std::stringstream ss;
    ss<<"path,mode,run,time\n";
    for(auto t : timings) {
        ss<<std::get<0>(t)<<","<<std::get<1>(t)<<","<<std::get<2>(t)<<","<<std::get<3>(t)<<"\n";
    }
    std::cout<<"RESULTS:\n----\n";
    std::cout<<ss.str()<<std::endl;
}

TEST(BasicInvocation, FlightsSampling) {
    using namespace std;
    using namespace tuplex;

    // get runtime lib path
    auto rtlib_path = ContextOptions::defaults().RUNTIME_LIBRARY().toString();

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"Starting exhaustive Flights hyperspecializaiton test:\n======="<<endl;
    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // for testing purposes, store here the root path to the flights data (simple ifdef)
#ifdef __APPLE__
    // leonhards macbook
    string flights_root = "/Users/leonhards/Downloads/flights/";
    //string flights_root = "/Users/leonhards/Downloads/flights_small/";
    string driver_memory = "2G";
#else
    // BBSN00
    string flights_root = "/hot/data/flights_all/";
    string driver_memory = "32G";
    string executor_memory = "10G";
    string num_executors = "0";
    //num_executors = "16";
#endif

    // --- use this for final PR ---
    // For testing purposes: resources/hyperspecialization/2003/*.csv holds two mini samples where wrong sampling triggers too many exceptions in general case mode
    string input_pattern = cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv," + cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_12.csv";
    // --- end use this for final PR ---

    // find all flight files in root
    vfs = VirtualFileSystem::fromURI(URI(flights_root));
    auto paths = vfs.globAll(flights_root + "/flights_on_time*.csv");
    // input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002_01.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003_11.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004_12.csv'
    paths = vector<URI>{URI(flights_root + "/flights_on_time_performance_2002_01.csv"),
             URI(flights_root + "/flights_on_time_performance_2003_11.csv"),
             URI(flights_root + "/flights_on_time_performance_2004_12.csv")};

    std::sort(paths.begin(), paths.end(), [](const URI& a, const URI& b) {
        auto a_str = a.toString();
        auto b_str = b.toString();
        return lexicographical_compare(a_str.begin(), a_str.end(), b_str.begin(), b_str.end());
    });
    cout<<"Found "<<paths.size()<<" CSV files.";
    cout<<basename(paths.front().toString())<<" ... "<<basename(paths.back().toString())<<endl;

    // determine here which files to use for sampling
    //
    //paths = {URI(flights_root + "flights_on_time_performance_2002_01.csv"), URI(flights_root + "flights_on_time_performance_2004_12.csv")};

    input_pattern = paths.front().toString() + "," + paths[1].toString() + "," + paths.back().toString();
    auto test_output_path = "./general_processing/";
    int num_threads = 1;
    auto spillURI = std::string("spill_folder");

    // sm = tuplex.dataset.SamplingMode.ALL_FILES | tuplex.dataset.SamplingMode.FIRST_ROWS
    auto sampling_mode = SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE | SamplingMode::LAST_FILE; // //SamplingMode::ALL_FILES | SamplingMode::FIRST_ROWS;
    auto tstage_hyper = create_flights_pipeline(input_pattern, "./hyper_processing/", true);
    auto tstage_general = create_flights_pipeline(input_pattern, "./general_processing/", false, sampling_mode);

    // now specify which files to run on.
    //paths = {URI(flights_root + "/flights_on_time_performance_2003_11.csv")};
    //std::reverse(paths.begin(), paths.end());
    for(const auto& path : paths) {
        Timer timer;
        cout<<"Testing "<<basename(path.toString())<<"...";
        cout.flush();
        int rc = checkHyperSpecialization(path, tstage_hyper, tstage_general, num_threads, spillURI);
        std::string hyper_ok = rc & 0x1 ? "OK" : "FAILED";
        std::string general_ok = rc & 0x2 ? "OK" : "FAILED";
        std::string validation_ok = rc & 0x4 ? "OK" : "FAILED";
        cout<<"  hyper: "<<hyper_ok<<" general: "<<general_ok<<" validation: "<<validation_ok<<" took: "<<timer.time()<<"s"<<endl;
    }

    cout<<"Test done."<<endl;
}

/*!
 * this here is the local machine experiment for the workshop paper. Note that this is done via C++ and not properly as
 * python file.
 */
TEST(BasicInvocation, WorkshopPaperLocalExperiment) {
    using namespace std;
    using namespace tuplex;

    // get runtime lib path
    auto rtlib_path = ContextOptions::defaults().RUNTIME_LIBRARY().toString();

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"Starting worksgop Flights hyperspecializaiton test:\n======="<<endl;
    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // for testing purposes, store here the root path to the flights data (simple ifdef)
#ifdef __APPLE__
    // leonhards macbook
    string flights_root = "/Users/leonhards/Downloads/flights/";
    //string flights_root = "/Users/leonhards/Downloads/flights_small/";
    string driver_memory = "2G";
#else
    // BBSN00
    string flights_root = "/hot/data/flights_all/";
    string driver_memory = "32G";
    string executor_memory = "10G";
    string num_executors = "0";
    //num_executors = "16";
#endif

    cout<<"flight data root is: "<<flights_root<<endl;

    // --- use this for final PR ---
    // For testing purposes: resources/hyperspecialization/2003/*.csv holds two mini samples where wrong sampling triggers too many exceptions in general case mode
    string input_pattern = cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv," + cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_12.csv";
    // --- end use this for final PR ---

    input_pattern = flights_root + "/flights_on_time*.csv";

    // use local
    input_pattern = cwd_path.string() + "/../resources/hyperspecialization/flights_all/*.csv.sample";

    // find all flight files in root
    vfs = VirtualFileSystem::fromURI(URI(flights_root));
    auto paths = vfs.globAll(input_pattern);
    std::sort(paths.begin(), paths.end(), [](const URI& a, const URI& b) {
        auto a_str = a.toString();
        auto b_str = b.toString();
        return lexicographical_compare(a_str.begin(), a_str.end(), b_str.begin(), b_str.end());
    });
    cout<<"Found "<<paths.size()<<" CSV files.";
    ASSERT_NE(paths.size(), 0);
    cout<<basename(paths.front().toString())<<" ... "<<basename(paths.back().toString())<<endl;

    std::reverse(paths.begin(), paths.end());

    // experiment settings
    int num_threads = 1;

    // 1. starting general processing experiment (using worker!)
    {
        cout<<">>> Starting general processing experiment"<<endl;
        cout<<"  input pattern: "<<input_pattern<<endl;
        cout<<"  "<<pluralize(paths.size(), "path")<<endl;

        Timer timer;

        auto test_output_path = "./general_processing/";

        auto spillURI = URI(std::string("general_spill_folder"));
        auto tstage_general = create_flights_pipeline(input_pattern, "./general_processing/", false);

        auto app = make_unique<WorkerApp>(WorkerSettings());

        // paths = {URI(flights_root + "/flights_on_time_performance_2013_03.csv")};

        // local WorkerApp
        // start worker within same process to easier debug...

        std::cout<<" --- General processing --- "<<std::endl;
        for(const auto& input_uri : paths) {
            vfs = VirtualFileSystem::fromURI(input_uri);
            uint64_t input_file_size = 0;
            vfs.file_size(input_uri, input_file_size);

            auto output_uri = URI(tstage_general->outputURI().toString() + "/" + basename(input_uri.toString()));
            auto json_message_general = transformStageToReqMessage(tstage_general, input_uri.toPath(),
                                                                   input_file_size, output_uri.toString(),
                                                                   false,
                                                                   num_threads,
                                                                   spillURI.toString());
            app->processJSONMessage(json_message_general);
        }

        app->shutdown();

        cout<<"general took: "<<timer.time()<<"s"<<endl;
    }

    // 2. starting hyper-specialized processing experiment (using worker!)
    {
        cout<<">>> Starting hyper processing experiment"<<endl;
        cout<<"  input pattern: "<<input_pattern<<endl;
        cout<<"  "<<pluralize(paths.size(), "path")<<endl;

        Timer timer;

        auto test_output_path = "./hyper_processing/";

        auto spillURI = URI(std::string("general_spill_folder"));
        auto tstage_hyper = create_flights_pipeline(input_pattern, "./hyper_processing/", true);

        auto app = make_unique<WorkerApp>(WorkerSettings());

        // paths = {URI(flights_root + "/flights_on_time_performance_2013_03.csv")};

        // local WorkerApp
        // start worker within same process to easier debug...

        std::cout<<" --- Hyper processing --- "<<std::endl;
        for(const auto& input_uri : paths) {
            vfs = VirtualFileSystem::fromURI(input_uri);
            uint64_t input_file_size = 0;
            vfs.file_size(input_uri, input_file_size);

            auto output_uri = URI(tstage_hyper->outputURI().toString() + "/" + basename(input_uri.toString()));
            auto json_message_general = transformStageToReqMessage(tstage_hyper, input_uri.toPath(),
                                                                   input_file_size, output_uri.toString(),
                                                                   false,
                                                                   num_threads,
                                                                   spillURI.toString());
            app->processJSONMessage(json_message_general);
        }

        app->shutdown();

        cout<<"hyper took: "<<timer.time()<<"s"<<endl;
    }

    cout<<"Experiment done."<<endl;
}

// an experiment to check that verbose logging is enabled
TEST(BasicInvocation, VerboseLogging) {
    using namespace std;
    using namespace tuplex;

    auto worker_path = find_worker();

    auto testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    auto scratchDir = "/tmp/" + testName;

    // change cwd & create dir to avoid conflicts!
    auto cwd_path = boost::filesystem::current_path();
    auto desired_cwd = cwd_path.string() + "/tests/" + testName;
    // create dir if it doesn't exist
    auto vfs = VirtualFileSystem::fromURI("file://");
    vfs.create_dir(desired_cwd);
    boost::filesystem::current_path(desired_cwd);

    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    EXPECT_NE(std::string(cur_dir).find(testName), std::string::npos);

    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // test config here:
    // local csv test file!
    auto test_path = URI("file://../resources/flights_on_time_performance_2019_01.sample.csv");
    auto test_output_path = URI("file://output.txt");
    auto spillURI = std::string("spill_folder");

    // local for quicker dev
    test_path = URI("file:///Users/leonhards/data/flights/flights_on_time_performance_2009_01.csv");

    size_t num_threads = 4;

    // invoke helper with --help
    std::string work_dir = cur_dir;

//    // Step 1: create ref pipeline using direct Tuplex invocation
//    auto test_ref_path = testName + "_ref.csv";
//    createRefPipeline(test_path.toString(), test_ref_path, scratchDir);
//
//    // load ref file from parts!
//    auto ref_files = glob(current_working_directory() + "/" + testName + "_ref*part*csv");
//    std::sort(ref_files.begin(), ref_files.end());
//
//    // merge into single large string
    std::string ref_content = "";
//    for(unsigned i = 0; i < ref_files.size(); ++i) {
//        if(i > 0) {
//            auto file_content = fileToString(ref_files[i]);
//            // skip header for these parts
//            auto idx = file_content.find("\n");
//            ref_content += file_content.substr(idx + 1);
//        } else {
//            ref_content = fileToString(ref_files[0]);
//        }
//    }
//    stringToFile("ref_content.txt", ref_content);
    ref_content = fileToString("ref_content.txt");

    // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
    ASSERT_TRUE(test_path.exists());
    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();
    auto enable_nvo = false; // test later with true! --> important for everything to work properly together!
    co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
    co.set("tuplex.useInterpreterOnly", "true");
    codegen::StageBuilderConfiguration conf(co);
    codegen::StageBuilder builder(0, true, conf); //(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                                                               option<bool>(true),
                                                                               option<char>(','), option<char>('"'),
                                                                               {""}, {}, {}, {}, DEFAULT_SAMPLING_MODE));
    auto mapop = std::make_shared<MapOperator>(csvop, UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}"), csvop->columns());
    auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());
    builder.addFileInput(csvop);
    builder.addOperator(mapop);
    builder.addFileOutput(fop);

    auto tstage = builder.build();

    // transform to message
    vfs = VirtualFileSystem::fromURI(test_path);
    uint64_t input_file_size = 0;
    vfs.file_size(test_path, input_file_size);
    auto json_message = transformStageToReqMessage(tstage, URI(test_path).toPath(),
                                                   input_file_size, test_output_path.toString(),
                                                   true,
                                                   num_threads,
                                                   spillURI,
                                                   true);

    // save to file
    auto msg_file = URI("test_message.json");
    stringToFile(msg_file, json_message);

    python::lockGIL();
    python::closeInterpreter();


    //  auto log_level = Aws::Utils::Logging::LogLevel::Trace;
    //        log_level = Aws::Utils::Logging::LogLevel::Info;
    //        auto log_system = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("tuplex", log_level);
    //        Aws::Utils::Logging::InitializeAWSLogging(log_system);

    // invoke worker with a message as separate process.

    // invoke worker with that message
    Timer timer;
    auto cmd = worker_path + " -m " + msg_file.toPath();
    auto res_stdout = runCommand(cmd);
    auto worker_invocation_duration = timer.time();
    cout<<res_stdout<<endl;
    cout<<"Invoking worker took: "<<worker_invocation_duration<<"s"<<endl;

    // check result contents
    cout<<"Checking worker results..."<<endl;
    cout<<"Invoked worker check done."<<endl;
    cout<<"Test done."<<endl;
}
