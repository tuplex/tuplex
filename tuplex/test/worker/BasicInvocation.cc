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
#include "../../worker/include/WorkerApp.h"
#include "../../worker/include/LambdaWorkerApp.h"

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


#include <procinfo.h>
#include <FileUtils.h>

// dummy so linking works
namespace tuplex {
    ContainerInfo getThisContainerInfo() {
        ContainerInfo info;
        return info;
    }
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
                                           size_t numThreads = 1, const std::string& spillURI="spill_folder") {
        messages::InvocationRequest req;

        size_t buf_spill_size = 512 * 1024; // for testing, set spill size to 512KB!

        // use higher buf, to avoid spilling
        buf_spill_size = 128 * 1024 * 1024; // 128MB


        req.set_type(messages::MessageType::MT_TRANSFORM);

        auto pb_stage = tstage->to_protobuf();

        pb_stage->set_bitcode(tstage->fastPathBitCode());
        req.set_allocated_stage(pb_stage.release());

        // add input file to request
        req.add_inputuris(inputURI);
        req.add_inputsizes(inputSize);

        // output uri of job? => final one? parts?
        req.set_baseoutputuri(output_uri);

        auto ws = std::make_unique<messages::WorkerSettings>();
        ws->set_numthreads(numThreads);
        ws->set_normalbuffersize(buf_spill_size);
        ws->set_exceptionbuffersize(buf_spill_size);
        ws->set_spillrooturi(spillURI);
        ws->set_useinterpreteronly(interpreterOnly);
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
        co.set("tuplex.csv.selectionPushdown", "false"); // same setting as below??
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
    codegen::StageBuilder builder(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                            option<bool>(true),
                                            option<char>(','), option<char>('"'),
                                            {""}, {}, {}, {}));
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
    codegen::StageBuilder builder(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                            option<bool>(true),
                                            option<char>(','), option<char>('"'),
                                            {""}, {}, {}, {}));
    auto mapop = std::make_shared<MapOperator>(csvop, UDF("lambda x: {'origin_airport_id': x['ORIGIN_AIRPORT_ID'], 'dest_airport_id':x['DEST_AIRPORT_ID']}"), csvop->columns());
    auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());
    builder.addFileInput(csvop);
    builder.addOperator(mapop);

    auto key_type = mapop->getOutputSchema().getRowType().parameters()[0];
    auto bucket_type = mapop->getOutputSchema().getRowType().parameters()[1];
    builder.addHashTableOutput(mapop->getOutputSchema(), true, false, {0}, key_type, bucket_type);


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
    co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
    codegen::StageBuilder builder(0, true, true, false, 0.9, true, enable_nvo, true, false);
    auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path.toString(), co,
                                       option<bool>(true),
                                               option<char>(','), option<char>('"'),
            {""}, {}, {}, {}));
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

TEST(BasicInvocation, SelfInvoke) {
    using namespace tuplex;

    // test selfInvoke function...

    auto cred = AWSCredentials::get();
    NetworkSettings ns;

    initAWS(cred, ns);

    auto ids = selfInvoke("tuplex-lambda-runner", 4, 1500, 75, cred, ns);

    shutdownAWS();
}

tuplex::TransformStage* create_flights_pipeline(const std::string& test_path, const std::string& test_output_path, bool build_for_hyper) {

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


        // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    // Note: This one is unoptimized, i.e. no projection pushdown, filter pushdown etc.
    TransformStage *tstage = nullptr;
    python::initInterpreter();
    python::unlockGIL();
    try {
        ContextOptions co = ContextOptions::defaults();
        auto enable_nvo = true; // test later with true! --> important for everything to work properly together!
        co.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", enable_nvo ? "true" : "false");
        codegen::StageBuilder builder(0, true, true, false, 0.9, true, enable_nvo, true, false);
        auto csvop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromCsv(test_path, co,
                                                                                   option<bool>(true),
                                                                                   option<char>(','), option<char>('"'),
                                                                                   {""}, {}, {}, {}));
        auto mapop = std::make_shared<MapOperator>(csvop, UDF(udf_code), csvop->columns());
        auto fop = std::make_shared<FileOutputOperator>(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());

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
    } catch(...) {
        std::cerr<<"Exception occurred! Failure"<<std::endl;
        return nullptr;
    }

    python::lockGIL();
    python::closeInterpreter();
    return tstage;
}

TEST(BasicInvocation, FlightsHyper) {
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

    // !!! use compatible files for inference when issuing queries, else there'll be errors.
    input_pattern = flights_root + "flights_on_time_performance_2003_01.csv," + flights_root + "flights_on_time_performance_2003_12.csv";


    auto test_path = input_pattern;
    auto test_output_path = "./general_processing/";
    int num_threads = 1;
    auto spillURI = std::string("spill_folder");
    bool use_hyper = true;
    auto tstage = create_flights_pipeline(test_path, test_output_path, use_hyper);


    // transform to message
    // create message only for first file!
//    auto input_uri = URI(cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_08.csv");
    auto input_uri = URI(cwd_path.string() + "/../resources/hyperspecialization/2003/flights_on_time_performance_2003_01.csv");
//    input_uri = URI(flights_root + "flights_on_time_performance_2003_08.csv");
    input_uri = URI(flights_root + "flights_on_time_performance_2003_01.csv");
    auto output_uri = URI(test_output_path + (use_hyper ? string("output_hyper.csv") : string("output_general.csv")));

    auto file = tuplex::VirtualFileSystem::open_file(output_uri, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);
    file.reset();

    vfs = VirtualFileSystem::fromURI(input_uri);
    uint64_t input_file_size = 0;
    vfs.file_size(input_uri, input_file_size);
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

    // check size of ref file
    uint64_t output_file_size = 0;
    vfs.file_size("file://./general_processing/output_hyper.csv.csv", output_file_size);
    cout<<"Output size of hyper file is: "<<output_file_size<<endl;
    vfs.file_size("file://./general_processing/output_general.csv.csv", output_file_size);
    cout<<"Output size of general file is: "<<output_file_size<<endl;

    // Output size of file is: 46046908 (general, no hyper)
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