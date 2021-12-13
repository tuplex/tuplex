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
#include <physical/StageBuilder.h>
#include <logical/FileInputOperator.h>
#include <logical/FileOutputOperator.h>
#include <logical/MapOperator.h>
#include <google/protobuf/util/json_util.h>

static const std::string worker_path = "tuplex-worker";

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
    std::string transformStageToReqMessage(const TransformStage* tstage, const std::string& inputURI, const size_t& inputSize, const std::string& output_uri) {
        messages::InvocationRequest req;
        auto pb_stage = tstage->to_protobuf();

        pb_stage->set_bitcode(tstage->bitCode());
        req.set_allocated_stage(pb_stage.release());

        // add input file to request
        req.add_inputuris(inputURI);
        req.add_inputsizes(inputSize);

        // output uri of job? => final one? parts?
        req.set_outputuri(output_uri);

        // transfrom to json
        std::string json_buf;
        google::protobuf::util::MessageToJsonString(req, &json_buf);
        return json_buf;
    }
}


TEST(BasicInvocation, Worker) {
    using namespace std;
    char buf[4096];
    auto cur_dir = getcwd(buf, 4096);

    cout<<"current working dir: "<<buf<<endl;

    // check worker exists
    ASSERT_TRUE(tuplex::fileExists(worker_path));

    // invoke helper with --help
    std::string work_dir = cur_dir;

    tuplex::Timer timer;
    auto res_stdout = runCommand(work_dir + "/" + worker_path + " --help");
    auto worker_invocation_duration = timer.time();
    cout<<res_stdout<<endl;
    cout<<"Invoking worker took: "<<worker_invocation_duration<<"s"<<endl;


    // create a simple TransformStage reading in a file & saving it. Then, execute via Worker!
    using namespace tuplex;

    // local csv test file!
    auto test_path = URI("file://../resources/flights_on_time_performance_2019_01.sample.csv");
    auto test_output_path = URI("file://output.txt");
    ASSERT_TRUE(fileExists(test_path.toPath()));
    python::initInterpreter();
    python::unlockGIL();
    ContextOptions co = ContextOptions::defaults();
    codegen::StageBuilder builder(0, true, true, false, 0.9, true, true);
    auto csvop = FileInputOperator::fromCsv(test_path.toString(), co,
                                       option<bool>(true),
                                               option<char>(','), option<char>('"'),
            {}, {}, {}, {});
    auto mapop = new MapOperator(csvop, UDF("lambda x: {'airport': x['ORIGIN_AIRPORT_ID']}"), csvop->columns());
    auto fop = new FileOutputOperator(mapop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, {});
    builder.addFileInput(csvop);
    builder.addOperator(mapop);
    builder.addFileOutput(fop);

    auto tstage = builder.build();

    // transform to message
    auto vfs = VirtualFileSystem::fromURI("file://test_input.csv");
    uint64_t input_file_size = 0;
    vfs.file_size("test_input.csv", input_file_size);
    auto json_message = transformStageToReqMessage(tstage, "test_input.csv", input_file_size, "test_output.csv");
    python::lockGIL();
    python::closeInterpreter();

}