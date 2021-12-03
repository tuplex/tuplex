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

    ASSERT_TRUE(fileExists(test_path.toPath()));

    ContextOptions co;
    codegen::StageBuilder builder(0, true, true, false, 0.9, true, true);
    auto csvop = FileInputOperator::fromCsv(test_path.toString(), co,
                                       option<bool>(true),
                                               option<char>(','), option<char>('"'),
            {}, {}, {}, {});
    builder.addFileInput(csvop);

}