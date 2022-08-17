//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 10/18/2021                                                                        //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Context.h>
#include "../core/TestUtils.h"
#include "FileSystemUtils.h"
#include <VirtualFileSystem.h>

class FileOutputTest : public TuplexTest {
protected:
    std::string folderName;

    void SetUp() override {
        using namespace tuplex;
        TuplexTest::SetUp();
        auto vfs = VirtualFileSystem::fromURI(".");
        folderName = "FileOutput" + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        vfs.remove(folderName);
        auto err = vfs.create_dir(folderName);
        ASSERT_TRUE(err == VirtualFileSystemStatus::VFS_OK);
    }

    void TearDown() override {
        using namespace tuplex;
        auto vfs = VirtualFileSystem::fromURI(".");
        vfs.remove(folderName);
    }
};

TEST_F(FileOutputTest, NewFolder) {
    using namespace tuplex;

    auto opts = microTestOptions();
    Context c(opts);

    auto newFolder = uniqueFileName(folderName+"/");

    std::vector<Row> rows({Row(1), Row(2), Row(3)});
    c.parallelize(rows).tocsv(newFolder);

    auto outputRows = c.csv(newFolder + "/part0.csv").collectAsVector();
    ASSERT_EQ(rows.size(), outputRows.size());

    // debug print
    std::cout<<"file contents:\n";
    auto file_content = fileToString(newFolder + "/part0.csv");
    ASSERT_TRUE(!file_content.empty());
    std::cout<<file_content;
    std::cout<<"\n"<<std::endl;
    for(unsigned i = 0; i < rows.size(); ++i) {
        std::cout<<"row "<<i<<": "<<outputRows.at(i).getRowType().desc()<<" "<<outputRows.at(i).toPythonString()<<std::endl;
    }

    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), outputRows.at(i).toPythonString());
    }
}

TEST_F(FileOutputTest, EmptyFolder) {
    using namespace tuplex;

    auto opts = microTestOptions();
    Context c(opts);

    auto emptyFolder = uniqueFileName(folderName+"/");

    auto vfs = VirtualFileSystem::fromURI(URI("."));
    vfs.create_dir(URI(emptyFolder));

    std::vector<Row> rows({Row(1), Row(2), Row(3)});
    c.parallelize(rows).tocsv(emptyFolder);

    auto outputRows = c.csv(emptyFolder + "/part0.csv").collectAsVector();
    ASSERT_EQ(rows.size(), outputRows.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), outputRows.at(i).toPythonString());
    }
}


TEST_F(FileOutputTest, NonEmptyFolder) {
    using namespace tuplex;

    // deactivated, skip for now b.c. of deactivated output specification validation
    GTEST_SKIP_("deactivated because output file specification not yet perfect");

    auto opts = microTestOptions();
    Context c(opts);

    auto nonEmptyFolder = uniqueFileName(folderName+"/");

    auto vfs = VirtualFileSystem::fromURI(URI("."));
    vfs.create_dir(URI(nonEmptyFolder));
    vfs.create_dir(URI(nonEmptyFolder + "/subfolder"));

    std::vector<Row> rows({Row(1), Row(2), Row(3)});
    EXPECT_ANY_THROW(c.parallelize(rows).tocsv(nonEmptyFolder));
}

TEST(FileOutput, AbsoluteAndRelativePaths) {
    using namespace tuplex;

    // check conversion to local & absolute paths!
    URI local_rel("test.csv");
    EXPECT_EQ(local_rel.toPath(), current_working_directory() + "/test.csv");
}
