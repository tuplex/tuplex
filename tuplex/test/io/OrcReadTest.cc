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
#include <stdlib.h>
#include <sstream>

class OrcReadTest : public PyTest {};

void testReadInput(tuplex::Context& context, const std::vector<tuplex::Row> &rows);

std::string uniqueFileName() {
    using namespace tuplex;
    auto lookup = "abcdefghijklmnopqrstuvqxyz";
    auto len = strlen(lookup);
    std::stringstream ss;
    ss << lookup[rand() & len];
    while (fileExists(ss.str()) && ss.str().length() < 255) {
        ss << lookup[rand() % len];
    }
    auto fileName = ss.str();
    if (fileExists(fileName)) {
        throw std::runtime_error("could not create unique file name");
    }
    return fileName;
}

TEST_F(OrcReadTest, FileDoesNotExist) {
    using namespace tuplex;
    auto opts = microTestOptions();
    Context c(opts);

    auto rows = c.orc(uniqueFileName()).collectAsVector();
    EXPECT_EQ(rows.size(), 0);
}

TEST_F(OrcReadTest, ReadOption) {
    using namespace tuplex;
    auto rows = {
            Row(option<int>(1), option<double>(1.1), option<bool>(false)),
            Row(option<int>::none, option<double>::none, option<bool>::none),
            Row(option<int>(2), option<double>(2.2), option<bool>(true)),
            Row(option<int>::none, option<double>::none, option<bool>::none)
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadDict) {
    using namespace tuplex;
    std::string i64_to_f64 = std::string(R"({"1":1.1,"2":2.2,"3":3.3})");
    std::string f64_to_i64 = std::string(R"({"1.1":1,"2.2":2,"3.3":3})");
    std::string str_to_bool = std::string(R"({"a":true,"b":false,"c":true})");
    std::string bool_to_str = std::string(R"({"true":"a","false":"b","true":"c"})");
    auto rows = {
            Row(Field::from_str_data(i64_to_f64,
                                     python::Type::makeDictionaryType(python::Type::I64, python::Type::F64)),
                Field::from_str_data(f64_to_i64,
                                     python::Type::makeDictionaryType(python::Type::F64, python::Type::I64)),
                Field::from_str_data(str_to_bool,
                                     python::Type::makeDictionaryType(python::Type::STRING, python::Type::BOOLEAN)),
                Field::from_str_data(bool_to_str,
                                     python::Type::makeDictionaryType(python::Type::BOOLEAN, python::Type::STRING)))
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadList) {
    using namespace tuplex;
    auto rows = {
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false))
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadNestedTuples) {
    using namespace tuplex;
    auto rows = {
            Row(Tuple("a", Tuple(1, 2)), Tuple("b", Tuple(3, 4)), Tuple("c", Tuple(1, Tuple(2)))),
            Row(Tuple("a", Tuple(1, 2)), Tuple("b", Tuple(3, 4)), Tuple("c", Tuple(1, Tuple(2)))),
            Row(Tuple("a", Tuple(1, 2)), Tuple("b", Tuple(3, 4)), Tuple("c", Tuple(1, Tuple(2)))),
            Row(Tuple("a", Tuple(1, 2)), Tuple("b", Tuple(3, 4)), Tuple("c", Tuple(1, Tuple(2)))),
            Row(Tuple("a", Tuple(1, 2)), Tuple("b", Tuple(3, 4)), Tuple("c", Tuple(1, Tuple(2))))
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadTuples) {
    using namespace tuplex;
    auto rows = {
            Row(Tuple(1, 2, 3), Tuple(1.1, 2.2, 3.3), Tuple("a", "b", "c"), Tuple(true, true, false)),
            Row(Tuple(1, 2, 3), Tuple(1.1, 2.2, 3.3), Tuple("a", "b", "c"), Tuple(true, true, false)),
            Row(Tuple(1, 2, 3), Tuple(1.1, 2.2, 3.3), Tuple("a", "b", "c"), Tuple(true, true, false)),
            Row(Tuple(1, 2, 3), Tuple(1.1, 2.2, 3.3), Tuple("a", "b", "c"), Tuple(true, true, false)),
            Row(Tuple(1, 2, 3), Tuple(1.1, 2.2, 3.3), Tuple("a", "b", "c"), Tuple(true, true, false))
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadI64) {
    using namespace tuplex;
    auto rows = {
            Row(-1, -2, -3),
            Row(4, 5, 6),
            Row(7, 8, 9)
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadF64) {
    using namespace tuplex;
    auto rows = {
            Row(-1.1, -2.2, -3.3),
            Row(4.4, 5.5, 6.6),
            Row(7.7777, 8.8888, 9.9999)
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadBoolean) {
    using namespace tuplex;
    auto rows ={
            Row(true, true, true),
            Row(false, false, false),
            Row(true, false, true)
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadString) {
    using namespace tuplex;
    auto rows ={
            Row("a", "bb", "ccc"),
            Row("aa", "bbb", "c"),
            Row("aaa", "b", "cc"),
            Row("", "", "")
    };
    auto opts = microTestOptions();
    Context c(opts);
    testReadInput(c, rows);
}

TEST_F(OrcReadTest, ReadZillow) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto files = VirtualFileSystem::globAll("../resources/pipelines/zillow/*.csv");
    for (const auto& file : files) {
        testReadInput(context, context.csv(file.toPath()).collectAsVector());
    }
}

TEST_F(OrcReadTest, ReadGtrace) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto files = VirtualFileSystem::globAll("../resources/pipelines/gtrace/*.csv");
    for (const auto& file : files) {
        testReadInput(context, context.csv(file.toPath()).collectAsVector());
    }
}

TEST_F(OrcReadTest, ReadWeblogs) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto files = VirtualFileSystem::globAll("../resources/pipelines/weblogs/*.csv");
    for (const auto& file : files) {
        testReadInput(context, context.csv(file.toPath()).collectAsVector());
    }
}

void testReadInput(tuplex::Context &context, const std::vector<tuplex::Row> &rows) {
    using namespace tuplex;

    std::string folderName(::testing::UnitTest::GetInstance()->current_test_info()->name());

    auto vfs = VirtualFileSystem::fromURI(".");
    auto err = vfs.create_dir(folderName);
    ASSERT_TRUE(err == VirtualFileSystemStatus::VFS_OK || err == VirtualFileSystemStatus::VFS_FILEEXISTS);

    // Remove existing files
    auto files = VirtualFileSystem::globAll(folderName + "/*");
    for (const auto &uri : files) {
        vfs.remove(uri);
    }

    auto fileInPattern = folderName + "/" + "orc_read_test.orc";
    auto fileOutPattern = folderName + "/" + "orc_read_test.*.orc";

    context.parallelize(rows).toorc(fileInPattern);

    auto outFiles = VirtualFileSystem::globAll(fileOutPattern);
    std::vector<Row> testOutput;
    testOutput.reserve(rows.size());
    for (const auto &fileName : outFiles) {
        auto filePtr = VirtualFileSystem::open_file(fileName, VirtualFileMode::VFS_READ);
        ASSERT_TRUE(filePtr != nullptr);
        filePtr->close();

        auto orcRows = context.orc(fileName.toPath()).collectAsVector();
        testOutput.insert(testOutput.end(), orcRows.begin(), orcRows.end());
    }

    ASSERT_EQ(testOutput.size(), rows.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), testOutput.at(i).toPythonString());
    }
}