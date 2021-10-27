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

class OrcRead : public PyTest {
protected:

    std::string folderName;

    void SetUp() override {
        PyTest::SetUp();

        using namespace tuplex;
        auto vfs = VirtualFileSystem::fromURI(".");
        folderName = "OrcRead" + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        vfs.remove(folderName);
        auto err = vfs.create_dir(folderName);
        ASSERT_TRUE(err == VirtualFileSystemStatus::VFS_OK);
    }

    void TearDown() override {
        PyTest::TearDown();

        using namespace tuplex;
        auto vfs = VirtualFileSystem::fromURI(".");
        vfs.remove(folderName);
    }
};

void testReadInput(std::string folderName, tuplex::Context &context, tuplex::DataSet &ds);

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

TEST_F(OrcRead, FileDoesNotExist) {
using namespace tuplex;
auto opts = microTestOptions();
Context c(opts);

auto rows = c.orc(uniqueFileName()).collectAsVector();
EXPECT_EQ(rows.size(), 0);
}

TEST_F(OrcRead, Option) {
using namespace tuplex;
auto rows = {
        Row(option<int>(1), option<double>(1.1), option<bool>(false)),
        Row(option<int>::none, option<double>::none, option<bool>::none),
        Row(option<int>(2), option<double>(2.2), option<bool>(true)),
        Row(option<int>::none, option<double>::none, option<bool>::none)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);

testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, Dict) {
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
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, List) {
using namespace tuplex;
auto rows = {
        Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
        Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
        Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
        Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false))
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, NestedTuples) {
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
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, Tuples) {
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
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, I64) {
using namespace tuplex;
auto rows = {
        Row(-1, -2, -3),
        Row(4, 5, 6),
        Row(7, 8, 9)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, F64) {
using namespace tuplex;
auto rows = {
        Row(-1.1, -2.2, -3.3),
        Row(4.4, 5.5, 6.6),
        Row(7.7777, 8.8888, 9.9999)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, Boolean) {
using namespace tuplex;
auto rows ={
        Row(true, true, true),
        Row(false, false, false),
        Row(true, false, true)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, String) {
using namespace tuplex;
auto rows ={
        Row("a", "bb", "ccc"),
        Row("aa", "bbb", "c"),
        Row("aaa", "b", "cc"),
        Row("", "", "")
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
testReadInput(folderName, c, ds);
}

TEST_F(OrcRead, Gtrace) {
using namespace tuplex;
ContextOptions co = ContextOptions::defaults();
Context context(co);
auto files = VirtualFileSystem::globAll("../resources/pipelines/gtrace/*.csv");
for (const auto& file : files) {
auto &ds = context.csv(file.toPath());
testReadInput(folderName, context, ds);
}
}

void testReadInput(std::string folderName, tuplex::Context &context, tuplex::DataSet &ds) {
    using namespace tuplex;

    auto fileInPattern = folderName + "/" + folderName + ".orc";
    auto fileOutPattern = folderName + "/" + folderName + ".*.orc";

    ds.toorc(fileInPattern);
    auto expectedOutput = ds.collect();
    auto numRows = expectedOutput->rowCount();

    auto outFiles = VirtualFileSystem::globAll(fileOutPattern);
    std::vector<Row> testOutput;
    testOutput.reserve(numRows);
    for (const auto &fileName : outFiles) {
        auto orcRows = context.orc(fileName.toPath()).collectAsVector();
        testOutput.insert(testOutput.end(), orcRows.begin(), orcRows.end());
    }

    ASSERT_EQ(testOutput.size(), numRows);
    for (int i = 0; i < numRows; ++i) {
        EXPECT_EQ(expectedOutput->getNextRow().toPythonString(), testOutput.at(i).toPythonString());
    }
}