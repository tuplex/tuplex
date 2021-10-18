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

class OrcReadTest : public PyTest {};

void testReadInput(const std::vector<tuplex::Row> &rows);

TEST_F(OrcReadTest, ReadOption) {
    using namespace tuplex;
    auto rows = {
            Row(option<int>(1), option<double>(1.1), option<bool>(false)),
            Row(option<int>::none, option<double>::none, option<bool>::none),
            Row(option<int>(2), option<double>(2.2), option<bool>(true)),
            Row(option<int>::none, option<double>::none, option<bool>::none)
    };
    testReadInput(rows);
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
    testReadInput(rows);
}

TEST_F(OrcReadTest, ReadList) {
    using namespace tuplex;
    auto rows = {
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false)),
            Row(List(1, 2, 3), List(1.1, 2.2, 3.3), List("a", "b", "c"), List(true, true, false))
    };
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
    testReadInput(rows);
}

TEST_F(OrcReadTest, ReadI64) {
    using namespace tuplex;
    auto rows = {
            Row(-1, -2, -3),
            Row(4, 5, 6),
            Row(7, 8, 9)
    };
    testReadInput(rows);
}

TEST_F(OrcReadTest, ReadF64) {
    using namespace tuplex;
    auto rows = {
            Row(-1.1, -2.2, -3.3),
            Row(4.4, 5.5, 6.6),
            Row(7.7777, 8.8888, 9.9999)
    };
    testReadInput(rows);
}

TEST_F(OrcReadTest, ReadBoolean) {
    using namespace tuplex;
    auto rows ={
            Row(true, true, true),
            Row(false, false, false),
            Row(true, false, true)
    };
    testReadInput(rows);
}

TEST_F(OrcReadTest, ReadString) {
    using namespace tuplex;
    auto rows ={
            Row("a", "bb", "ccc"),
            Row("aa", "bbb", "c"),
            Row("aaa", "b", "cc"),
            Row("", "", "")
    };
    testReadInput(rows);
}

TEST_F(OrcReadTest, ReadZillow) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto files = VirtualFileSystem::globAll("../resources/pipelines/zillow/*.csv");
    for (const auto& file : files) {
        testReadInput(context.csv(file.toPath()).collectAsVector());
    }
}

TEST_F(OrcReadTest, ReadGtrace) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto files = VirtualFileSystem::globAll("../resources/pipelines/gtrace/*.csv");
    for (const auto& file : files) {
        testReadInput(context.csv(file.toPath()).collectAsVector());
    }
}

TEST_F(OrcReadTest, ReadWeblogs) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto files = VirtualFileSystem::globAll("../resources/pipelines/weblogs/*.csv");
    for (const auto& file : files) {
        testReadInput(context.csv(file.toPath()).collectAsVector());
    }
}

void testReadInput(const std::vector<tuplex::Row> &rows) {
    using namespace tuplex;

    const std::string& origFileName = "orc_read_test.orc";
    const std::string& outFileName = "orc_read_test.*.orc";

    auto vfs = VirtualFileSystem::fromURI(".");
    auto files = VirtualFileSystem::globAll(outFileName);
    for (const auto& uri : files) {
        vfs.remove(uri);
    }

    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    context.parallelize(rows).toorc(origFileName);

    auto outFiles = VirtualFileSystem::globAll(outFileName);
    std::vector<Row> results;
    for (const auto &fileName : outFiles) {
        auto filePtr = VirtualFileSystem::open_file(fileName, VirtualFileMode::VFS_READ);
        EXPECT_TRUE(filePtr != nullptr);
        auto orcRows = context.orc(fileName.toPath()).collectAsVector();
        results.insert(results.end(), orcRows.begin(), orcRows.end());
    }

    EXPECT_EQ(results.size(), rows.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), results.at(i).toPythonString());
    }
}