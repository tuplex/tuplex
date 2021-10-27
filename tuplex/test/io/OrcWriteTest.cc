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
#include <DataSet.h>

#ifdef BUILD_WITH_ORC

#include <orc/OrcFile.hh>
#include <orc/ColumnPrinter.hh>
#include "../core/TestUtils.h"

class OrcWrite : public PyTest {
protected:

    std::string folderName;

    void SetUp() override {
        PyTest::SetUp();

        using namespace tuplex;
        auto vfs = VirtualFileSystem::fromURI(".");
        folderName = "OrcWrite" + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
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

void writeDataSet(const std::string& folderName, tuplex::DataSet &ds);

TEST_F(OrcWrite, Complex) {
using namespace tuplex;
auto rows = {
        Row(Tuple(List(1, 2, 3))),
        Row(Tuple(List(5)))
};

auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, Tuples) {
using namespace tuplex;
auto rows = {
        Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9))),
        Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9))),
        Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9)))
};

auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, Dict) {
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
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, Columns) {
using namespace tuplex;
auto rows = {
        Row(1, true),
        Row(2, true),
        Row(3, true)
};

auto opts = microTestOptions();
Context c(opts);
std::vector<std::string> columnNames({"int", "bool"});
auto &ds = c.parallelize(rows, columnNames);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, List) {
using namespace tuplex;
auto rows = {
        Row(List(1,2,3), List(1.1), List("a", "b"), List(true, true)),
        Row(List(4, 5), List(2.2, 3.3), List("c", "d", "e"), List(false, false)),
        Row(List(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1), List(1.1,1.1,1.1,1.1,1.1,1.1,1.1), List("a","a","a","a","a","a","a","a","a","a","a","a","a","a","a"), List(false,false,false,false,false,false,false,false,false,false,false)),
        Row(List(6), List(4.4, 5.5, 6.6), List("f"), List(false))
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, Options) {
using namespace tuplex;
auto rows = {
        Row(option<int>(1), option<double>(1.1), option<std::string>("a"), option<bool>(false)),
        Row(option<int>::none, option<double>::none, option<std::string>::none, option<bool>::none),
        Row(option<int>(2), option<double>(2.2), option<std::string>("bb"), option<bool>(true)),
        Row(option<int>::none, option<double>::none, option<std::string>::none, option<bool>::none)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, I64) {
using namespace tuplex;
auto rows = {
        Row(-1, -2, -3),
        Row(4, 5, 6),
        Row(7, 8, 9)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, F64) {
using namespace tuplex;
auto rows = {
        Row(-1.1, -2.2, -3.3),
        Row(4.4, 5.5, 6.6),
        Row(7.7777, 8.8888, 9.9999)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, Boolean) {
using namespace tuplex;
auto rows ={
        Row(true, true, true),
        Row(false, false, false),
        Row(true, false, true)
};
auto opts = microTestOptions();
Context c(opts);
auto &ds = c.parallelize(rows);
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, String) {
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
writeDataSet(folderName, ds);
}

TEST_F(OrcWrite, Gtrace) {
using namespace tuplex;
auto files = VirtualFileSystem::globAll("../resources/pipelines/gtrace/*.csv");

auto opts = ContextOptions::defaults();
Context c(opts);

for (const auto& file : files) {
auto &ds = c.csv(file.toPath());
writeDataSet(folderName, ds);
}
}

tuplex::Field keyToField(cJSON *entry, const python::Type& type);
tuplex::Field valueToField(cJSON *entry, const python::Type& type);
std::string fieldToORCString(const tuplex::Field &field, const python::Type& type);
std::string rowToORCString(const tuplex::Row &row, const std::vector<std::string>& columnNames = {});

void writeDataSet(const std::string& folderName, tuplex::DataSet &ds) {
    using namespace tuplex;

    auto fileInPattern = folderName + "/" + folderName + ".orc";
    auto fileOutPattern = folderName + "/" + folderName + ".*.orc";

    ds.toorc(fileInPattern);
    auto expectedOutput = ds.collect();
    auto numRows = expectedOutput->rowCount();
    auto columnNames = ds.columns();

    auto outFiles = VirtualFileSystem::globAll(fileOutPattern);

    std::vector<std::string> testOutput;
    testOutput.reserve(numRows);
    for (const auto &fileName : outFiles) {
        using namespace orc;
        ORC_UNIQUE_PTR<InputStream> inStream = readLocalFile(fileName.toPath());
        ReaderOptions options;
        ORC_UNIQUE_PTR<Reader> reader = createReader(std::move(inStream), options);

        RowReaderOptions rowReaderOptions;
        ORC_UNIQUE_PTR<RowReader> rowReader = reader->createRowReader(rowReaderOptions);

        std::string line;
        ORC_UNIQUE_PTR<ColumnPrinter> printer = createColumnPrinter(line, &rowReader->getSelectedType());

        auto batch = rowReader->createRowBatch(numRows);
        while (rowReader->next(*batch)) {
            printer->reset(*batch);
            for (uint64_t i = 0; i < batch->numElements; ++i) {
                line.clear();
                printer->printRow(i);
                testOutput.push_back(line);
            }
        }

    }

    ASSERT_EQ(testOutput.size(), numRows);
    for (int i = 0; i < numRows; ++i) {
        auto expectedRow = rowToORCString(expectedOutput->getNextRow(), columnNames);
        ASSERT_EQ(expectedRow, testOutput.at(i));
    }
}

TEST(ORC, RowToStr) {
using namespace tuplex;
std::vector<std::string> cols({"one", "two", "three"});

auto row1 = Row(1, 2, 3);
EXPECT_EQ(R"({"": 1, "": 2, "": 3})", rowToORCString(row1));
EXPECT_EQ(R"({"one": 1, "two": 2, "three": 3})", rowToORCString(row1, cols));

auto row2 = Row(1.1, 2.2, 3.3);
EXPECT_EQ(R"({"": 1.1, "": 2.2, "": 3.3})", rowToORCString(row2));
EXPECT_EQ(R"({"one": 1.1, "two": 2.2, "three": 3.3})", rowToORCString(row2, cols));

auto row3 = Row(true, true, false);
EXPECT_EQ(R"({"": true, "": true, "": false})", rowToORCString(row3));
EXPECT_EQ(R"({"one": true, "two": true, "three": false})", rowToORCString(row3, cols));

auto row4 = Row("abc", "de", "f");
EXPECT_EQ(R"({"": "abc", "": "de", "": "f"})", rowToORCString(row4));
EXPECT_EQ(R"({"one": "abc", "two": "de", "three": "f"})", rowToORCString(row4, cols));

auto row5 = Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9)), 10);
EXPECT_EQ(R"({"": {"": 1, "": {"": 2, "": 3}}, "": {"": {"": 4, "": 5}, "": 6, "": 7, "": {"": 8, "": 9}}, "": 10})", rowToORCString(row5));
EXPECT_EQ(R"({"one": {"": 1, "": {"": 2, "": 3}}, "two": {"": {"": 4, "": 5}, "": 6, "": 7, "": {"": 8, "": 9}}, "three": 10})", rowToORCString(row5, cols));

auto row6 = Row(List(1,2,3), List(1.1, 2.2, 3.3), List(true));
EXPECT_EQ(R"({"": [1, 2, 3], "": [1.1, 2.2, 3.3], "": [true]})", rowToORCString(row6));

std::string i64_to_f64 = std::string(R"({"1":1.1,"2":2.2,"3":3.3})");
std::string f64_to_i64 = std::string(R"({"1.1":1,"2.2":2,"3.3":3})");
std::string str_to_bool = std::string(R"({"a":true,"b":false,"c":true})");
std::string bool_to_str = std::string(R"({"true":"a","false":"b","true":"c"})");
auto row7 = Row(Field::from_str_data(i64_to_f64, python::Type::makeDictionaryType(python::Type::I64, python::Type::F64)),
                Field::from_str_data(f64_to_i64, python::Type::makeDictionaryType(python::Type::F64, python::Type::I64)),
                Field::from_str_data(str_to_bool, python::Type::makeDictionaryType(python::Type::STRING, python::Type::BOOLEAN)),
                Field::from_str_data(bool_to_str, python::Type::makeDictionaryType(python::Type::BOOLEAN, python::Type::STRING)));
EXPECT_EQ(R"({"": [{"key": 1, "value": 1.1}, {"key": 2, "value": 2.2}, {"key": 3, "value": 3.3}], "": [{"key": 1.1, "value": 1}, {"key": 2.2, "value": 2}, {"key": 3.3, "value": 3}], "": [{"key": "a", "value": true}, {"key": "b", "value": false}, {"key": "c", "value": true}], "": [{"key": true, "value": "a"}, {"key": false, "value": "b"}, {"key": true, "value": "c"}]})", rowToORCString(row7));

auto row8 = Row(1, option<int>::none, 2);
EXPECT_EQ(R"({"": 1, "": null, "": 2})", rowToORCString(row8));
}

tuplex::Field keyToField(cJSON *entry, const python::Type& type) {
    using namespace tuplex;
    std::string str(entry->string);
    if (type == python::Type::I64) {
        return Field((int64_t) std::stoi(str));
    } else if (type == python::Type::F64) {
        return Field((double) std::stod(str));
    } else if (type == python::Type::STRING) {
        return Field(str);
    } else {
        if (str == "true") {
            return Field(true);
        } else {
            return Field(false);
        }
    }
}

tuplex::Field valueToField(cJSON *entry, const python::Type& type) {
    using namespace tuplex;
    if (type == python::Type::I64) {
        return Field((int64_t) entry->valueint);
    } else if (type == python::Type::F64) {
        return Field(entry->valuedouble);
    } else if (type == python::Type::BOOLEAN) {
        return Field((bool) entry->valueint);
    } else {
        return Field(entry->valuestring);
    }
}

std::string fieldToORCString(const tuplex::Field &field, const python::Type& type) {
    using namespace tuplex;
    if (field.isNull()) {
        return "null";
    } else if (type == python::Type::I64) {
        return std::to_string(field.getInt());
    } else if (type == python::Type::STRING) {
        return "\"" + std::string(reinterpret_cast<char*>(field.getPtr())) + "\"";
    } else if (type == python::Type::BOOLEAN) {
        if (field.getInt()) {
            return "true";
        } else {
            return "false";
        }
    } else if (type == python::Type::F64) {
        char numBuffer[64];
        snprintf(numBuffer, sizeof(numBuffer), "%.14g", field.getDouble());
        return std::string(numBuffer);
    } else if (type.isTupleType()) {
        auto tuple = (Tuple *) field.getPtr();
        std::string str = "{";
        for (int i = 0; i < tuple->numElements(); ++i) {
            str += "\"\": ";
            str += fieldToORCString(tuple->getField(i), type.parameters().at(i));

            if (i != tuple->numElements() - 1) {
                str += ", ";
            }
        }
        str += "}";
        return str;
    } else if (type.isListType()) {
        auto list = (List *) field.getPtr();
        std::string str = "[";
        for (int i = 0; i < list->numElements(); ++i) {
            str += fieldToORCString(list->getField(i), type.elementType());

            if (i != list->numElements() - 1) {
                str += ", ";
            }
        }
        str += "]";
        return str;
    } else if (type.isOptionType()) {
        return fieldToORCString(field, type.elementType());
    } else if (type.isDictionaryType()) {
        std::string str = "[";
        auto dict = cJSON_Parse(reinterpret_cast<char *>(field.getPtr()));
        auto cur = dict->child;
        while (cur) {
            str += R"({"key": )";
            auto keyType = type.keyType();
            str += fieldToORCString(keyToField(cur, keyType), keyType);
            str += R"(, "value": )";
            auto valueType = type.valueType();
            str += fieldToORCString(valueToField(cur, valueType), valueType);
            str += "}";
            cur = cur->next;
            if (cur) {
                str += ", ";
            }
        }
        str += "]";
        return str;
    }
    return "";
}

std::string rowToORCString(const tuplex::Row &row, const std::vector<std::string>& columnNames) {
    auto columnTypes = row.getSchema().getRowType().parameters();
    std::string s = "{";
    for (int i = 0; i < row.getNumColumns(); ++i) {
        s += "\"";
        if (!columnNames.empty()) {
            s += columnNames.at(i);
        }
        s += "\": ";

        s += fieldToORCString(row.get(i), columnTypes.at(i));
        if (i != row.getNumColumns() - 1) {
            s += ", ";
        }
    }
    s += "}";
    return s;
}
#endif
