#include <gtest/gtest.h>
#include <Context.h>
#include <DataSet.h>
#include <orc/OrcFile.hh>
#include <orc/ColumnPrinter.hh>
#include "../core/TestUtils.h"

void writeRowInput(const std::vector<tuplex::Row> &rows, const std::vector<std::string>& columnNames = {});
void writeCSVInput(const std::string &path);

class OrcTest : public PyTest {};

//TEST_F(OrcTest, WriteSpark) {
//    using namespace tuplex;
//    auto files = VirtualFileSystem::globAll("../resources/spark-csv/*.csv");
//    for (const auto& file : files) {
//        writeCSVInput(file.toPath());
//    }
//}
//
//TEST_F(OrcTest, WriteFlights) {
//    using namespace tuplex;
//    auto files = VirtualFileSystem::globAll("../resources/pipelines/flights/*.csv");
//    for (const auto& file : files) {
//        writeCSVInput(file.toPath());
//    }
//}

TEST(ORC, WriteZillow) {
using namespace tuplex;
auto files = VirtualFileSystem::globAll("../resources/pipelines/zillow/*.csv");
for (const auto& file : files) {
writeCSVInput(file.toPath());
}
}

TEST(ORC, WriteGtrace) {
using namespace tuplex;
auto files = VirtualFileSystem::globAll("../resources/pipelines/gtrace/*.csv");
for (const auto& file : files) {
writeCSVInput(file.toPath());
}
}

TEST(ORC, WriteWeblogs) {
using namespace tuplex;
auto files = VirtualFileSystem::globAll("../resources/pipelines/weblogs/*.csv");
for (const auto& file : files) {
writeCSVInput(file.toPath());
}
}

TEST(ORC, WriteComplex) {
using namespace tuplex;
auto rows = {
        Row(Tuple(List(1, 2, 3))),
        Row(Tuple(List(5)))
};
writeRowInput(rows);
}

TEST(ORC, WriteTuples) {
using namespace tuplex;
auto rows = {
        Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9))),
        Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9))),
        Row(Tuple(1, Tuple(2, 3)), Tuple(Tuple(4, 5), 6, 7, Tuple(8,9)))
};
writeRowInput(rows);
}

TEST(ORC, WriteDict) {
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
writeRowInput(rows);
}

TEST(ORC, WriteColumns) {
using namespace tuplex;
auto rows = {
        Row(1, true),
        Row(2, true),
        Row(3, true)
};
writeRowInput(rows, {"int", "bool"});
}

TEST(ORC, WriteList) {
using namespace tuplex;
auto rows = {
        Row(List(1,2,3), List(1.1), List("a", "b"), List(true, true)),
        Row(List(4, 5), List(2.2, 3.3), List("c", "d", "e"), List(false, false)),
        Row(List(1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1), List(1.1,1.1,1.1,1.1,1.1,1.1,1.1), List("a","a","a","a","a","a","a","a","a","a","a","a","a","a","a"), List(false,false,false,false,false,false,false,false,false,false,false)),
        Row(List(6), List(4.4, 5.5, 6.6), List("f"), List(false))
};
writeRowInput(rows);
}

TEST(ORC, WriteOptions) {
using namespace tuplex;
auto rows = {
        Row(option<int>(1), option<double>(1.1), option<std::string>("a"), option<bool>(false)),
        Row(option<int>::none, option<double>::none, option<std::string>::none, option<bool>::none),
        Row(option<int>(2), option<double>(2.2), option<std::string>("bb"), option<bool>(true)),
        Row(option<int>::none, option<double>::none, option<std::string>::none, option<bool>::none)
};
writeRowInput(rows);
}

TEST(ORC, WriteI64) {
using namespace tuplex;
auto rows = {
        Row(-1, -2, -3),
        Row(4, 5, 6),
        Row(7, 8, 9)
};
writeRowInput(rows);
}

TEST(ORC, WriteF64) {
using namespace tuplex;
auto rows = {
        Row(-1.1, -2.2, -3.3),
        Row(4.4, 5.5, 6.6),
        Row(7.7777, 8.8888, 9.9999)
};
writeRowInput(rows);
}

TEST(ORC, WriteBoolean) {
using namespace tuplex;
auto rows ={
        Row(true, true, true),
        Row(false, false, false),
        Row(true, false, true)
};
writeRowInput(rows);
}

TEST(ORC, WriteString) {
using namespace tuplex;
auto rows ={
        Row("a", "bb", "ccc"),
        Row("aa", "bbb", "c"),
        Row("aaa", "b", "cc"),
        Row("", "", "")
};
writeRowInput(rows);
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

std::string rowToORCString(const tuplex::Row &row, const std::vector<std::string>& columnNames = {}) {
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

void writeDataSet(tuplex::DataSet &ds) {
    using namespace tuplex;

    std::string fileInPattern("orc_write_test.orc");
    auto fileOutPattern = "orc_write_test.*.orc";

    // Remove any files that already exist
    auto vfs = VirtualFileSystem::fromURI(".");
    auto files = VirtualFileSystem::globAll(fileOutPattern);
    for (const auto& uri : files) {
        vfs.remove(uri);
    }

    ds.toorc(fileInPattern);
    auto rows = ds.collectAsVector();
    auto columnNames = ds.columns();

    auto outFiles = VirtualFileSystem::globAll(fileOutPattern);
    std::vector<std::string> results;
    for (const auto& fileName : outFiles) {
        auto filePtr = VirtualFileSystem::open_file(fileName, VirtualFileMode::VFS_READ);
        EXPECT_TRUE(filePtr != nullptr);

        using namespace orc;
        ORC_UNIQUE_PTR<InputStream> inStream = readLocalFile(fileName.toPath());
        ReaderOptions options;
        ORC_UNIQUE_PTR<Reader> reader = createReader(std::move(inStream), options);

        RowReaderOptions rowReaderOptions;
        ORC_UNIQUE_PTR<RowReader> rowReader = reader->createRowReader(rowReaderOptions);

        std::string line;
        ORC_UNIQUE_PTR<ColumnPrinter> printer = createColumnPrinter(line, &rowReader->getSelectedType());

        auto batch = rowReader->createRowBatch(rows.size());
        while (rowReader->next(*batch)) {
            printer->reset(*batch);
            for (uint64_t i = 0; i < batch->numElements; ++i) {
                line.clear();
                printer->printRow(i);
                results.push_back(line);
            }
        }
    }

    EXPECT_EQ(results.size(), rows.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rowToORCString(rows.at(i), columnNames), results.at(i));
    }
}

void writeCSVInput(const std::string &path) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto& ds = context.csv(path);
    writeDataSet(ds);
}

void writeRowInput(const std::vector<tuplex::Row> &rows, const std::vector<std::string>& columnNames) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    Context context(co);
    auto& ds = context.parallelize(rows);
    if (!columnNames.empty()) {
        ds.setColumns(columnNames);
    }
    writeDataSet(ds);
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
