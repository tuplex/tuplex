//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include "physical/execution/csvmonkey.h"
#include <Context.h>
#include <CSVStatistic.h>

using namespace tuplex;

// header has one field missing...
// --> should not lead to crash!
TEST(CSVStats, IncompatibleHeader) {
    auto test_str = "a,b,c\n"
                    "1,2,3,FAST ETL!\n"
                    "4,5,6,FAST ETL!\n"
                    "7,8,9,FAST ETL!";

    ContextOptions co = ContextOptions::defaults();
    CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                         co.CSV_QUOTECHAR(), co.SAMPLE_MAX_DETECTION_MEMORY(),
                         co.SAMPLE_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD());

    csvstat.estimate(test_str, strlen(test_str));

    EXPECT_TRUE(csvstat.type() == python::Type::makeTupleType({python::Type::I64,
                                                               python::Type::I64,
                                                               python::Type::I64,
                                                               python::Type::STRING}));
}


// helper function to get file stats
CSVStatistic getFileCSVStat(const URI& uri, const std::vector<std::string>& null_values=std::vector<std::string>{}) {
    using namespace std;

    ContextOptions co = ContextOptions::defaults();

    // disable null inference
    CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                         co.CSV_QUOTECHAR(), co.SAMPLE_MAX_DETECTION_MEMORY(),
                         co.SAMPLE_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD(), null_values);

    auto vfs = VirtualFileSystem::fromURI(uri);
    Logger::instance().defaultLogger().info("testing on file " + uri.toString());
    auto vf = vfs.map_file(uri);
    if(!vf) {
        Logger::instance().defaultLogger().error("could not open file " + uri.toString());
        return csvstat;
    }
    size_t size = 0;
    vfs.file_size(uri, reinterpret_cast<uint64_t&>(size));
    size_t sampleSize = 0;
    // determine sample size
    if(size > co.SAMPLE_MAX_DETECTION_MEMORY())
        sampleSize = core::floorToMultiple(std::min(co.SAMPLE_MAX_DETECTION_MEMORY(), size), 16ul);
    else {
        sampleSize = core::ceilToMultiple(std::min(co.SAMPLE_MAX_DETECTION_MEMORY(), size), 16ul);
    }

    assert(sampleSize % 16 == 0); // needs to be divisible by 16...

    // copy out memory from file for analysis
    char* start = new char[sampleSize + 1];
    // memset last 16 bytes to 0
    assert(start + sampleSize - 16ul >= start);
    assert(sampleSize >= 16ul);
    std::memset(start + sampleSize - 16ul, 0, 16ul);

    std::memcpy(start, vf->getStartPtr(), sampleSize);
    auto end = start + sampleSize;
    start[sampleSize] = 0; // important!

    csvstat.estimate(start, std::min(sampleSize, size), option<bool>::none, option<char>::none, true);
    delete [] start;
    return csvstat;
}

TEST(CSVStats, FileInference) {
    // go over bunch of files and infer type
    // specify here uri of the file & its type

    auto car_type = python::Type::makeTupleType({python::Type::I64,
                                                 python::Type::STRING,
                                                 python::Type::STRING,
                                                 python::Type::STRING,
                                                 python::Type::STRING});
    std::vector<std::pair<URI, python::Type>> pairs = {{URI("../resources/spark-csv/bool.csv"),
                                                               python::Type::makeTupleType({python::Type::BOOLEAN})},
                                                       {URI("../resources/spark-csv/cars.csv"), car_type},
                                                       {URI("../resources/spark-csv/cars-alternative.csv"), car_type},
                                                       {URI("../resources/spark-csv/cars-blank-column-name.csv"), car_type},
                                                       {URI("../resources/spark-csv/cars-malformed.csv"), python::Type::makeTupleType({python::Type::I64,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING})},
                                                       {URI("../resources/spark-csv/cars-unbalanced-quotes.csv"), python::Type::makeTupleType({python::Type::STRING,
                                                                                                                                               python::Type::STRING,
                                                                                                                                               python::Type::STRING,
                                                                                                                                               python::Type::STRING,
                                                                                                                                               python::Type::STRING})},
                                                       {URI("../resources/spark-csv/cars-null.csv"), car_type},
                                                       {URI("../resources/spark-csv/comments.csv"), python::Type::makeTupleType({python::Type::I64,
                                                                                                                                 python::Type::I64,
                                                                                                                                 python::Type::I64,
                                                                                                                                 python::Type::I64,
                                                                                                                                 python::Type::F64,
                                                                                                                                 python::Type::STRING})}
                                                       };


    for(auto p : pairs) {
        auto uri = std::get<0>(p);
        auto expected_type = std::get<1>(p);

        auto csvstat = getFileCSVStat(uri);

        // check type
        Logger::instance().defaultLogger().info("type of " + uri.toPath() + " is: " + csvstat.type().desc());
        Logger::instance().defaultLogger().info("expected type is: " + expected_type.desc());
        EXPECT_TRUE(csvstat.type() == expected_type);
    }
}


TEST(CSVStats, RareValueManyNulls) {
    using namespace std;

    string path = "CSVStats.RareValueManyNulls.csv";

    FILE *fp = fopen(path.c_str(), "w");

    for(int i = 0; i < 10; ++i)
        fprintf(fp, ",\n");
    fprintf(fp, "1.0,\n");
    for(int i = 0; i < 10; ++i)
        fprintf(fp, ",\n");

    fclose(fp);


    auto csvstat = getFileCSVStat(path, {"", "NULL"});
    auto expected_type = python::Type::makeTupleType({python::Type::makeOptionType(python::Type::F64), python::Type::makeOptionType(python::Type::STRING)});

    // check type
    Logger::instance().defaultLogger().info("type is: " + csvstat.type().desc());
    EXPECT_TRUE(csvstat.type() == expected_type);
}


TEST(CSVStats, FlightDetect) {
    using namespace tuplex;
    using namespace std;

    string f_path = "../resources/flights_on_time_performance_2019_01.sample.csv";

    auto csvstat = getFileCSVStat(f_path, {"", "NULL"});

    std::cout<<"specialized type: "<<csvstat.type().desc()<<std::endl;
    std::cout<<"general type: "<<csvstat.superType().desc()<<std::endl;

    // get small cell candidates...
    std::cout<<csvstat.smallCellCandidates().size()<<"/110 cells are candidates for delayed parsing optimization: "<<std::endl;
    // most, i.e. 102/110 of all flights cells are actually candidates for delayed parsing.
    // Yet, is the optimization worth it according to the pipeline?
    std::cout<<csvstat.smallCellCandidates()<<std::endl;

    EXPECT_NE(csvstat.smallCellCandidates().size(), csvstat.columns().size());
}

namespace tuplex {

class FixedBufferCursor : public csvmonkey::StreamCursor {
private:
    char *_buf;
    char *_ptr;
    char *_end_ptr;
public:
    ~FixedBufferCursor() {
        if(_buf)
            delete [] _buf;
        _buf = nullptr;
        _end_ptr = nullptr;
    }

    FixedBufferCursor(const char* buf, size_t size) : _buf(nullptr), _end_ptr(nullptr) {
        auto safe_size = size + 32;
        _buf = new char[safe_size];
        memcpy(_buf, buf, size);
        _end_ptr = _buf + size;
        _ptr = _buf;
    }

    const char* buf() { return _ptr; }

    size_t size() { return _end_ptr - _ptr; }

    void consume(size_t n) { _ptr += std::min(n, (size_t)(_end_ptr - _ptr)); }

    bool fill() { return false; }

};


    std::vector<Row> csv_parseRowsNEW(const char* buf, size_t buf_size, size_t expected_column_count, size_t range_start,
                                   char delimiter, char quotechar, const std::vector<std::string>& null_values, size_t limit) {
        auto sample_length = std::min(buf_size - 1, strlen(buf));
        auto end = buf + sample_length;

        size_t start_offset = 0;
        // range start != 0?
        if(0 != range_start) {
            start_offset = csvFindLineStart(buf, sample_length, expected_column_count, delimiter, quotechar);
            if(start_offset < 0)
                return {};
            sample_length -= std::min((size_t)start_offset, sample_length);
        }

        // parse as rows using the settings detected.
        std::vector<Row> v;
        ExceptionCode ec;
        std::vector<std::string> cells;
        size_t num_bytes = 0;
        const char* p = buf + start_offset;

        // use csvmonkey parser
        FixedBufferCursor cursor(p, end - p);
        csvmonkey::CsvReader<> reader(cursor, delimiter, quotechar);
        auto &row = reader.row();
        unsigned row_count = 0;

        // quickly create null hashmap & bool hashmap
        std::unordered_map<std::string, int8_t> value_map;
        // use 0/1 for bool, -1 for null-value.
        for(auto s : booleanTrueStrings())
            value_map[s] = 1;
        for(auto s : booleanFalseStrings())
            value_map[s] = 0;
        for(auto s : null_values)
            value_map[s] = -1;

        v.reserve(100);

        std::vector<Field> fields;
        fields.reserve(expected_column_count);
        while(reader.read_row()) {
            if(row.count != expected_column_count)
                continue;
            fields.clear();

            for(unsigned i = 0; i < expected_column_count; ++i) {
                auto cell = row.cells[i].as_str();
                auto it = value_map.find(cell);
                if(it != value_map.end()) {
                    if(it->second < 0)
                        fields.push_back(Field::null());
                    else
                        fields.push_back(Field(static_cast<bool>(it->second)));
                } else {
                    // int or float?
                    int64_t i = 0;
                    double d = 0;
                    if(0 == tuplex::fast_atoi64(cell.c_str(), cell.c_str() + cell.size(), &i)) {
                        fields.push_back(Field(i));
                    } else if(0 == tuplex::fast_atod(cell.c_str(), cell.c_str() + cell.size(), &d)) {
                        fields.push_back(Field(d));
                    } else {
                        fields.push_back(Field(cell));
                    }
                }
            }
            v.push_back(Row::from_vector(fields));
            row_count++;
        }
        std::cout<<"read: "<<row_count<<" rows"<<std::endl;


//        while(p < end && (ec = parseRow(p, end, cells, num_bytes, delimiter, quotechar, false)) == ExceptionCode::SUCCESS) {
//            // convert cells to row
//            if(cells.size() >= expected_column_count) {
//                auto row = cellsToRow(cells, null_values);
//                v.push_back(row);
//            }
//            cells.clear();
//            p += num_bytes;
//            if(v.size() >= limit)
//                break;
//        }
        return v;
    }

    std::vector<Row> csv_parseRowsOLD(const char* buf, size_t buf_size, size_t expected_column_count, size_t range_start,
                                      char delimiter, char quotechar, const std::vector<std::string>& null_values, size_t limit) {
        auto sample_length = std::min(buf_size - 1, strlen(buf));
        auto end = buf + sample_length;

        size_t start_offset = 0;
        // range start != 0?
        if(0 != range_start) {
            start_offset = csvFindLineStart(buf, sample_length, expected_column_count, delimiter, quotechar);
            if(start_offset < 0)
                return {};
            sample_length -= std::min((size_t)start_offset, sample_length);
        }

        // parse as rows using the settings detected.
        std::vector<Row> v;
        ExceptionCode ec;
        std::vector<std::string> cells;
        size_t num_bytes = 0;
        const char* p = buf + start_offset;

        while(p < end && (ec = parseRow(p, end, cells, num_bytes, delimiter, quotechar, false)) == ExceptionCode::SUCCESS) {
            // convert cells to row
            if(cells.size() >= expected_column_count) {
                auto row = cellsToRow(cells, null_values);
                v.push_back(row);
            }
            cells.clear();
            p += num_bytes;
            if(v.size() >= limit)
                break;
        }
        return v;
    }
}


TEST(CSVStats, CSVParseRows) {
    using namespace tuplex;
    using namespace std;

    string f_path = "../resources/hyperspecialization/flights_all/flights_on_time_performance_1987_10.csv.sample";

    auto data_str = fileToString(f_path);
    double old_time = 0.0;
    double new_time = 0.0;
    {
        std::cout<<"OLD parse mode:"<<std::endl;
        Timer timer;
        auto rows = csv_parseRowsOLD(data_str.c_str(), data_str.size(), 110, 0,
                                  ',', '"', std::vector<std::string>{"", "NULL"}, std::numeric_limits<size_t>::max());
        old_time = timer.time();
        std::cout<<"time to parse "<<pluralize(rows.size(), "row")<<": "<<timer.time()<<std::endl;
        std::cout<<"time per row: "<<timer.time() / (1.0 * rows.size()) * 1000.0<<"ms"<<std::endl;
    }
    {
        std::cout<<"NEW parse mode:"<<std::endl;
        Timer timer;
        auto rows = csv_parseRowsNEW(data_str.c_str(), data_str.size(), 110, 0,
                                     ',', '"', std::vector<std::string>{"", "NULL"}, std::numeric_limits<size_t>::max());
        new_time = timer.time();
        std::cout<<"time to parse "<<pluralize(rows.size(), "row")<<": "<<timer.time()<<std::endl;
        std::cout<<"time per row: "<<timer.time() / (1.0 * rows.size()) * 1000.0<<"ms"<<std::endl;
    }

    std::cout<<"NEW over old speedup factor: "<<old_time / new_time<<"x"<<std::endl;
}