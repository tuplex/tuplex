//
// Created by Leonhard Spiegelberg on 3/18/22.
//

// @March, you can test the JsonReader here

#include <gtest/gtest.h>
#include <physical/execution/JsonReader.h>
#include <physical/experimental/JsonHelper.h>

//struct HelperJsonStruct {
//        JsonPar
//};

// returns bytes read
int64_t dummy_read_functor(void* userData, const uint8_t* buf, int64_t buf_size, int64_t* normal_rows_out, int64_t* bad_rows_out, int8_t is_last_row) {
    using namespace tuplex;
    using namespace tuplex::codegen;
    size_t row_number = 0;

    int64_t normal_count = 0;
    int64_t bad_count = 0;

    auto j = JsonParser_init();
    if (!j)
        throw std::runtime_error("failed to initialize parser");
    JsonParser_open(j, reinterpret_cast<const char*>(buf), buf_size);
    while (JsonParser_hasNextRow(j)) {
        if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
            // BADPARSE_STRINGINPUT
            auto line = JsonParser_getMallocedRow(j);
            free(line);
            bad_count++;
        } else {
            normal_count++;
        }

        // line ok, now extract something from the object!
        // => basically need to traverse...
        auto doc = *j->it;

        // get type
        JsonItem *obj = nullptr;
        uint64_t rc = JsonParser_getObject(j, &obj);
        if (rc != 0)
            break; // --> don't forget to release stuff here!

        // release all allocated things
        JsonItem_Free(obj);

        row_number++;
        JsonParser_moveToNextRow(j);
    }

    // get parsed bytes
    auto truncated_bytes = JsonParser_TruncatedBytes(j);

    JsonParser_close(j);
    JsonParser_free(j);

    if(normal_rows_out)
        *normal_rows_out = normal_count;
    if(bad_rows_out)
        *bad_rows_out = bad_count;
    auto parsed_bytes = buf_size - truncated_bytes;
    std::cout << "Parsed " << pluralize(row_number, "row") << std::endl;
    return parsed_bytes;
}


TEST(JsonReader, Ranges) {
    using namespace tuplex;
    using namespace tuplex::codegen;
    using namespace std;

    string path = "../resources/hyperspecialization/github_daily/2021-10-15.json.sample";

    auto data_str = fileToString(path);

    ASSERT_FALSE(data_str.empty());

    auto buf = data_str.data();
    auto buf_size = data_str.size();

    // open up large test file parse in full.
    {
        auto partitionSize = 128 * 1024 * 1024;
        JsonReader reader(nullptr, dummy_read_functor, partitionSize);
        reader.read(path);
        auto row_count = reader.inputRowCount();
        EXPECT_EQ(row_count, 1200);
    }

    // use buffer smaller than file
    {
        auto partitionSize = 512 * 1024; // 512KB buffer
        JsonReader reader(nullptr, dummy_read_functor, partitionSize);
        reader.read(path);
        auto row_count = reader.inputRowCount();
        EXPECT_EQ(row_count, 1200);
    }

    // use ranges
    {
        std::cout<<"Range Test I::"<<std::endl;
        auto partitionSize = 512 * 1024; // 512KB buffer
        auto fileSize = buf_size + 1;

        // parse in 128KB increments
        auto delta = 128 * 1024; // delta smaller than partition buffer.
        unsigned pos = 0;
        unsigned total_row_count = 0;
        while(pos < fileSize) {

            JsonReader reader(nullptr, dummy_read_functor, partitionSize);
            std::cout<<"reading range: "<<pos<<" - "<<pos + delta<<std::endl;
            reader.setRange(pos, pos + delta);
            reader.read(path);
            auto row_count = reader.inputRowCount();
            pos += delta;
            total_row_count += row_count;
        }
        EXPECT_EQ(total_row_count, 1200);
    }

    // use ranges
    {
        std::cout<<"Range Test II::"<<std::endl;
        auto partitionSize = 256 * 1024; // 256KB buffer
        auto fileSize = buf_size + 1;

        // parse in increments
        auto delta = 301 * 1024; // delta bigger than partition buffer.
        unsigned pos = 0;
        unsigned total_row_count = 0;
        while(pos < fileSize) {

            JsonReader reader(nullptr, dummy_read_functor, partitionSize);
            std::cout<<"reading range: "<<pos<<" - "<<pos + delta<<std::endl;
            reader.setRange(pos, pos + delta);
            reader.read(path);
            auto row_count = reader.inputRowCount();
            pos += delta;
            total_row_count += row_count;
        }
        EXPECT_EQ(total_row_count, 1200);
    }

    // use ranges
    {
        std::cout<<"Range Test III::"<<std::endl;
        auto partitionSize = 256 * 1024; // 256KB buffer
        auto fileSize = buf_size + 1;

        // parse in increments
        auto delta = 1400 * 1024; // delta bigger than partition buffer.
        unsigned pos = 0;
        unsigned total_row_count = 0;
        while(pos < fileSize) {

            JsonReader reader(nullptr, dummy_read_functor, partitionSize);
            std::cout<<"reading range: "<<pos<<" - "<<pos + delta<<std::endl;
            reader.setRange(pos, pos + delta);
            reader.read(path);
            auto row_count = reader.inputRowCount();
            pos += delta;
            total_row_count += row_count;
        }
        EXPECT_EQ(total_row_count, 1200);
    }

}

////@March: you can test here
//TEST(JsonReader, Basic) {
//    using namespace tuplex;
//    using namespace std;
//
//    auto reader = make_unique<JsonReader>(nullptr, nullptr);
//    URI test_uri = "../resources/sample_file.json"; //@TODO: edit
//    reader->read(test_uri);
//
//    EXPECT_EQ(reader->inputRowCount(), 42); //@TODO: edit
//}
//
//
//TEST(JsonReader, Chunked) {
//    using namespace tuplex;
//    using namespace std;
//
//    auto reader = make_unique<JsonReader>(nullptr, nullptr);
//    auto test_uri = "../resources/sample_file.json"; //@TODO: edit
//    auto file_size = ...;
//
//    reader->setRange(0, file_size/2);
//    reader->read(test_uri);
//
//    reader->setRange(file_size/2, file_size);
//
//    EXPECT_EQ(reader->inputRowCount(), 42); //@TODO: edit
//}