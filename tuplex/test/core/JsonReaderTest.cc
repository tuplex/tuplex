//
// Created by Leonhard Spiegelberg on 3/18/22.
//

// @March, you can test the JsonReader here

#include <gtest/gtest.h>
#include <physical/JsonReader.h>

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