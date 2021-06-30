//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <RuntimeInterface.h>
#include "TestUtils.h"

// test that i64, f64, bool are properly serialized & deserialized
TEST(CodegenSerialization, FixedLenPrimitiveTypes) {
    using namespace tuplex;
    Row res;

    // simple fields
    res = execRow(Row({Field((int64_t) 10)}));
    EXPECT_EQ(res.getInt(0), 10);

    res = execRow(Row({Field(10.78)}));
    EXPECT_EQ(res.getDouble(0), 10.78);

    res = execRow(Row({Field(false)}));
    EXPECT_EQ(res.getBoolean(0), false);
}

TEST(CodegenSerialization, VarLenPrimitiveTypes) {
    using namespace tuplex;
    Row res;

    res = execRow(Row({Field("hello world")}));
    EXPECT_EQ(res.getString(0), "hello world");

    auto dict_str = std::string("{\"sshello\":\"world\"}");
    res = execRow(Row({Field::from_str_data(dict_str, python::Type::makeDictionaryType(python::Type::STRING,
                                                                                       python::Type::STRING))}));
    EXPECT_EQ(res.getString(0), dict_str);
}

TEST(CodegenSerialization, ListsI) {
    using namespace tuplex;
    Row res;

    res = execRow(Row({List()}));
    EXPECT_EQ(res.toPythonString(), "([],)");

    res = execRow(Row({List(1, 2, 3)}));
    EXPECT_EQ(res.toPythonString(), "([1,2,3],)");

    res = execRow(Row({List(1.1, 2.2, 3.3)}));
    EXPECT_EQ(res.toPythonString(), "([1.10000,2.20000,3.30000],)");

    res = execRow(Row({List(true, false, true)}));
    EXPECT_EQ(res.toPythonString(), "([True,False,True],)");

    res = execRow(Row({List("hello", "world", "!")}));
    EXPECT_EQ(res.toPythonString(), "(['hello','world','!'],)");

    res = execRow(Row({List(Field::null(), Field::null(), Field::null())}));
    EXPECT_EQ(res.toPythonString(), "([None,None,None],)");

    res = execRow(Row({List(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple())}));
    EXPECT_EQ(res.toPythonString(), "([(),(),()],)");

    res = execRow(Row({List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict())}));
    EXPECT_EQ(res.toPythonString(), "([{},{},{}],)");

    res = execRow(Row({List(Field::empty_list(), Field::empty_list(), Field::empty_list())}));
    EXPECT_EQ(res.toPythonString(), "([[],[],[]],)");
}

TEST(CodegenSerialization, ListsII) {
    using namespace tuplex;
    Row res;

    res = execRow(Row({List(), List(), List()}));
    EXPECT_EQ(res.toPythonString(), "([],[],[])");

    res = execRow(Row({List(1, 2, 3), List(4, 5, 6), List(7, 8, 9)}));
    EXPECT_EQ(res.toPythonString(), "([1,2,3],[4,5,6],[7,8,9])");

    res = execRow(Row({List(1.1, 2.2, 3.3), List(4.4, 5.5, 6.6), List(7.7, 8.8, 9.9)}));
    EXPECT_EQ(res.toPythonString(), "([1.10000,2.20000,3.30000],[4.40000,5.50000,6.60000],[7.70000,8.80000,9.90000])");

    res = execRow(Row({List(true, false, true), List(false, true, true), List(false, false, false)}));
    EXPECT_EQ(res.toPythonString(), "([True,False,True],[False,True,True],[False,False,False])");

    res = execRow(Row({List("hello", "world", "!"), List("this", "is", "list"), List("", "empty", "")}));
    EXPECT_EQ(res.toPythonString(), "(['hello','world','!'],['this','is','list'],['','empty',''])");

    res = execRow(Row({List(Field::null(), Field::null(), Field::null()), List(Field::null(), Field::null(), Field::null()), List(Field::null(), Field::null(), Field::null())}));
    EXPECT_EQ(res.toPythonString(), "([None,None,None],[None,None,None],[None,None,None])");

    res = execRow(Row({List(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple()), List(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple()), List(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple())}));
    EXPECT_EQ(res.toPythonString(), "([(),(),()],[(),(),()],[(),(),()])");

    res = execRow(Row({List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict()), List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict()), List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict())}));
    EXPECT_EQ(res.toPythonString(), "([{},{},{}],[{},{},{}],[{},{},{}])");

    res = execRow(Row({List(Field::empty_list(), Field::empty_list(), Field::empty_list()), List(Field::empty_list(), Field::empty_list(), Field::empty_list()), List(Field::empty_list(), Field::empty_list(), Field::empty_list())}));
    EXPECT_EQ(res.toPythonString(), "([[],[],[]],[[],[],[]],[[],[],[]])");
}

TEST(CodegenSerialization, ListsIII) {
    using namespace tuplex;
    Row res;

    res = execRow(Row({List(1, 2, 3, 4, 5, 6), List(7, 8), List(9), List(10, 11)}));
    EXPECT_EQ(res.toPythonString(), "([1,2,3,4,5,6],[7,8],[9],[10,11])");

    res = execRow(Row({List(1.1), List(2.2, 3.3, 4.4, 5.5), List(6.6, 7.7), List(8.8,9.9)}));
    EXPECT_EQ(res.toPythonString(), "([1.10000],[2.20000,3.30000,4.40000,5.50000],[6.60000,7.70000],[8.80000,9.90000])");

    res = execRow(Row({List(true, false, true, false), List(true, true, true), List(false, false)}));
    EXPECT_EQ(res.toPythonString(), "([True,False,True,False],[True,True,True],[False,False])");

    res = execRow(Row({List("hello", "world", "!", "", "list"), List("abc"), List("", "", "")}));
    EXPECT_EQ(res.toPythonString(), "(['hello','world','!','','list'],['abc'],['','',''])");

    res = execRow(Row({List(Field::null(), Field::null(), Field::null(), Field::null()), List(Field::null(), Field::null()), List(Field::null(), Field::null())}));
    EXPECT_EQ(res.toPythonString(), "([None,None,None,None],[None,None],[None,None])");

    res = execRow(Row({List(Field::empty_tuple()), List(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple()), List(Field::empty_tuple(), Field::empty_tuple())}));
    EXPECT_EQ(res.toPythonString(), "([()],[(),(),(),(),()],[(),()])");

    res = execRow(Row({List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict()), List(Field::empty_dict()), List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict(), Field::empty_dict(), Field::empty_dict())}));
    EXPECT_EQ(res.toPythonString(), "([{},{},{}],[{}],[{},{},{},{},{}])");

    res = execRow(Row({List(Field::empty_list(), Field::empty_list(), Field::empty_list(), Field::empty_list()), List(Field::empty_list()), List(Field::empty_list(), Field::empty_list())}));
    EXPECT_EQ(res.toPythonString(), "([[],[],[],[]],[[]],[[],[]])");
}


TEST(CodegenSerialization, NullableTypes) {
    using namespace tuplex;

    Row input = Row({Field("test"), Field(option<int64_t>(42)), Field(option<std::string>::none)});

    auto res = execRow(input);

    EXPECT_EQ(res.toPythonString(), "('test',42,None)");
}

TEST(CodegenSerialization, SingleElementsNull_And_NonNull) {
    using namespace tuplex;

    // TODO: write tests for options...

    // primitive types
    Row r1(true);                   // boolean
    Row r2(14);                     // int64_t
    Row r3(3.141);                  // double
    Row r4("hello world");          // string
    Row r5(Field::null());          // NULL/None
    Row r6(Field::empty_tuple());   // empty tuple
    Row r7(Field::empty_dict());    // empty dictionary
    // TODO: empty list is missing...


    // (1) primitives
    EXPECT_EQ(execRow(r1).toPythonString(), "(True,)");
    EXPECT_EQ(execRow(r2).toPythonString(), "(14,)");
    EXPECT_EQ(execRow(r3).toPythonString(), "(3.14100,)"); // note rounding is here an issue when printing strings
    EXPECT_EQ(execRow(r4).toPythonString(), "('hello world',)");

    EXPECT_EQ(execRow(r5).toPythonString(), "(None,)");
    EXPECT_EQ(execRow(r6).toPythonString(), "((),)");
    EXPECT_EQ(execRow(r7).toPythonString(), "({},)");

    // nested types or compound types, i.e. dicts/tuples/lists & Co
    // rows with multiple columns
    auto optional_emptytuple_field = Field::empty_tuple();
    optional_emptytuple_field.makeOptional();
    auto optional_emptydict_field = Field::empty_dict();
    optional_emptydict_field.makeOptional();

    Row r8(true, false, option<std::string>(std::string("hello world")),
            option<int64_t>::none, Field::empty_tuple(), Field::empty_dict());
    Row r9(true, false, option<std::string>(std::string("hello world")),
            option<int64_t>::none, optional_emptytuple_field, Field::empty_dict());
    Row r10(true, false, option<std::string>(std::string("hello world")),
           option<int64_t>::none, optional_emptytuple_field, optional_emptydict_field);

    ASSERT_EQ(execRow(r8).toPythonString(), "(True,False,'hello world',None,(),{})");
    ASSERT_EQ(execRow(r9).toPythonString(), "(True,False,'hello world',None,(),{})");
    ASSERT_EQ(execRow(r10).toPythonString(), "(True,False,'hello world',None,(),{})");

    // TODO: nested objects within row?
}