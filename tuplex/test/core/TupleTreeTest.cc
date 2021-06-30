//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Field.h>
#include <gtest/gtest.h>
#include <TupleTree.h>

TEST(TupleTree, LeaveCount) {
    using namespace tuplex;

    TupleTree<Field> t1(python::Type::EMPTYTUPLE);
    EXPECT_EQ(t1.numElements(), 1);

    TupleTree<Field> t2(python::Type::I64);
    EXPECT_EQ(t2.numElements(), 1);

    TupleTree<Field> t3(python::Type::F64);
    EXPECT_EQ(t3.numElements(), 1);

    TupleTree<Field> t4(python::Type::STRING);
    EXPECT_EQ(t4.numElements(), 1);

    TupleTree<Field> t5(python::Type::makeTupleType({python::Type::STRING,
                                                     python::Type::makeTupleType({
                                                                                         python::Type::I64, python::Type::F64})}));
    EXPECT_EQ(t5.numElements(), 3);
}

TEST(TupleTree, Leaves) {
    using namespace tuplex;
    using namespace std;
    TupleTree<int> t5(python::Type::makeTupleType({python::Type::STRING,
                                                     python::Type::makeTupleType({
                                                                                         python::Type::I64, python::Type::F64})}));

    t5.set(vector<int>{0}, 10);
    t5.set(vector<int>{1, 0}, 20);
    t5.set(vector<int>{1, 1}, 30);
    auto v = t5.elements();
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0], 10);
    EXPECT_EQ(v[1], 20);
    EXPECT_EQ(v[2], 30);


    t5.setElements({7, 6, 5});
    v = t5.elements();
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0], 7);
    EXPECT_EQ(v[1], 6);
    EXPECT_EQ(v[2], 5);
}


TEST(TupleTree, LeavesSetterGetter) {
    using namespace tuplex;
    using namespace std;
    TupleTree<int> t5(python::Type::makeTupleType({python::Type::STRING,
                                                   python::Type::makeTupleType({python::Type::I64, python::Type::F64})}));

    t5.set(vector<int>{0}, 10);
    t5.set(vector<int>{1, 0}, 20);
    t5.set(vector<int>{1, 1}, 30);
    auto v = t5.elements();
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0], 10);
    EXPECT_EQ(v[1], 20);
    EXPECT_EQ(v[2], 30);

    t5.set(2, 42);
    EXPECT_EQ(t5.get(0), 10);
    t5.set({0}, 7);
    EXPECT_EQ(t5.get({1, 0}), 20);
    t5.set(1, 6);

    v = t5.elements();
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0], 7);
    EXPECT_EQ(v[1], 6);
    EXPECT_EQ(v[2], 42);
}

TEST(TupleTree, LeavesFieldType) {
    using namespace tuplex;
    using namespace std;
    TupleTree<int> t5(python::Type::makeTupleType({python::Type::STRING,
                                                   python::Type::makeTupleType(vector<python::Type>{python::Type::I64, python::Type::F64})}));

    t5.set(vector<int>{0}, 10);
    t5.set(vector<int>{1, 0}, 20);
    t5.set(vector<int>{1, 1}, 30);

    EXPECT_TRUE(t5.fieldType(0) == python::Type::STRING);
    EXPECT_TRUE(t5.fieldType(1) == python::Type::I64);
    EXPECT_TRUE(t5.fieldType(2) == python::Type::F64);
}