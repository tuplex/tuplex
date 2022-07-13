//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "TestUtils.h"

class ClosureTest : public PyTest {};

TEST_F(ClosureTest, GlobalConstants) {

    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.addGlobal("g", 20);

    UDF udf("lambda x: x + g", "", ce);

    auto res = c.parallelize({Row(10), Row(20)}).map(udf).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 30);
    EXPECT_EQ(res[1].getInt(0), 40);
}

TEST_F(ClosureTest, SimpleRegex) {
    auto code = "lambda x: re.search('\\\\d+', x) != None";
    auto code2 = "lambda x: regex.search('\\\\d+', x) != None";
    auto code3 = "lambda x: search('\\\\d+', x) != None";
    auto code4 = "lambda x: re_search('\\\\d+', x) != None";

    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    // (1) standard importing
    // import re
    // f = lambda x: re.search('\\d+', x) != None

    ClosureEnvironment ce;
    ce.addGlobal("g", 20);
    ce.importModuleAs("re", "re");

    UDF udf(code, "", ce);

    auto res = c.parallelize({Row("abc"), Row("123"), Row("12a45")}).map(udf).collectAsVector();
    ASSERT_EQ(res.size(), 3);
    EXPECT_EQ(res[0].getBoolean(0), false);
    EXPECT_EQ(res[1].getBoolean(0), true);
    EXPECT_EQ(res[2].getBoolean(0), true);

    // (2) import with aliasing of module name
    // import re as regex
    // f = lambda x: regex.search('\\d+', x) != None
    ClosureEnvironment ce2;
    ce2.importModuleAs("regex", "re");

    UDF udf2(code2, "", ce2);

    auto res2 = c.parallelize({Row("abc"), Row("123"), Row("12a45")}).map(udf2).collectAsVector();
    ASSERT_EQ(res2.size(), 3);
    EXPECT_EQ(res2[0].getBoolean(0), false);
    EXPECT_EQ(res2[1].getBoolean(0), true);
    EXPECT_EQ(res2[2].getBoolean(0), true);

    // (3) import of function from module
    // from re import search
    // f = lambda x: search('\\d+', x) != None
    ClosureEnvironment ce3;
    ClosureEnvironment::Function f3;
    f3.identifier = "search";
    f3.qualified_name = "search";
    f3.package = "re";
    f3.location ="builtin";
    ce3.addFunction(f3);

    UDF udf3(code3, "", ce3);

    auto res3 = c.parallelize({Row("abc"), Row("123"), Row("12a45")}).map(udf3).collectAsVector();
    ASSERT_EQ(res3.size(), 3);
    EXPECT_EQ(res3[0].getBoolean(0), false);
    EXPECT_EQ(res3[1].getBoolean(0), true);
    EXPECT_EQ(res3[2].getBoolean(0), true);

    // ==> i.e. ClosureEnvironment should have functions which come from another module!!!

    // then, the next special cases with as
    // i.e. single function aliased

    // (4) import of function from module and aliasing thereof
    // from re import search as srch
    // f = lambda x: srch('\\d+', x) != None

    ClosureEnvironment ce4;
    ClosureEnvironment::Function f4;
    f4.identifier = "re_search";
    f4.qualified_name = "search";
    f4.package = "re";
    f4.location ="builtin";
    ce4.addFunction(f4);

    UDF udf4(code4, "", ce4);

    auto res4 = c.parallelize({Row("abc"), Row("123"), Row("12a45")}).map(udf4).collectAsVector();
    ASSERT_EQ(res4.size(), 3);
    EXPECT_EQ(res4[0].getBoolean(0), false);
    EXPECT_EQ(res4[1].getBoolean(0), true);
    EXPECT_EQ(res4[2].getBoolean(0), true);
}


TEST_F(ClosureTest, GlobalReturnRewriting) {
    using namespace tuplex;

    // this should work!
    auto code = "def f(x):\n"
                "\treturn {GLOBAL_VAR_NAME: str(x)}\n";

    ClosureEnvironment ce;
    ce.addGlobal("GLOBAL_VAR_NAME", "var");

    UDF udf(code, "", ce);
    Context c(microTestOptions());
    auto& ds = c.parallelize({Row(10), Row(20)}).map(udf);
    auto res = ds.collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getString(0), "10");
    EXPECT_EQ(res[1].getString(0), "20");
    EXPECT_EQ(ds.columns().size(), 1);
    EXPECT_EQ(ds.columns()[0], "var");
}

TEST_F(ClosureTest, SpecializeAttribute) {
    using namespace tuplex;
    using namespace std;
    ClosureEnvironment ce;
    ce.importModuleAs("random", "random");
    UDF udf("lambda x: ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for t in range(10)])", "", ce);
    Context c(microTestOptions());
    auto& ds = c.parallelize({Row(10), Row(20)}).map(udf);
    auto res = ds.collectAsVector();

    for(auto r : res)
        cout<<r.toPythonString()<<endl;
}


// define new test for bad UDF
// i.e. c.parallelize([(0, '123'), (1, 'abc')]).filter(lambda a, b: 10 / a > len(b)).resolve(ZeroDivisionError, lambda x: re.search('\\d+', x) != None).collect()
// -> should not end in segfault! instead graciously exit!!!

TEST_F(ClosureTest, IntegerIndexing) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());
    ClosureEnvironment ce;
    ce.importModuleAs("re", "re");
    ce.addGlobal("NUM_COL_IDX", 1);
    auto v = c.parallelize({Row(0, "123"), Row(12, "abc")}).filter(UDF("lambda a, b: 10 / a > len(b)"))
    .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: re.search('\\\\d', x[1]) != None", "", ce))
    .map(UDF("lambda x: int(x[NUM_COL_IDX])", "", ce)).collectAsVector();

    ASSERT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "(123,)");
}

TEST_F(ClosureTest, DoNotCrashOnMissingSymbol) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

//    // missing import, provide warning message!
//    c.parallelize({Row(0), Row(10)}).map(UDF("lambda x: math.cos(x)")).collect();

    // no symbol found in module, i.e. AttributeError!
    ClosureEnvironment ce;
    ce.fromModuleImport("math", "*"); // everything
    c.parallelize({Row(0), Row(10)}).map(UDF("lambda x: math.xyz(x)")).collect();
}


// also do a test for
// NUM_COL_IDX = 1
//        res = c.parallelize([(0, '123'), (12, 'abc')]).filter(lambda a, b: 10 / a > len(b)) \
//               .resolve(ZeroDivisionError, lambda x: re.search('\\d+', x[1]) != None) \
//               .map(lambda x: int(x[NUM_COL_IDX])).collect()
//        self.assertEqual(res, ['123'])
// => that should work as welL!


// other interesting symbol table issues/python behaviors...

// GLOBAL_VAR_NAME = 'test'
//def f(x):
//    if x> 20:
//        GLOBAL_VAR_NAME='hello'
//        print(GLOBAL_VAR_NAME)
//    else:
//        print(GLOBAL_VAR_NAME)
//f(40) --> prints hello
//f(10) --> throw UnboundLocalError


//GLOBAL_VAR_NAME = 'test'
//def f(x):
//    if x> 20:
//        print(GLOBAL_VAR_NAME)
//    else:
//        print(GLOBAL_VAR_NAME)
//f(40) -> prints test
//f(10) -> prints test

// def f(x):
//    if x > 10:
//        y = 20
//    print(y)
//f(30) # no error, 20 bound to y!
//f(10) #-> UnboundLocalError

// cf. here for some of the weird scoping stuff in python...
// https://eli.thegreenplace.net/2011/05/15/understanding-unboundlocalerror-in-python
// https://medium.com/@vbsreddy1/unboundlocalerror-when-the-variable-has-a-value-in-python-e34e097547d6

// and https://github.com/python/cpython/blob/fd009e606a48e803e7187983bf9a5682e938fddb/Python/symtable.c


// @TODO:
// TEST(..., Fallback) {
// use some unsupported module, i.e. os.path.join(..)
// which is then operated in fallback mode...
// }

// TODO: resolve symbol table issues +