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
#include "TestUtils.h"
#include <fstream>
#include <memory>
#include <Context.h>
#include <DataSet.h>
#include <third_party/ryu/ryu.h>
#include "llvm/Transforms/IPO/PassManagerBuilder.h"

class UseCaseFunctionsTest : public PyTest {
protected:
    std::unique_ptr<tuplex::Context> context;
    void SetUp() override {

        PyTest::SetUp();

        context = std::make_unique<tuplex::Context>(microTestOptions());
    }

    void TearDown() override {
        PyTest::TearDown();
    }

};

TEST_F(UseCaseFunctionsTest, LenCall) {
    using namespace tuplex;

    auto v = context->parallelize({Row("hello"), Row("world"), Row("!"), Row("")})
            .map(UDF("lambda x: len(x)")).collectAsVector();

    ASSERT_EQ(v.size(), 4);

    EXPECT_EQ(v[0], Row((int)strlen("hello")));
    EXPECT_EQ(v[1], Row((int)strlen("world")));
    EXPECT_EQ(v[2], Row((int)strlen("!")));
    EXPECT_EQ(v[3], Row((int)strlen("")));
}

// @Todo: Test len on tuples as well + python tests.... No time yet to implement...

TEST_F(UseCaseFunctionsTest, UpperCall) {
    using namespace tuplex;

    auto v = context->parallelize({Row("hello"), Row("world"), Row("!"), Row("")})
            .map(UDF("lambda x: x.upper()")).collectAsVector();

    ASSERT_EQ(v.size(), 4);

    EXPECT_EQ(v[0], Row("HELLO"));
    EXPECT_EQ(v[1], Row("WORLD"));
    EXPECT_EQ(v[2], Row("!"));
    EXPECT_EQ(v[3], Row(""));
}

TEST_F(UseCaseFunctionsTest, LowerCall) {
    using namespace tuplex;

    auto v = context->parallelize({Row("HeLlo"), Row("wOrld"), Row("!"), Row("")})
            .map(UDF("lambda x: x.lower()")).collectAsVector();

    ASSERT_EQ(v.size(), 4);

    EXPECT_EQ(v[0], Row("hello"));
    EXPECT_EQ(v[1], Row("world"));
    EXPECT_EQ(v[2], Row("!"));
    EXPECT_EQ(v[3], Row(""));
}

TEST_F(UseCaseFunctionsTest, cleanCity) {
    using namespace tuplex;

    auto code = "lambda x: x[0].upper() + x[1:].lower()";

    std::vector<Row> input{Row("WOBURN"), Row("Woburn"), Row("WINCHESTER"), Row("Winchester"), Row("SAUGUS"), Row("Saugus"), Row("Lynn"), Row("DEDHAM"), Row("Dedham"), Row("BOSTON"), Row("Boston"), Row("Street"), Row("Beacon Hill"), Row("boston"), Row("Commonwealth"), Row("Roxbury"), Row("Newton"), Row("ROXBURY"), Row("Jamaica Plain"), Row("DORCHESTER"), Row("Roxbury Crossing"), Row("Mission Hill"), Row("Dorchester"), Row("Dorchester Center"), Row("Boston Dorchester"), Row("Dorchester Lower Mills"), Row("Mattapan"), Row("MATTAPAN"), Row("South Boston"), Row("SOUTH BOSTON"), Row("MA 02127 unit 3"), Row("East Boston"), Row("East boston"), Row("Charlestown"), Row("Roslindale"), Row("West Roxbury"), Row("CHESTNUT HILL"), Row("Needham"), Row("NEEDHAM"), Row("Brighton"), Row("Allston"), Row("WESTWOOD"), Row("Quincy"), Row("RANDOLPH"), Row("BRIGHTON"), Row("Hyde Park"), Row("HYDE PARK"), Row("CAMBRIDGE"), Row("Cambridge"), Row("Cambridge Central Square"), Row("cambridge"), Row("Cambridge Harvard Square"), Row("North Cambridge"), Row("East Cambridge"), Row("Cambridge East Cambridge"), Row("Cambridge Kendall Square"), Row("SOMERVILLE"), Row("Somerville"), Row("Somervillle"), Row("somerville"), Row("Somervile"), Row("Malden"), Row("MALDEN"), Row("Revere"), Row("Melrose"), Row("EVERETT"), Row("Everett"), Row("everett"), Row("CHELSEA"), Row("Chelsea"), Row("MEDFORD"), Row("Medford"), Row("Brookline"), Row("BROOKLINE"), Row("QUINCY"), Row("North Quincy"), Row("Squantum"), Row("MELROSE"), Row("STONEHAM"), Row("Stoneham"), Row("WATERTOWN"), Row("WINTHROP"), Row("LYNN"), Row("ARLINGTON"), Row("St"), Row("Boston Allston"), Row("NEWTON"), Row("newton"), Row("Newton MA"), Row("Newton Center"), Row("Newton Centre"), Row("Chestnut Hill"), Row("Boston Clg"), Row("Chestnut hill"), Row("Chestnut Hill Brookline"), Row("WEST ROXBURY"), Row("Watertown"), Row("East Watertown"), Row("watertown"), Row("Arlington"), Row("arlington"), Row("BELMONT"), Row("Belmont")};
    std::vector<Row> expected{Row("Woburn"), Row("Woburn"), Row("Winchester"), Row("Winchester"), Row("Saugus"), Row("Saugus"), Row("Lynn"), Row("Dedham"), Row("Dedham"), Row("Boston"), Row("Boston"), Row("Street"), Row("Beacon hill"), Row("Boston"), Row("Commonwealth"), Row("Roxbury"), Row("Newton"), Row("Roxbury"), Row("Jamaica plain"), Row("Dorchester"), Row("Roxbury crossing"), Row("Mission hill"), Row("Dorchester"), Row("Dorchester center"), Row("Boston dorchester"), Row("Dorchester lower mills"), Row("Mattapan"), Row("Mattapan"), Row("South boston"), Row("South boston"), Row("Ma 02127 unit 3"), Row("East boston"), Row("East boston"), Row("Charlestown"), Row("Roslindale"), Row("West roxbury"), Row("Chestnut hill"), Row("Needham"), Row("Needham"), Row("Brighton"), Row("Allston"), Row("Westwood"), Row("Quincy"), Row("Randolph"), Row("Brighton"), Row("Hyde park"), Row("Hyde park"), Row("Cambridge"), Row("Cambridge"), Row("Cambridge central square"), Row("Cambridge"), Row("Cambridge harvard square"), Row("North cambridge"), Row("East cambridge"), Row("Cambridge east cambridge"), Row("Cambridge kendall square"), Row("Somerville"), Row("Somerville"), Row("Somervillle"), Row("Somerville"), Row("Somervile"), Row("Malden"), Row("Malden"), Row("Revere"), Row("Melrose"), Row("Everett"), Row("Everett"), Row("Everett"), Row("Chelsea"), Row("Chelsea"), Row("Medford"), Row("Medford"), Row("Brookline"), Row("Brookline"), Row("Quincy"), Row("North quincy"), Row("Squantum"), Row("Melrose"), Row("Stoneham"), Row("Stoneham"), Row("Watertown"), Row("Winthrop"), Row("Lynn"), Row("Arlington"), Row("St"), Row("Boston allston"), Row("Newton"), Row("Newton"), Row("Newton ma"), Row("Newton center"), Row("Newton centre"), Row("Chestnut hill"), Row("Boston clg"), Row("Chestnut hill"), Row("Chestnut hill brookline"), Row("West roxbury"), Row("Watertown"), Row("East watertown"), Row("Watertown"), Row("Arlington"), Row("Arlington"), Row("Belmont"), Row("Belmont")};
    auto v = context->parallelize(input)
            .map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), expected.size());

    for(int i = 0; i < v.size(); ++i)
        EXPECT_EQ(v[i], expected[i]);
}


TEST_F(UseCaseFunctionsTest, IntCast) {
    using namespace tuplex;

    // int cast is interesting, because it can throw a parse exception...
    // it actually throws a ValueError (in Python).
    auto v = context->parallelize({Row("200"), Row("0"), Row("-10"), Row("42")})
            .map(UDF("lambda x: int(x)")).collectAsVector();

    ASSERT_EQ(v.size(), 4);

    EXPECT_EQ(v[0], Row(200));
    EXPECT_EQ(v[1], Row(0));
    EXPECT_EQ(v[2], Row(-10));
    EXPECT_EQ(v[3], Row(42));


    // TODO: New broken test: calling constructor with no arguments breaks
//    auto v1 = context->parallelize({Row("123")})
//            .map(UDF("lambda x: int()")).collectAsVector();
//
//    ASSERT_EQ(v1.size(), 1);
//    EXPECT_EQ(v1[0], Row(0));
}

TEST_F(UseCaseFunctionsTest, IntCastII) {
    using namespace tuplex;

    // int cast is interesting, because it can throw a parse exception...
    // it actually throws a ValueError (in Python).
    auto v = context->parallelize({Row(20.7), Row(3.141), Row(0.0), Row(-8.7)})
            .map(UDF("lambda x: int(x)")).collectAsVector();

    ASSERT_EQ(v.size(), 4);

    EXPECT_EQ(v[0], Row(20));
    EXPECT_EQ(v[1], Row(3));
    EXPECT_EQ(v[2], Row(0));
    EXPECT_EQ(v[3], Row(-8));
}

TEST_F(UseCaseFunctionsTest, FloatCast) {
    using namespace tuplex;

    auto v1 = context->parallelize({Row("20"), Row("3.141"), Row("0"), Row("iNf"), Row("iNfINity"), Row("naN"), Row("-5.23")})
            .map(UDF("lambda x: float(x)")).collectAsVector();

    ASSERT_EQ(v1.size(), 7);

    EXPECT_EQ(v1[0], Row(20.0));
    EXPECT_EQ(v1[1], Row(3.141));
    EXPECT_EQ(v1[2], Row(0.0));
    EXPECT_EQ(v1[3], Row(std::numeric_limits<double>::infinity()));
    EXPECT_EQ(v1[4], Row(std::numeric_limits<double>::infinity()));
    EXPECT_EQ(v1[5], Row(std::numeric_limits<double>::quiet_NaN()));
    EXPECT_EQ(v1[6], Row(-5.23));

    auto v2 = context->parallelize({Row(20), Row(-30)})
            .map(UDF("lambda x: float(x)")).collectAsVector();

    ASSERT_EQ(v2.size(), 2);

    EXPECT_EQ(v2[0], Row(20.0));
    EXPECT_EQ(v2[1], Row(-30.0));
}

TEST_F(UseCaseFunctionsTest, BoolCast) {
    using namespace tuplex;

    auto v1 = context->parallelize({Row("hello"), Row("3.141"), Row("False"), Row("")})
            .map(UDF("lambda x: bool(x)")).collectAsVector();

    ASSERT_EQ(v1.size(), 4);

    EXPECT_EQ(v1[0], Row(true));
    EXPECT_EQ(v1[1], Row(true));
    EXPECT_EQ(v1[2], Row(true));
    EXPECT_EQ(v1[3], Row(false));

    auto v2 = context->parallelize({Row(20), Row(-30), Row(0)})
            .map(UDF("lambda x: bool(x)")).collectAsVector();

    ASSERT_EQ(v2.size(), 3);

    EXPECT_EQ(v2[0], Row(true));
    EXPECT_EQ(v2[1], Row(true));
    EXPECT_EQ(v2[2], Row(false));

    auto v3 = context->parallelize({Row(-10.123), Row(0), Row(1.234), Row(std::numeric_limits<double>::infinity()),
                                    Row(std::numeric_limits<double>::quiet_NaN())})
            .map(UDF("lambda x: bool(x)")).collectAsVector();

    ASSERT_EQ(v3.size(), 5);

    EXPECT_EQ(v3[0], Row(true));
    EXPECT_EQ(v3[1], Row(false));
    EXPECT_EQ(v3[2], Row(true));
    EXPECT_EQ(v3[3], Row(true));
    EXPECT_EQ(v3[4], Row(true));
}

TEST_F(UseCaseFunctionsTest, StrCast) {
    using namespace tuplex;

    auto v0 = context->parallelize({Row(false, true)})
            .map(UDF("lambda x, y: (str(x), str(y))")).collectAsVector();

    ASSERT_EQ(v0.size(), 1);
    EXPECT_EQ(v0[0].toPythonString(), std::string("('False','True')"));

    v0 = context->parallelize({Row(false, true)})
            .map(UDF("lambda x: (str(x[0]), str(x[1]))")).collectAsVector();


    ASSERT_EQ(v0.size(), 1);
    EXPECT_EQ(v0[0].toPythonString(), std::string("('False','True')"));

    auto v1 = context->parallelize({Row(-10.123), Row(3.141), Row(0.0), Row(std::numeric_limits<double>::infinity()),
                                    Row(std::numeric_limits<double>::quiet_NaN())})
            .map(UDF("lambda x: str(x)")).collectAsVector();

    ASSERT_EQ(v1.size(), 5);

    EXPECT_EQ(v1[0], Row("-10.123"));
    EXPECT_EQ(v1[1], Row("3.141"));
    EXPECT_EQ(v1[2], Row("0.0"));
    EXPECT_EQ(v1[3], Row("inf"));
    EXPECT_EQ(v1[4], Row("nan"));

    auto v2 = context->parallelize({Row(20), Row(-30), Row(0)})
            .map(UDF("lambda x: str(x)")).collectAsVector();

    ASSERT_EQ(v2.size(), 3);

    EXPECT_EQ(v2[0], Row("20"));
    EXPECT_EQ(v2[1], Row("-30"));
    EXPECT_EQ(v2[2], Row("0"));
}

TEST_F(UseCaseFunctionsTest, StringFormatOperator) {
    using namespace tuplex;

    // int cast is interesting, because it can throw a parse exception...
    // it actually throws a ValueError (in Python).
    auto v = context->parallelize({Row(12), Row(13), Row(14)})
            .map(UDF("lambda x: '%04d' % x")).collectAsVector();

    ASSERT_EQ(v.size(), 3);

    EXPECT_EQ(v[0], Row("0012"));
    EXPECT_EQ(v[1], Row("0013"));
    EXPECT_EQ(v[2], Row("0014"));
}


TEST_F(UseCaseFunctionsTest, StringInOperator) {
    using namespace tuplex;

    // int cast is interesting, because it can throw a parse exception...
    // it actually throws a ValueError (in Python).
    auto v = context->parallelize({Row("hello world"), Row("what a wonderful world"), Row("the earth is a globe")})
            .map(UDF("lambda x: 'world' in x")).collectAsVector();

    ASSERT_EQ(v.size(), 3);

    EXPECT_EQ(v[0], Row(true));
    EXPECT_EQ(v[1], Row(true));
    EXPECT_EQ(v[2], Row(false));
}

TEST_F(UseCaseFunctionsTest, StringInOperatorII) {
    using namespace tuplex;
    auto v = context->parallelize({Row("hello world"), Row("what a wonderful world"), Row("the earth is a globe")})
            .map(UDF("lambda x: 'world' not in x")).collectAsVector();

    ASSERT_EQ(v.size(), 3);

    EXPECT_EQ(v[0], Row(false));
    EXPECT_EQ(v[1], Row(false));
    EXPECT_EQ(v[2], Row(true));
}

TEST_F(UseCaseFunctionsTest, extractOffer) {
    using namespace tuplex;

    auto code = "def extractOffer(x):\n"
                "    offer = x.lower()\n"
                "\n"
                "    if 'sale' in offer:\n"
                "        offer = 'sale'\n"
                "    elif 'rent' in offer:\n"
                "        offer = 'rent'\n"
                "    elif 'sold' in offer:\n"
                "        offer = 'sold'\n"
                "    elif 'foreclos' in offer.lower():\n"
                "        offer = 'foreclosed'\n"
                "    else:\n"
                "        offer = 'unknown'\n"
                "\n"
                "    return offer";

    auto v = context->parallelize({Row("House for sale"), Row("Townhouse for sale"),
                                   Row("Condo for sale"), Row("For sale by owner"),
                                   Row("Apartment for sale"), Row("Foreclosure"),
                                   Row("Foreclosed"), Row("Coming soon"),
                                   Row("New construction"), Row("Make me move®")})
                                           .map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 10);

    for(auto r : v) {
        std::cout<<r.toPythonString()<<std::endl;
    }

    // test results
    EXPECT_EQ(v[0], Row("sale"));
    EXPECT_EQ(v[1], Row("sale"));
    EXPECT_EQ(v[2], Row("sale"));
    EXPECT_EQ(v[3], Row("sale"));
    EXPECT_EQ(v[4], Row("sale"));
    EXPECT_EQ(v[5], Row("foreclosed"));
    EXPECT_EQ(v[6], Row("foreclosed"));
    EXPECT_EQ(v[7], Row("unknown"));
    EXPECT_EQ(v[8], Row("unknown"));
    EXPECT_EQ(v[9], Row("unknown"));
}

TEST_F(UseCaseFunctionsTest, strFindFunction) {
    using namespace tuplex;

    auto code = "def test(x, y):\n"
                "    return x.find(y)";

    auto v = context->parallelize({Row("hello", "l"),
                                   Row("hello", "w"),
                                   Row("hello", "")})
            .map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 3);

    for(auto r : v) {
        std::cout<<r.toPythonString()<<std::endl;
    }

    EXPECT_EQ(v[0], Row(2));
    EXPECT_EQ(v[1], Row(-1));
    EXPECT_EQ(v[2], Row(0));
}

TEST_F(UseCaseFunctionsTest, strReverseFindFunction) {
    using namespace tuplex;

    auto code = "def test(x, y):\n"
                "    return x.rfind(y)";

    auto v = context->parallelize({Row("/usr/local/hello", "/"),
                                   Row("this.file.ext", "."),
                                   Row("test", ""),
                                   Row("", ""),
                                   Row("/usr/local/hello", "\\")})
            .map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 5);

    for(auto r : v) {
        std::cout<<r.toPythonString()<<std::endl;
    }

    EXPECT_EQ(v[0], Row(10));
    EXPECT_EQ(v[1], Row(9));
    EXPECT_EQ(v[2], Row(4));
    EXPECT_EQ(v[3], Row(0));
    EXPECT_EQ(v[4], Row(-1));
}

TEST_F(UseCaseFunctionsTest, strReplaceFunction) {
    using namespace tuplex;

    auto code = "def test(x, y):\n"
                "    return x.replace(y, 'abc')";

    auto v = context->parallelize({Row("/usr/local/hello", "/"),
                                   Row("hello world", "world"),
                                   Row("test", ""),
                                   Row("this is a test", "test"),
                                   Row("hello world", "test")})
            .map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 5);

    for(auto r : v) {
        std::cout<<r.toPythonString()<<std::endl;
    }

    EXPECT_EQ(v[0], Row("abcusrabclocalabchello"));
    EXPECT_EQ(v[1], Row("hello abc"));
    EXPECT_EQ(v[2], Row("abctabceabcsabctabc"));
    EXPECT_EQ(v[3], Row("this is a abc"));
    EXPECT_EQ(v[4], Row("hello world"));
}

TEST_F(UseCaseFunctionsTest, strFormatFunction) {
    using namespace tuplex;

    auto code = "def test(s):\n"
                "    return s.format(1, 2, 3)";

    auto v0 = context->parallelize({
        Row("{} {} {}"),
    }).map(UDF(code)).collectAsVector();

    ASSERT_EQ(v0.size(), 1);
    EXPECT_EQ(v0[0].getString(0), Row("1 2 3"));

    code = "def test(s, x, y):\n"
           "    return s.format(x, y)";

    auto v1 = context->parallelize({
        Row("{} {}", 5, "hi"),
        Row("{} bye {}", 10, "goodbye"),
        Row("{:x} = {} in hex", 12, "12"),
        Row("{:X} = {} in upper hex", 12, "12")
    }).map(UDF(code)).collectAsVector();

    ASSERT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], std::string("5 hi"));
    EXPECT_EQ(v1[1], std::string("10 bye goodbye"));
    EXPECT_EQ(v1[2], std::string("c = 12 in hex"));
    EXPECT_EQ(v1[3], std::string("C = 12 in upper hex"));

    // %<(+/-)n>s, %.<n>s, %d, %f, %.<n>f, %x/X (lower/upper hex)
    auto v2 = context->parallelize({
        Row("{1:.{0}f}", 5, 5.123456789),
        Row("{1:{0}.0f}", 3, 6.987654321),
        Row("{1:3.{0}f}", 3, 6.987654321),
        Row("{1:8.{0}f}", 3, 6.987654321)
    }).map(UDF("lambda s, x, y: s.format(x, y)")).collectAsVector();
    ASSERT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0], std::string("5.12346"));
    EXPECT_EQ(v2[1], std::string("  7"));
    EXPECT_EQ(v2[2], std::string("6.988"));
    EXPECT_EQ(v2[3], std::string("   6.988"));

    auto v3 = context->parallelize({
        Row("{0} {2:.{1}s}", 2.63, 5, "goodbye"),
        Row("{0:.2f} hi {2:{1}.0s}", 1.23, 3, "goodbye"),
        Row("hi {0:.2f} {2:3.{1}s}", 3.72,  3, "goodbye"),
        Row("hola {0} {2:8.{1}s}", 2.6, 3, "goodbye")
    }).map(UDF("lambda s, x, y, z: s.format(x, y, z)")).collectAsVector();
    ASSERT_EQ(v3.size(), 4);
    EXPECT_EQ(v3[0], std::string("2.63 goodb"));
    EXPECT_EQ(v3[1], std::string("1.23 hi    "));
    EXPECT_EQ(v3[2], std::string("hi 3.72 goo"));
    EXPECT_EQ(v3[3], std::string("hola 2.6 goo     "));

    auto v4 = context->parallelize({
        Row("hi")
    }).map(UDF("lambda s: s.format()")).collectAsVector();
    ASSERT_EQ(v4.size(), 1);
    EXPECT_EQ(v4[0], std::string("hi"));

    // boolean test
    auto v5 = context->parallelize({Row(false), Row(true)}).map(UDF("lambda b: '{}'.format(b)")).collectAsVector();
    ASSERT_EQ(v5.size(), 2);
    EXPECT_EQ(v5[0], std::string("False"));
    EXPECT_EQ(v5[1], std::string("True"));


    for(auto row : v5)
        std::cout<<row.toPythonString()<<std::endl;
}

TEST_F(UseCaseFunctionsTest, strFormatFunctionBooleans) {
    using namespace tuplex;

    // weird optimizations happening here??
    auto r = Row(false);
    EXPECT_EQ(r.toPythonString(), "(False,)");
    auto r2 = Row(true);
    EXPECT_EQ(r2.toPythonString(), "(True,)");


    // boolean test
    auto v5 = context->parallelize({Row(false), Row(true)}).map(UDF("lambda b: '{}'.format(b)")).collectAsVector();
    ASSERT_EQ(v5.size(), 2);

    std::cout<<"result is:\n---\n"<<std::endl;
    for(auto row : v5)
        std::cout<<row.toPythonString()<<std::endl;

    EXPECT_EQ(v5[0], std::string("False"));
    EXPECT_EQ(v5[1], std::string("True"));
}

TEST_F(UseCaseFunctionsTest, strStripFunctions) {
    using namespace tuplex;

    auto code = "def test(s):\n"
                "    return s.strip()";

    auto v0 = context->parallelize({
           Row("  hello ! \n "),
           Row("  \t\n"),
           Row("abcde  \r\r"),
           Row("  \n\r abcde")
    }).map(UDF(code)).collectAsVector();

    ASSERT_EQ(v0.size(), 4);
    EXPECT_EQ(v0[0], std::string("hello !"));
    EXPECT_EQ(v0[1], std::string(""));
    EXPECT_EQ(v0[2], std::string("abcde"));
    EXPECT_EQ(v0[3], std::string("abcde"));

    auto v1 = context->parallelize({
       Row("  hello ! \n "),
       Row("  \t\n"),
       Row("abcde  \r\r"),
       Row("  \n\r abcde")
    }).map(UDF("lambda s: s.rstrip()")).collectAsVector();

    ASSERT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], std::string("  hello !"));
    EXPECT_EQ(v1[1], std::string(""));
    EXPECT_EQ(v1[2], std::string("abcde"));
    EXPECT_EQ(v1[3], std::string("  \n\r abcde"));

    auto v2 = context->parallelize({
       Row("  hello ! \n "),
       Row("  \t\n"),
       Row("abcde  \r\r"),
       Row("  \n\r abcde")
    }).map(UDF("lambda s: s.lstrip()")).collectAsVector();

    ASSERT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0], std::string("hello ! \n "));
    EXPECT_EQ(v2[1], std::string(""));
    EXPECT_EQ(v2[2], std::string("abcde  \r\r"));
    EXPECT_EQ(v2[3], std::string("abcde"));

    auto v3 = context->parallelize({
       Row("!!abcdegt", "!atg"),
       Row("www.test.com", "w."),
       Row("hello", "helo"),
       Row("?23test\n", "\nt?")
    }).map(UDF("lambda x, y: x.strip(y)")).collectAsVector();

    ASSERT_EQ(v3.size(), 4);
    EXPECT_EQ(v3[0], std::string("bcde"));
    EXPECT_EQ(v3[1], std::string("test.com"));
    EXPECT_EQ(v3[2], std::string(""));
    EXPECT_EQ(v3[3], std::string("23tes"));

    auto v4 = context->parallelize({
           Row("!!abcdegt", "!atg"),
           Row("www.test.com", "w."),
           Row("hello", "helo"),
           Row("?23test\n", "\nt?")
    }).map(UDF("lambda x, y: x.lstrip(y)")).collectAsVector();

    ASSERT_EQ(v4.size(), 4);
    EXPECT_EQ(v4[0], std::string("bcdegt"));
    EXPECT_EQ(v4[1], std::string("test.com"));
    EXPECT_EQ(v4[2], std::string(""));
    EXPECT_EQ(v4[3], std::string("23test\n"));

    auto v5 = context->parallelize({
           Row("!!abcdegt", "!atg"),
           Row("www.test.com.w", "w."),
           Row("hello", "helo"),
           Row("?23test\n", "\nt?")
    }).map(UDF("lambda x, y: x.rstrip(y)")).collectAsVector();

    ASSERT_EQ(v5.size(), 4);
    EXPECT_EQ(v5[0], std::string("!!abcde"));
    EXPECT_EQ(v5[1], std::string("www.test.com"));
    EXPECT_EQ(v5[2], std::string(""));
    EXPECT_EQ(v5[3], std::string("?23tes"));
}

TEST_F(UseCaseFunctionsTest, strSliceStartEnd) {
    using namespace tuplex;

    auto code = "def test(x, a,b):\n"
                "    return x[a:b]";

    auto v = context->parallelize({Row("hello", 0, 2),
                                   Row("hello world", 2, 5)
                                   })
            .map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 2);

    for(auto r : v) {
        std::cout<<r.toPythonString()<<std::endl;
    }

    EXPECT_EQ(v[0], Row("he"));
    EXPECT_EQ(v[1], Row("llo"));
}

// use this here for replace function... https://creativeandcritical.net/str-replace-c
TEST_F(UseCaseFunctionsTest, extractPrice) {
    using namespace tuplex;

    auto code = "def extractPrice(price, offer, facts, sqft):\n"
                "    p = 0\n"
                "    if offer == 'sold':\n"
                "        # price is to be calculated using price/sqft * sqft\n"
                "        val = facts\n"
                "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
                "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
                "        price_per_sqft = int(r)\n"
                "        p = price_per_sqft * sqft\n"
                "    elif offer == 'rent':\n"
                "        max_idx = price.rfind('/')\n"
                "        p = int(price[1:max_idx].replace(',', ''))\n"
                "    else:\n"
                "        # take price from price column\n"
                "        p = int(price[1:].replace(',', ''))\n"
                "\n"
                "    return p";

    auto v = context->parallelize({Row("$489,000", "sale", "2 bds , 1 ba , 920 sqft", 920),
                                   Row("$3,250/mo", "rent", "3 bds , 1.5 ba , 1,100 sqft", 1100),
                                   Row("SOLD", "sold", "Price/sqft: $244 , 4 bds , 2 ba , 2,124 sqft", 2124)})
                                           .map(UDF(code)).collectAsVector();

    for(auto r : v) {
        std::cout<<r.toPythonString()<<std::endl;
    }

    ASSERT_EQ(v.size(), 3);

    EXPECT_EQ(v[0], Row(489000));
    EXPECT_EQ(v[1], Row(3250));
    EXPECT_EQ(v[2], Row(244 * 2124));
}

TEST_F(UseCaseFunctionsTest, VariableOverwrite) {
    // to test: def f(x):
    //    x = 'hello'
    //    return x
    // is an example where string is returned!
    using namespace tuplex;

    // @TODO: some symbol table issue here! Needs to be fixed...
    auto code = "def f(x):\n"
                "    x = 'hello'\n"
                "    return x";

    auto v = context->parallelize({Row(10), Row(20)}).map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row("hello"));
    EXPECT_EQ(v[1], Row("hello"));
}

TEST_F(UseCaseFunctionsTest, VariableOverwriteIf) {
    // to test: def f(x):
    //    x = 'hello'
    //    return x
    // is an example where string is returned!
    using namespace tuplex;

    auto code = "def f(x):\n"
                "   if x >= 10:\n"
                "       x = 'two digits'\n"
                "   else:\n"
                "       x = 'one digit'\n"
                "   return x";

    auto v = context->parallelize({Row(10), Row(2)}).map(UDF(code)).collectAsVector();

    for(const auto& r : v)
        std::cout<<"type: "<<r.getRowType().desc()<<" content: "<<r.toPythonString()<<std::endl;

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].getRowType(), python::Type::makeTupleType({python::Type::STRING}));
    EXPECT_EQ(v[1].getRowType(), python::Type::makeTupleType({python::Type::STRING}));
    EXPECT_EQ(v[0].toPythonString(), Row("two digits").toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row("one digit").toPythonString());
}


TEST_F(UseCaseFunctionsTest, ColumnNamesMap) {
    using namespace tuplex;

    auto v = context->parallelize({Row("hello", 20, -1, 9.0),
                                   Row("world", 30, -2, 10.0),
                                   Row("@",     40, -3, 11.0)}, {"a", "b", "c", "d"})
                     .map(UDF("lambda x: (x['d'], x['b'])")).collectAsVector();

    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0], Row(9.0, 20));
    EXPECT_EQ(v[1], Row(10.0, 30));
    EXPECT_EQ(v[2], Row(11.0, 40));
}

TEST_F(UseCaseFunctionsTest, withColumnSimple) {
    using namespace tuplex;

    auto v = context->parallelize({Row(20),
                                   Row(30),
                                   Row(40)}, {"a"})
            .withColumn("x", UDF("lambda a: a / 2 - 1.0")).collectAsVector();

    for(const auto &r: v)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0], Row(20, 9.0));
    EXPECT_EQ(v[1], Row(30, 14.0));
    EXPECT_EQ(v[2], Row(40, 19.0));
}

TEST_F(UseCaseFunctionsTest, scientificNumbers) {
    using namespace tuplex;

    auto v = context->parallelize({Row(20000),
                                   Row(300),
                                   Row(4000),
                                   Row(100000)})
            .filter(UDF("lambda x: x < 2e4")).collectAsVector();

    for(auto r: v)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row(300));
    EXPECT_EQ(v[1], Row(4000));
}

TEST_F(UseCaseFunctionsTest, IfShortCircuit) {
    using namespace tuplex;
    using namespace std;

    auto lam0 = "def g(x):\n"
               "    if True or (1/x > 0):\n"
               "        return 1\n"
               "    else:\n"
               "        return 0";

    auto res0 = context->parallelize({3, 2, 1, 0, -1, -2, -3}).map(UDF(lam0)).collectAsVector();
    ASSERT_EQ(res0.size(), 7);
    for(int i = 0; i < 7; i++) ASSERT_EQ(res0[i], 1);

    auto lam1 = "def g(x):\n"
                "    if False and (1/x > 0):\n"
                "        return 1\n"
                "    else:\n"
                "        return 0";

    auto res1 = context->parallelize({3, 2, 1, 0, -1, -2, -3}).map(UDF(lam1)).collectAsVector();
    ASSERT_EQ(res1.size(), 7);
    for(int i = 0; i < 7; i++) ASSERT_EQ(res1[i], 0);

    auto lam2 = "def g(x):\n"
                "    if (x == 1 or 1/(x-1) > 0) and (1/(x) > 0):\n"
                "        return 1\n"
                "    else:\n"
                "        return 0";

    auto res2 = context->parallelize({3, 2, 1, 0, -1, -2, -3}).map(UDF(lam2)).collectAsVector();
    ASSERT_EQ(res2.size(), 7);
    for(int i = 0; i < 3; i++) ASSERT_EQ(res2[i], 1);
    for(int i = 3; i < 7; i++) ASSERT_EQ(res2[i], 0);


    auto lam3 = "def l3(x):\n"
                "    if (x == 0 or x == 1) and ((x != 0 and 1/(x) <= 0) or (x != 1 and 1/(x-1) <= 0)):\n"
                "        return 1\n"
                "    else:\n"
                "        return 0";

    auto res3 = context->parallelize({3, 2, 1, 0, -1, -2, -3}).map(UDF(lam3)).collectAsVector();
    ASSERT_EQ(res3.size(), 7);
    for(int i = 0; i < 2; i++) ASSERT_EQ(res3[i], 0);
    ASSERT_EQ(res3[2], 0);
    ASSERT_EQ(res3[3], 1);
    for(int i = 4; i < 7; i++) ASSERT_EQ(res3[i], 0);
}

TEST_F(UseCaseFunctionsTest, NestedIf) {
    using namespace tuplex;
    using namespace std;

    auto fillInTimes_C = "def fillInTimesUDF(row):\n"
                         "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
                         "    if row['DivReachedDest']:\n"
                         "        if int(row['DivReachedDest']) > 0:\n"
                         "            return float(row['DivActualElapsedTime'])\n"
                         "        else:\n"
                         "            return ACTUAL_ELAPSED_TIME\n"
                         "    else:\n"
                         "        return ACTUAL_ELAPSED_TIME";

    // DivActualElapsedTime is nulled.
    // however, no data occurs where that branch is visited! Hence, it can get speculated away!
    Row r1(89.0, option<double>::none, Field::null());
    Row r2(option<double>::none, 1.0, Field::null());

    auto res = context->parallelize({r1, r2}, {"ActualElapsedTime", "DivReachedDest", "DivActualElapsedTime"}).withColumn("ActualElapsedTime", UDF(fillInTimes_C)).collectAsVector();

    for(auto r : res)
        cout<<r.toPythonString()<<endl;


    //   Row r1(89.0, option<double>::none, option<double>::none);
    //    Row r2(option<double>::none, 1.0, 211.0);
    //
    //    auto res = context->parallelize({r1, r2}, {"ActualElapsedTime", "DivReachedDest", "DivActualElapsedTime"}).withColumn("ActualElapsedTime", UDF(fillInTimes_C)).collectAsVector();
    //
    //    for(auto r : res)
    //        cout<<r.toPythonString()<<endl;

    // @TODO: check what happens in the case options for DivActualElapsedTime gets specialized to NULL.
    // -> first type annotator visitor for float(null) fails!
    // -> requires speculation!

}

TEST_F(UseCaseFunctionsTest, regexSearch) {
    using namespace tuplex;

    ClosureEnvironment ce;
    ce.importModuleAs("re", "re");

    // global pattern
    auto v1 = context->parallelize({Row("hello"),
                                   Row("world"), Row("ball")})
            .map(UDF("lambda x: re.search(r'(.*)l.*', x)[1]", "", ce)).collectAsVector();

    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0], Row("hel"));
    EXPECT_EQ(v1[1], Row("wor"));
    EXPECT_EQ(v1[2], Row("bal"));

    auto code = "def f(x):\n"
                "    match = re.search(r'(.*)l(.*)', x)\n"
                "    return (match[1], match[2])";

    auto v2 = context->parallelize({Row("hello"), Row("world"), Row("ball")})
            .map(UDF(code, "", ce)).collectAsVector();

    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0], Row("hel", "o"));
    EXPECT_EQ(v2[1], Row("wor", "d"));
    EXPECT_EQ(v2[2], Row("bal", ""));

    // bad PCRE2 pattern example
    // const char* pattern = "(.*)l[^0-9][0-9]*)";
    // int error_code = 0;
    // size_t error_offset = 0;
    // auto compiled_pattern = pcre2_compile_8(reinterpret_cast<PCRE2_SPTR8>(pattern), strlen(pattern), 0, &error_code, &error_offset, nullptr);

    // global pattern with addition
    auto v3 = context->parallelize({Row("hello123"),
                                    Row("world456"), Row("ball789")})
            .map(UDF("lambda x: re.search(r'(.*)l[^0-9]*' + r'([0-9]*)', x)[2]", "", ce)).collectAsVector();

    ASSERT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0], Row("123"));
    EXPECT_EQ(v3[1], Row("456"));
    EXPECT_EQ(v3[2], Row("789"));

    code = "def f(x):\n"
                "    p1 = r'(.*)l([^0-9]*)'\n"
                "    p2 = r'([0-9]*)'\n"
                "    match = re.search(p1 + p2, x)\n"
                "    return (match[1], match[2], match[3])";

    auto v4 = context->parallelize({Row("hello123"), Row("world456"), Row("ball789")})
            .map(UDF(code, "", ce)).collectAsVector();

    ASSERT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0], Row("hel", "o", "123"));
    EXPECT_EQ(v4[1], Row("wor", "d", "456"));
    EXPECT_EQ(v4[2], Row("bal", "", "789"));

    // non-global pattern
    auto v5 = context->parallelize({Row("(.*)l.*", "hello"),
                                    Row("(.*)(l).*", "world456"), Row("(.*)(l)(.*)", "ball")})
            .map(UDF("lambda x, y: re.search(x, y)[1]", "", ce)).collectAsVector();

    ASSERT_EQ(v5.size(), 3);
    EXPECT_EQ(v5[0], Row("hel"));
    EXPECT_EQ(v5[1], Row("wor"));
    EXPECT_EQ(v5[2], Row("bal"));

    code = "def f(x, y):\n"
           "    match = re.search(x, y)\n"
           "    return (match[1], match[2])";

    auto v6 = context->parallelize({Row("(.*)l(.*)", "hello"),
                                    Row("(.*)(l).*", "world456"), Row("(.*)(l)(.*)", "ball")})
            .map(UDF(code, "", ce)).collectAsVector();

    ASSERT_EQ(v6.size(), 3);
    EXPECT_EQ(v6[0], Row("hel", "o"));
    EXPECT_EQ(v6[1], Row("wor", "l"));
    EXPECT_EQ(v6[2], Row("bal", "l"));
}


// test null-value optimization with ifelse
// IA: ifelse with if branch normal
// IB: ifelse with else branch normal
// IIA: ifelse (nested) with if branch normal
// IIB: ifelse (nested) with else branch normal
// IIIa: if with if branch normal
// IIIb: if with if branch except
// all of the above, but as expressions instead of statements!

TEST_F(UseCaseFunctionsTest, NullValueOptIf) {
    using namespace tuplex;
    using namespace std;

    auto opt_nopt = microTestOptions();
    // enable NullValue Optimization
    opt_nopt.set("tuplex.useLLVMOptimizer", "true");
    opt_nopt.set("tuplex.optimizer.generateParser", "true");
    opt_nopt.set("tuplex.executorCount", "0");

    opt_nopt.set("tuplex.optimizer.nullValueOptimization", "true");
    opt_nopt.set("tuplex.normalcaseThreshold", "0.6"); // set higher, so optimization is more aggressive.
    Context c(opt_nopt);

    // test code with simple if branch
    auto code_I = "def test(a, b):\n"
                  "\tif a:\n"
                  "\t\treturn 10\n"
                  "\telse:\n"
                  "\t\treturn b\n";

    // null-value opt does not work with parallelize, b.c. there the type is known
    // so it will be rejected! However, more interesting case when reading from files!
    std::stringstream ss;
    ss<<"A, B\n";
    for(int i = 0; i < 10; ++i)
        ss<<"10,20\n";
    ss<<",20\n";
    stringToFile(URI(testName + ".csv"), ss.str());

    // IA:
    auto vIA = c.csv(testName + ".csv")
                   .map(UDF(code_I)).collectAsVector();

    for(auto r : vIA)
        std::cout<<r.toPythonString()<<std::endl;
    ASSERT_EQ(vIA.size(), 11);
}

TEST_F(UseCaseFunctionsTest, FloatNullError) {
    using namespace tuplex;
    using namespace std;

    auto opt_nopt = microTestOptions();
    // enable NullValue Optimization
    opt_nopt.set("tuplex.optimizer.nullValueOptimization", "true");
    opt_nopt.set("tuplex.normalcaseThreshold", "0.6"); // set higher, so optimization is more aggressive.
    Context c(opt_nopt);

     // Note: when using null, this should generate a type error
     // => when name not found, generate NameError
     auto row = Row(option<string>::none);
     auto res = c.parallelize({row, row}).map(UDF("lambda x: float(x) if x else None")).collectAsVector();

    // this scenario here could occur with the optimizer, i.e. address that

//    auto rowII = Row(Field::null());
//    auto resII = c.parallelize({rowII, rowII}).map(UDF("lambda x: float(x) if x else None")).collectAsVector();

//    cout<<"pipeline result:"<<endl;
//    for(auto r : resII) {
//        cout<<r.toPythonString()<<endl;
//    }
}

// function with different return types => test
// i.e. string/int


//THERE IS SOME ISSUE
//with the extractOffer function written like this:
//def extractOffer(x):
//offer = x['title']
//
//if 'Sale' in offer:
//offer = 'sale'
//if 'Rent' in offer:
//offer = 'rent'
//if 'SOLD' in offer:
//offer = 'sold'
//if 'foreclose' in offer.lower():
//        offer = 'foreclosed'
//
//return offer
//
//checkout what goes wrong here with the real input. it seems none of the if branches is visited when using nullValueOp false
//probably an issue with the in operator and option[type]??

TEST_F(UseCaseFunctionsTest, regexSub) {
    using namespace tuplex;

    ClosureEnvironment ce;
    ce.importModuleAs("re", "re");

    // global pattern
    auto v1 = context->parallelize({Row("hello"),
                                    Row("world"), Row("ball")})
            .map(UDF("lambda x: re.sub('l', 'x', x)", "", ce)).collectAsVector();

    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0], Row("hexxo"));
    EXPECT_EQ(v1[1], Row("worxd"));
    EXPECT_EQ(v1[2], Row("baxx"));

    // match groups
    auto v2 = context->parallelize({Row("hello123"),
                                    Row("world456"), Row("ball789")})
            .map(UDF("lambda x: re.sub(r'(.*)l([^0-9]*)', '$1', x)", "", ce)).collectAsVector();

    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0], Row("hel123"));
    EXPECT_EQ(v2[1], Row("wor456"));
    EXPECT_EQ(v2[2], Row("bal789"));
}

TEST_F(UseCaseFunctionsTest, randomChoice) {
    using namespace tuplex;

    ClosureEnvironment ce;
    ce.importModuleAs("re", "re");
    ce.importModuleAs("random", "random");

    auto v1 = context->parallelize({Row("hello"), Row("world"), Row("abc"), Row("def")}).map(UDF("lambda x: random.choice(x)", "", ce)).collectAsVector();
    ASSERT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0].getString(0).size(), 1);
    ASSERT_TRUE(std::string("hello").find(v1[0].getString(0)[0]) != std::string::npos);
    EXPECT_EQ(v1[1].getString(0).size(), 1);
    ASSERT_TRUE(std::string("world").find(v1[1].getString(0)[0]) != std::string::npos);
    EXPECT_EQ(v1[2].getString(0).size(), 1);
    ASSERT_TRUE(std::string("abc").find(v1[2].getString(0)[0]) != std::string::npos);
    EXPECT_EQ(v1[3].getString(0).size(), 1);
    ASSERT_TRUE(std::string("def").find(v1[3].getString(0)[0]) != std::string::npos);

    auto v2 = context->parallelize({Row(List(1, 2, 3, 4)), Row(List(2, 3, 4, 5)), Row(List(3, 4)), Row(List(-1, 0, 1))}).map(UDF("lambda x: random.choice(x)", "", ce)).collectAsVector();
    ASSERT_EQ(v2.size(), 4);
    EXPECT_TRUE(v2[0].getInt(0) >= 1);
    EXPECT_TRUE(v2[0].getInt(0) <= 4);
    EXPECT_TRUE(v2[1].getInt(0) >= 2);
    EXPECT_TRUE(v2[1].getInt(0) <= 5);
    EXPECT_TRUE(v2[2].getInt(0) >= 3);
    EXPECT_TRUE(v2[2].getInt(0) <= 4);
    EXPECT_TRUE(v2[3].getInt(0) >= -1);
    EXPECT_TRUE(v2[3].getInt(0) <= 1);

    auto v3 = context->parallelize({Row(List(Field::empty_dict(), Field::empty_dict(), Field::empty_dict())), Row(List(Field::empty_dict()))}).map(UDF("lambda x: random.choice(x)", "", ce)).collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "({},)");
    EXPECT_EQ(v3[1].toPythonString(), "({},)");

    auto v4 = context->parallelize({Row(List("hello", "world", "!"))}).map(UDF("lambda x: random.choice(x)", "", ce)).collectAsVector();
    ASSERT_EQ(v4.size(), 1);
    auto str = v4[0].getString(0);
    EXPECT_TRUE(str == "hello" || str == "world" || str == "!");
}

TEST_F(UseCaseFunctionsTest, randomStringGeneration) {
    using namespace tuplex;

    ClosureEnvironment ce;
    ce.importModuleAs("random", "random");

    auto v1 = context->parallelize({Row(5), Row(8)})
            .map(UDF("lambda x: ''.join([random.choice('abc') for t in range(x)])", "", ce)).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    std::cout << "*** RESULT: " << v1[0].toPythonString() << std::endl;
    std::cout << "*** RESULT: " << v1[1].toPythonString() << std::endl;
}

TEST(Mini, RyuDistanceConversion) {
    using namespace std;

    // some rows have different distance, that's weird. what is going on?
    double distance = 50.0 / 0.00062137119224;
    char buffer[4096]; memset(buffer, 0, 4096);

    snprintf(buffer, 4096, "%f", distance);
    cout<<"sprintf: "<<buffer<<endl;
    memset(buffer, 0, 4096);
    d2fixed_buffered_n(distance, 6, buffer);
    cout<<"ryu: "<<buffer<<endl;
}

// // @TODO: fix this here...
// TEST_F(UseCaseFunctionsTest, TypingForNullCase) {
//     using namespace tuplex;
//     using namespace std;
//
//     auto fillInTimes_C = "def fillInTimesUDF(row):\n"
//                          "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
//                          "    if row['DivReachedDest']:\n"
//                          "        if float(row['DivReachedDest']) > 0:\n"
//                          "            return float(row['DivActualElapsedTime'])\n"
//                          "        else:\n"
//                          "            return ACTUAL_ELAPSED_TIME\n"
//                          "    else:\n"
//                          "        return ACTUAL_ELAPSED_TIME";
//
//     Context c(microTestOptions());
//     c.parallelize({Row(Field::null(), 34.0, Field::null())}, vector<string>{"ActualElapsedTime",
//                                                                             "DivReachedDest",
//                                                                             "DivActualElapsedTime"}).withColumn("ActualElapsedTime", UDF(fillInTimes_C)).collectAsVector();
//
//
//     // Note: in flights query there's a single row failure for tuplex
//     // i.e. one value error by above function for file 2019_09 --> probably fixing this test should fix the other thing as well
//     // the row ist stored in resources/pipelines/flights/value_error.single-row.csv
//     // => i.e. fix that one
// }

TEST_F(UseCaseFunctionsTest, ZillowResolveFuncs) {
    using namespace tuplex;
    using namespace std;

    // extractBd => check for studio, ignore other rows that don't have that!
    Context c(microTestOptions());

    auto extractBd_c = "def extractBd(x):\n"
                       "    val = x\n"
                       "    max_idx = val.find(' bd')\n"
                       "    if max_idx < 0:\n"
                       "        max_idx = len(val)\n"
                       "    s = val[:max_idx]\n"
                       "\n"
                       "    # find comma before\n"
                       "    split_idx = s.rfind(',')\n"
                       "    if split_idx < 0:\n"
                       "        split_idx = 0\n"
                       "    else:\n"
                       "        split_idx += 2\n"
                       "    r = s[split_idx:]\n"
                       "    return int(r)";

    auto resolveBd_c = "def resolveBd(x):\n"
                       "    if 'Studio' in x:\n"
                       "        return 1\n"
                       "    raise ValueError\n";

    auto res = c.parallelize({Row("Studio , 1 ba , -- sqft"), Row("3 $2,800+")})
     .withColumn("bedrooms", UDF(extractBd_c))
     .resolve(ExceptionCode::VALUEERROR, UDF(resolveBd_c))
     .ignore(ExceptionCode::VALUEERROR)
     .collectAsVector();

    ASSERT_EQ(res.size(), 1);
}

#ifndef BUILD_FOR_CI
TEST_F(UseCaseFunctionsTest, ZillowCacheEachStep) {

    using namespace tuplex;
    using namespace std;

    const std::string zillow_path = "../resources/pipelines/zillow/zillow_noexc.csv";
    // Zillow pipeline is:
    //     # Tuplex pipeline
    //    c.csv(file_path) \
        //     .withColumn("bedrooms", extractBd) \
        //     .filter(lambda x: x['bedrooms'] < 10) \
        //     .withColumn("type", extractType) \
        //     .filter(lambda x: x['type'] == 'house') \
        //     .withColumn("zipcode", lambda x: '%05d' % int(x['postal_code'])) \
        //     .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
        //     .withColumn("bathrooms", extractBa) \
        //     .withColumn("sqft", extractSqft) \
        //     .withColumn("offer", extractOffer) \
        //     .withColumn("price", extractPrice) \
        //     .filter(lambda x: 100000 < x['price'] < 2e7) \
        //     .selectColumns(["url", "zipcode", "address", "city", "state",
    //                     "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
        //     .tocsv("expout.csv")


    auto opt_ref = testOptions();

    opt_ref.set("tuplex.runTimeMemory", "256MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorMemory", "2GB");
    opt_ref.set("tuplex.driverMemory", "2GB");
    opt_ref.set("tuplex.partitionSize", "16MB");
    opt_ref.set("tuplex.executorCount", "4"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "true"); // deactivate
    opt_ref.set("tuplex.optimizer.nullValueOptimization", "true");
    opt_ref.set("tuplex.csv.selectionPushdown", "true"); // disable for now, prob errors later...
    opt_ref.set("tuplex.optimizer.generateParser", "true"); // do not use par => wrong parse for some cell here!
    opt_ref.set("tuplex.inputSplitSize", "64MB"); // probably something wrong with the reader, again??
    Context ctx(opt_ref);

    auto extractBd_c = "def extractBd(x):\n"
                       "    val = x['facts and features']\n"
                       "    max_idx = val.find(' bd')\n"
                       "    if max_idx < 0:\n"
                       "        max_idx = len(val)\n"
                       "    s = val[:max_idx]\n"
                       "\n"
                       "    # find comma before\n"
                       "    split_idx = s.rfind(',')\n"
                       "    if split_idx < 0:\n"
                       "        split_idx = 0\n"
                       "    else:\n"
                       "        split_idx += 2\n"
                       "    r = s[split_idx:]\n"
                       "    return int(r)";
    auto extractType_c = "def extractType(x):\n"
                         "    t = x['title'].lower()\n"
                         "    type = 'unknown'\n"
                         "    if 'condo' in t or 'apartment' in t:\n"
                         "        type = 'condo'\n"
                         "    if 'house' in t:\n"
                         "        type = 'house'\n"
                         "    return type";

    auto extractBa_c = "def extractBa(x):\n"
                       "    val = x['facts and features']\n"
                       "    max_idx = val.find(' ba')\n"
                       "    if max_idx < 0:\n"
                       "        max_idx = len(val)\n"
                       "    s = val[:max_idx]\n"
                       "\n"
                       "    # find comma before\n"
                       "    split_idx = s.rfind(',')\n"
                       "    if split_idx < 0:\n"
                       "        split_idx = 0\n"
                       "    else:\n"
                       "        split_idx += 2\n"
                       "    r = s[split_idx:]\n"
                       "    return int(r)";

    auto extractSqft_c = "def extractSqft(x):\n"
                         "    val = x['facts and features']\n"
                         "    max_idx = val.find(' sqft')\n"
                         "    if max_idx < 0:\n"
                         "        max_idx = len(val)\n"
                         "    s = val[:max_idx]\n"
                         "\n"
                         "    split_idx = s.rfind('ba ,')\n"
                         "    if split_idx < 0:\n"
                         "        split_idx = 0\n"
                         "    else:\n"
                         "        split_idx += 5\n"
                         "    r = s[split_idx:]\n"
                         "    r = r.replace(',', '')\n"
                         "    return int(r)";

    auto extractOffer_c = "def extractOffer(x):\n"
                          "    offer = x['title'].lower()\n"
                          "\n"
                          "    if 'sale' in offer:\n"
                          "        offer = 'sale'\n"
                          "    elif 'rent' in offer:\n"
                          "        offer = 'rent'\n"
                          "    elif 'sold' in offer:\n"
                          "        offer = 'sold'\n"
                          "    elif 'foreclos' in offer.lower():\n"
                          "        offer = 'foreclosed'\n"
                          "    else:\n"
                          "        offer = 'unknown'\n"
                          "\n"
                          "    return offer";

    // this works...
    auto extractPrice_c = "def extractPrice(x):\n"
                          "    price = x['price']\n"
                          "    p = 0\n"
                          "    if x['offer'] == 'sold':\n"
                          "        # price is to be calculated using price/sqft * sqft\n"
                          "        val = x['facts and features']\n"
                          "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
                          "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
                          "        price_per_sqft = int(r)\n"
                          "        p = price_per_sqft * x['sqft']\n"
                          "    elif x['offer'] == 'rent':\n"
                          "        max_idx = price.rfind('/')\n"
                          "        p = int(price[1:max_idx].replace(',', ''))\n"
                          "    else:\n"
                          "        # take price from price column\n"
                          "        p = int(price[1:].replace(',', ''))\n"
                          "\n"
                          "    return p";
    // cache data & select only required columns to simulate pushdown
    vector<string> required_cols{"title", "address", "city", "state",
                                 "postal_code", "price", "facts and features", "url"};
    auto &ds1 = ctx.csv(zillow_path).selectColumns(required_cols).cache()
            .withColumn("bedrooms", UDF(extractBd_c)).cache()
            .filter(UDF("lambda x: x['bedrooms'] < 10")).cache()
            .withColumn("type", UDF(extractType_c)).cache()
            .filter(UDF("lambda x: x['type'] == 'house'")).cache()
            .withColumn("zipcode", UDF("lambda x: '%05d' % int(x['postal_code'])")).cache()
            //.mapColumn("city", UDF("lambda x: x[0].upper() + x[1:].lower()")).cache()
            .withColumn("bathrooms", UDF(extractBa_c)).cache()
            .withColumn("sqft", UDF(extractSqft_c)).cache()
            .withColumn("offer", UDF(extractOffer_c)).cache()
            .withColumn("price", UDF(extractPrice_c)).cache()
            .filter(UDF("lambda x: 100000 < x['price'] < 2e7")).cache()
            .selectColumns(vector<string>{"url", "zipcode", "address", "city", "state",
                                          "bedrooms", "bathrooms", "sqft", "offer",
                                          "type", "price"}).cache();
}
#endif

TEST_F(UseCaseFunctionsTest, PaperExampleCode) {
    using namespace tuplex;
    using namespace std;

    auto env = make_shared<tuplex::codegen::LLVMEnvironment>();

//    UDF udf("lambda m: m * 1.609");
//
//    // hint with null type
//    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)));
//
//    // compile udf
//    udf.compile(*env, true, true);
//
//    // retype UDF with Option[i64]
//    UDF udf2("lambda m: m * 1.609");
//    udf2.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64))));
//    udf2.compile(*env, true, true);


    // slightly altered example
    UDF udfAlt("lambda m: m * 1.609 if m else 0.0");
    udfAlt.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)), true);
    udfAlt.compile(*env);

    udfAlt.removeTypes();
    udfAlt.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64))), true);
    udfAlt.compile(*env);

    udfAlt.removeTypes();
    udfAlt.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::NULLVALUE)), true);
    udfAlt.compile(*env);

    auto& mod = *env->getModule();
    // run cfg-simplification pass to get rid of unnecessary basic blocks
    auto fpm = std::make_unique<llvm::legacy::FunctionPassManager>(&mod);
    assert(fpm.get());
    fpm->add(llvm::createCFGSimplificationPass());
    fpm->add(llvm::createDeadCodeEliminationPass());
    fpm->doInitialization();

    // run function passes over each function in the module
    for(llvm::Function& f: mod.getFunctionList())
        fpm->run(f);

    cout<<env->getIR()<<endl;
}