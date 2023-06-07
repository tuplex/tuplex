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
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"

class StringFunctions : public PyTest {};

TEST_F(StringFunctions, Concat) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                Row(Tuple("hello", "world")),
                Row(Tuple("foo", "bar")),
                Row(Tuple("blank", "")),
                Row(Tuple("", "another")),
                Row(Tuple("", ""))
             }).map(UDF("lambda a, b: a + b")).collectAsVector();

    EXPECT_EQ(v.size(), 5);
    EXPECT_EQ(v[0], std::string("helloworld"));
    EXPECT_EQ(v[1], std::string("foobar"));
    EXPECT_EQ(v[2], std::string("blank"));
    EXPECT_EQ(v[3], std::string("another"));
    EXPECT_EQ(v[4], std::string(""));
}

TEST_F(StringFunctions, Startswith) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                Row(Tuple("hello", "hel")),
                Row(Tuple("foo", "bar")),
                Row(Tuple("blank", "bl")),
                Row(Tuple("", "another")),
                Row(Tuple("", ""))
             }).map(UDF("lambda a, b: a.startswith(b)")).collectAsVector();

    EXPECT_EQ(v.size(), 5);
    EXPECT_EQ(v[0], true);
    EXPECT_EQ(v[1], false);
    EXPECT_EQ(v[2], true);
    EXPECT_EQ(v[3], false);
    EXPECT_EQ(v[4], true);
}

TEST_F(StringFunctions, Endswith) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row(Tuple("hello", "llo")),
                                   Row(Tuple("foo", "bar")),
                                   Row(Tuple("blank", "nk")),
                                   Row(Tuple("", "another")),
                                   Row(Tuple("", ""))
                           }).map(UDF("lambda a, b: a.endswith(b)")).collectAsVector();

    EXPECT_EQ(v.size(), 5);
    EXPECT_EQ(v[0], true);
    EXPECT_EQ(v[1], false);
    EXPECT_EQ(v[2], true);
    EXPECT_EQ(v[3], false);
    EXPECT_EQ(v[4], true);
}

TEST_F(StringFunctions, Index) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row(Tuple("hello", "llo")),
                                   Row(Tuple("foo", "bar")),
                                   Row(Tuple("blank", "nk")),
                                   Row(Tuple("", "another")),
                                   Row(Tuple("", "")),
                                   Row(Tuple("haha", "ha"))
                           })
               .map(UDF("lambda a, b: a.index(b)"))
               .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: -1")).collectAsVector();

    ASSERT_EQ(v.size(), 6);
    EXPECT_EQ(v[0].getInt(0), 2);
    EXPECT_EQ(v[1].getInt(0), -1);
    EXPECT_EQ(v[2].getInt(0), 3);
    EXPECT_EQ(v[3].getInt(0), -1);
    EXPECT_EQ(v[4].getInt(0), 0);
    EXPECT_EQ(v[5].getInt(0), 0);
}

TEST_F(StringFunctions, RIndex) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row(Tuple("hello", "llo")),
                                   Row(Tuple("foo", "bar")),
                                   Row(Tuple("blank", "nk")),
                                   Row(Tuple("", "another")),
                                   Row(Tuple("", "")),
                                   Row(Tuple("haha", "ha")),
                                   Row(Tuple("AaAaA", "AA")),
                                   Row(Tuple("https://github.com/LeonhardFS/Tuplex/pull/74/files", "/")),
                                   Row(Tuple("/usr/local/bin/foo", "/"))
                           }).map(UDF("lambda a, b: a.rindex(b)"))
                                   .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: -1"))
                                   .collectAsVector();

    EXPECT_EQ(v.size(), 9);
    EXPECT_EQ(v[0], 2);
    EXPECT_EQ(v[1], -1);
    EXPECT_EQ(v[2], 3);
    EXPECT_EQ(v[3], -1);
    EXPECT_EQ(v[4], 0);
    EXPECT_EQ(v[5], 2);
    EXPECT_EQ(v[6], -1);
    EXPECT_EQ(v[7], 44);
    EXPECT_EQ(v[8], 14);
}


TEST_F(StringFunctions, IsDecimal) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row("aaa"),
                                   Row("93802820"),
                                   Row("a938aj0"),
                                   Row(""),
                           }).map(UDF("lambda a: a.isdecimal()")).collectAsVector();

    EXPECT_EQ(v.size(), 4);
    EXPECT_EQ(v[0].getBoolean(0), false);
    EXPECT_EQ(v[1].getBoolean(0), true);
    EXPECT_EQ(v[2].getBoolean(0), false);
    EXPECT_EQ(v[3].getBoolean(0), false);
}

/**
 * IsDigit is equivalent to IsDecimal without unicode support.
 */
TEST_F(StringFunctions, IsDigit) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row("aaa"),
                                   Row("93802820"),
                                   Row("a938aj0"),
                                   Row(""),
                           }).map(UDF("lambda a: a.isdigit()")).collectAsVector();

    EXPECT_EQ(v.size(), 4);
    EXPECT_EQ(v[0], false);
    EXPECT_EQ(v[1], true);
    EXPECT_EQ(v[2], false);
    EXPECT_EQ(v[3], false);
}

TEST_F(StringFunctions, IsAlpha) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row("aaa"),
                                   Row("abcdefghijk"),
                                   Row("ab3d"),
                                   Row("sentence!"),
                                   Row(""),
                           }).map(UDF("lambda a: a.isalpha()")).collectAsVector();

    EXPECT_EQ(v.size(), 5);
    EXPECT_EQ(v[0], true);
    EXPECT_EQ(v[1], true);
    EXPECT_EQ(v[2], false);
    EXPECT_EQ(v[3], false);
    EXPECT_EQ(v[4], false);
}

TEST_F(StringFunctions, IsAlNum) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row("abc"),
                                   Row("123"),
                                   Row("abc123"),
                                   Row("abc 123"),
                                   Row(""),
                           }).map(UDF("lambda a: a.isalnum()")).collectAsVector();

    EXPECT_EQ(v.size(), 5);
    EXPECT_EQ(v[0], true);
    EXPECT_EQ(v[1], true);
    EXPECT_EQ(v[2], true);
    EXPECT_EQ(v[3], false);
    EXPECT_EQ(v[4], false);
}

TEST_F(StringFunctions, Count) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({
                                   Row(Tuple("hello", "llo")),
                                   Row(Tuple("foo", "bar")),
                                   Row(Tuple("haha", "ha")),
                                   Row(Tuple("", "another")),
                                   Row(Tuple("bar", "")),
                                   Row(Tuple("aaa", "aa")),
                                   Row(Tuple("aaa", "aaa"))
                           }).map(UDF("lambda a, b: a.count(b)")).collectAsVector();

    EXPECT_EQ(v.size(), 7);
    EXPECT_EQ(v[0], 1);
    EXPECT_EQ(v[1], 0);
    EXPECT_EQ(v[2], 2);
    EXPECT_EQ(v[3], 0);
    EXPECT_EQ(v[4], 0);
    EXPECT_EQ(v[5], 1);
    EXPECT_EQ(v[6], 1);
}

TEST_F(StringFunctions, Replication) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
        Row(Tuple("negative", -2)),
        Row(Tuple("zero", 0)),
        Row(Tuple("hello", 1)),
        Row(Tuple("goodbye", 5)),
    }).map(UDF("lambda a, b: a * b")).collectAsVector();
    auto v2 = c.parallelize({
        Row(Tuple(-2, "negative")),
        Row(Tuple(0, "zero")),
        Row(Tuple(1, "hello")),
        Row(Tuple(6, "foo")),
    }).map(UDF("lambda a, b: a * b")).collectAsVector();
    auto v3 = c.parallelize({
        Row(Tuple(true, "true")),
        Row(Tuple(false, "false")),
    }).map(UDF("lambda a, b: a * b")).collectAsVector();
    auto v4 = c.parallelize({
         Row(Tuple("false", false)),
         Row(Tuple("true", true)),
    }).map(UDF("lambda a, b: a * b")).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], std::string(""));
    EXPECT_EQ(v1[1], std::string(""));
    EXPECT_EQ(v1[2], std::string("hello"));
    EXPECT_EQ(v1[3], std::string("goodbyegoodbyegoodbyegoodbyegoodbye"));

    EXPECT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0], std::string(""));
    EXPECT_EQ(v2[1], std::string(""));
    EXPECT_EQ(v2[2], std::string("hello"));
    EXPECT_EQ(v2[3], std::string("foofoofoofoofoofoo"));

    EXPECT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0], std::string("true"));
    EXPECT_EQ(v3[1], std::string(""));

    EXPECT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0], std::string(""));
    EXPECT_EQ(v4[1], std::string("true"));
}


TEST_F(StringFunctions, SimpleSlice) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
        Row("hello", 0),
        Row("hello", 1),
        Row("hello", 2),
        Row("hello", 3),
        Row("hello", 4),
        Row("hello", 5)
    }).map(UDF("lambda s, i: s[i:]")).collectAsVector();

    EXPECT_EQ(v1.size(), 6);
    EXPECT_EQ(v1[0], std::string("hello"));
    EXPECT_EQ(v1[1], std::string("ello"));
    EXPECT_EQ(v1[2], std::string("llo"));
    EXPECT_EQ(v1[3], std::string("lo"));
    EXPECT_EQ(v1[4], std::string("o"));
    EXPECT_EQ(v1[5], std::string(""));

    auto v2 = c.parallelize({
       Row("hello", 0),
       Row("hello", 1),
       Row("hello", 2),
       Row("hello", 3),
       Row("hello", 4),
       Row("hello", 5)
    }).map(UDF("lambda s, i: s[:i]")).collectAsVector();

    EXPECT_EQ(v2.size(), 6);
    EXPECT_EQ(v2[0], std::string(""));
    EXPECT_EQ(v2[1], std::string("h"));
    EXPECT_EQ(v2[2], std::string("he"));
    EXPECT_EQ(v2[3], std::string("hel"));
    EXPECT_EQ(v2[4], std::string("hell"));
    EXPECT_EQ(v2[5], std::string("hello"));

    auto v3 = c.parallelize({
        Row("hello", 0, 3, 1),
        Row("hello", 1, 4, 1),
        Row("hello", 2, 6, 2),
        Row("hello", 1, 6, 3),
        Row("hello", 5, 1, -1),
        Row("hello", 6, 2, -2),
        Row("hello", 6, -2, -3),
        Row("hello", 6, -1, -3),
        Row("hello", 6, -8, -3),
        Row("hello", 6, -2, 0)
    }).map(UDF("lambda s, m, n, p: s[m:n:p]")).resolve(ExceptionCode::VALUEERROR, UDF("lambda x: ''")).collectAsVector();

    EXPECT_EQ(v3.size(), 10);
    EXPECT_EQ(v3[0], std::string("hel"));
    EXPECT_EQ(v3[1], std::string("ell"));
    EXPECT_EQ(v3[2], std::string("lo"));
    EXPECT_EQ(v3[3], std::string("eo"));
    EXPECT_EQ(v3[4], std::string("oll"));
    EXPECT_EQ(v3[5], std::string("o"));
    EXPECT_EQ(v3[6], std::string("o"));
    EXPECT_EQ(v3[7], std::string(""));
    EXPECT_EQ(v3[8], std::string("oe"));
    EXPECT_EQ(v3[9], std::string(""));
}

TEST_F(StringFunctions, OldStyleFormat) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
        Row("%d %s", 5, "hi"),
        Row("%d bye %s", 10, "goodbye"),
        Row("%x = %s in hex", 12, "12"),
        Row("%X = %s in upper hex", 12, "12")
    }).map(UDF("lambda s, x, y: s % (x, y)")).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], std::string("5 hi"));
    EXPECT_EQ(v1[1], std::string("10 bye goodbye"));
    EXPECT_EQ(v1[2], std::string("c = 12 in hex"));
    EXPECT_EQ(v1[3], std::string("C = 12 in upper hex"));

// %<(+/-)n>s, %.<n>s, %d, %f, %.<n>f, %x/X (lower/upper hex)
    auto v2 = c.parallelize({
        Row("%.*f", 5, 5.123456789),
        Row("%*.f", 3, 6.987654321),
        Row("%3.*f", 3, 6.987654321),
        Row("%8.*f", 3, 6.987654321)
    }).map(UDF("lambda s, x, y: s % (x, y)")).collectAsVector();
    EXPECT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0], std::string("5.12346"));
    EXPECT_EQ(v2[1], std::string("  7"));
    EXPECT_EQ(v2[2], std::string("6.988"));
    EXPECT_EQ(v2[3], std::string("   6.988"));

    auto v3 = c.parallelize({
        Row("%f %.*s", 2.63, 5, "goodbye"),
        Row("%.2f hi %*.s", 1.23, 3, "goodbye"),
        Row("hi %.2f %3.*s", 3.72,  3, "goodbye"),
        Row("hola %f %8.*s", 2.6, 3, "goodbye")
    }).map(UDF("lambda s, x, y, z: s % (x, y, z)")).collectAsVector();
    EXPECT_EQ(v3.size(), 4);
    EXPECT_EQ(v3[0], std::string("2.630000 goodb"));
    EXPECT_EQ(v3[1], std::string("1.23 hi    "));
    EXPECT_EQ(v3[2], std::string("hi 3.72 goo"));
    EXPECT_EQ(v3[3], std::string("hola 2.600000      goo"));

    auto v4 = c.parallelize({
        Row("hi")
    }).map(UDF("lambda s: s % ()")).collectAsVector();
    EXPECT_EQ(v4.size(), 1);
    EXPECT_EQ(v4[0], std::string("hi"));
}


TEST_F(StringFunctions, Capwords) {

    // NOTE: This is a hacky test. It's only good for testing, later correct python syntax needs to be used!!!

    using namespace tuplex;
    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto v1 = c.parallelize({Row(" \t  there's a nEEd to caPitalize  \tevery Word\t\t"),
                             Row(" \t  a b  \tc"),
                             Row("AB C   \t\t\t\t"),
                             Row("")}).map(UDF("lambda s: string.capwords(s)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], std::string("There's A Need To Capitalize Every Word"));
    EXPECT_EQ(v1[1], std::string("A B C"));
    EXPECT_EQ(v1[2], std::string("Ab C"));
    EXPECT_EQ(v1[3], std::string(""));
}

TEST_F(StringFunctions, StrJoin) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
        Row(List("a", "b", "c")),
        Row(List("12", "3", "456")),
        Row(List("xyz", "", "abcde"))
        }).map(UDF("lambda x: ', '.join(x)")).collectAsVector();

    EXPECT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0], std::string("a, b, c"));
    EXPECT_EQ(v1[1], std::string("12, 3, 456"));
    EXPECT_EQ(v1[2], std::string("xyz, , abcde"));

    auto v2 = c.parallelize({
            Row("a", "b", "c"),
            Row("12", "3", "456"),
            Row("xyz", "", "abcde")
    }).map(UDF("lambda x, y, z: ''.join([x, y, z])")).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0], std::string("abc"));
    EXPECT_EQ(v2[1], std::string("123456"));
    EXPECT_EQ(v2[2], std::string("xyzabcde"));
}

TEST_F(StringFunctions, StrSplit) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
            Row("hello world! this is Tuplex.", " "),
            Row("this, is, a, comma, separated, list", ", "),
            Row("one more for good measure", "e "),
            Row("no matches", "x")
    }).map(UDF("lambda x, y: x.split(y)")).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0].toPythonString(), std::string("(['hello','world!','this','is','Tuplex.'],)"));
    EXPECT_EQ(v1[1].toPythonString(), std::string("(['this','is','a','comma','separated','list'],)"));
    EXPECT_EQ(v1[2].toPythonString(), std::string("(['on','mor','for good measure'],)"));
    EXPECT_EQ(v1[3].toPythonString(), std::string("(['no matches'],)"));
}

TEST_F(StringFunctions, StrStrip) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
        Row("hello", "hlo"),
        Row("abceeeeeedr", "acedr"),
        Row("lmnop", "p"),
        Row("lmnop", "lm")
    }).map(UDF("lambda x, y: x.strip(y)")).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], std::string("e"));
    EXPECT_EQ(v1[1], std::string("b"));
    EXPECT_EQ(v1[2], std::string("lmno"));
    EXPECT_EQ(v1[3], std::string("nop"));

    auto v2 = c.parallelize({
        Row(" \n\t\r hello \n\r\r\r  "),
        Row("  \t\r\x0b"),
        Row("goodbye!\n\n\n"),
        Row(" \r\tabcde")
    }).map(UDF("lambda x: x.strip()")).collectAsVector();

    EXPECT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0], std::string("hello"));
    EXPECT_EQ(v2[1], std::string(""));
    EXPECT_EQ(v2[2], std::string("goodbye!"));
    EXPECT_EQ(v2[3], std::string("abcde"));
}

TEST_F(StringFunctions, StrSwapcase) {
    using namespace tuplex;
    Context c(microTestOptions());

    std::string longString = std::string(80, 'Q') + std::string(10, 'p') + std::string(60, '\t')
            + std::string(15, ' ') + std::string("**!!") +  std::string(40, 'Z') + std::string("longSTRING");
    const char *ls = longString.c_str();
    std::string longStringSwap = std::string("('") + std::string(80, 'q') + std::string(10, 'P')
            + std::string(60, '\t') + std::string(15, ' ') + std::string("**!!")
            + std::string(40, 'z') + std::string("LONGstring") + std::string("',)");

    auto v = c.parallelize({
        Row("ALL UPPER"),
        Row("all lower"),
        Row("uPPER & lOWER"),
        Row("with #Number 12.34%"),
        Row(""),
        Row(ls)
    }).map(UDF("lambda x: x.swapcase()")).collectAsVector();

    EXPECT_EQ(v.size(), 6);
    EXPECT_EQ(v[0].toPythonString(), std::string("('all upper',)"));
    EXPECT_EQ(v[1].toPythonString(), std::string("('ALL LOWER',)"));
    EXPECT_EQ(v[2].toPythonString(), std::string("('Upper & Lower',)"));
    EXPECT_EQ(v[3].toPythonString(), std::string("('WITH #nUMBER 12.34%',)"));
    EXPECT_EQ(v[4].toPythonString(), std::string("('',)"));
    EXPECT_EQ(v[5].toPythonString(), longStringSwap);
}

TEST_F(StringFunctions, StrCenter) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({
        Row(""),
        Row("a"),
        Row("ab"),
        Row("abc"),
        Row("abcd"),
    }).map(UDF("lambda x: x.center(3)")).collectAsVector();

    EXPECT_EQ(v1.size(), 5);
    EXPECT_EQ(v1[0], std::string("   "));
    EXPECT_EQ(v1[1], std::string(" a "));
    EXPECT_EQ(v1[2], std::string(" ab"));
    EXPECT_EQ(v1[3], std::string("abc"));
    EXPECT_EQ(v1[4], std::string("abcd"));

    auto v2 = c.parallelize({
        Row("", "|"),
        Row("a", ","),
        Row("ab", "+"),
        Row("abc", "2"),
        Row("abcd", "%"),
        Row("abcde", "*"),
    }).map(UDF("lambda x, y: x.center(4, y)")).collectAsVector();

    EXPECT_EQ(v2.size(), 6);
    EXPECT_EQ(v2[0], std::string("||||"));
    EXPECT_EQ(v2[1], std::string(",a,,"));
    EXPECT_EQ(v2[2], std::string("+ab+"));
    EXPECT_EQ(v2[3], std::string("abc2"));
    EXPECT_EQ(v2[4], std::string("abcd"));
    EXPECT_EQ(v2[5], std::string("abcde"));
}