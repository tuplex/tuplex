//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <thread>
#include <random>
#include "../../runtime/include/Runtime.h"
#include <algorithm>
#include <cctype>
#include <string>


TEST(Runtime, malloc) {
    setRunTimeMemory(1024 * 1024 * 10, 0); // use 10MB

    // allocate 575x 1kb of memory
    for(int i = 0; i < 575; ++i) {
        void *ptr = rtmalloc(1024);
        EXPECT_TRUE(ptr != nullptr);

    }

    rtfree_all();

    freeRunTimeMemory();
}

int intRand(const int & min, const int & max) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
}

TEST(Runtime, MultithreadedMallocAndFree) {
    using namespace std;
    int num_threads = 40;
    int num_allocs = 1000;
    int alloc_size = 5000; // 2 pages
    int rtSize = 1024 * 1024 * 32; // 32MB each
    vector<std::thread> threads;


    cout<<"starting threads"<<endl;
    for(int i = 0; i < num_threads; ++i) {
        threads.emplace_back(std::thread([&](int thread_id) {
            setRunTimeMemory(rtSize, 0);

            sleep(intRand(0, 10));

            for(int j = 0; j < num_allocs; ++j) {
                void *ptr = rtmalloc(alloc_size);

                // This forces the value to never be optimized away
                // by taking and using it.
                if((uint64_t)ptr & 0x1)
                    rtfree_all();

                if(j == 100) {
                    rtfree_all();
                }
            }

            sleep(intRand(0, 10));
            freeRunTimeMemory();
        }, i));
    }

    for(auto& t : threads)
        t.join();
    cout<<"done"<<endl;
}

TEST(Runtime, strSwapcase) {
    setRunTimeMemory(1020 * 1020 * 10, 0); // use 10MB

    auto res1 = strSwapcase("Test Swapcase 1 2", sizeof("Test Swapcase 1 2"));
    EXPECT_EQ(std::string(res1), std::string("tEST sWAPCASE 1 2"));

    auto res2 = strSwapcase("AA.bb cc,DD !", sizeof("AA.bb cc,DD !"));
    EXPECT_EQ(std::string(res2), std::string("aa.BB CC,dd !"));

    auto res3 = strSwapcase("", sizeof(""));
    EXPECT_EQ(std::string(res3), std::string(""));

    // test long string
    std::string longString = std::string(40, 'a') + std::string(10, '\t') + std::string(60, 'B')
            + std::string(15, ' ') + std::string("**!!") +  std::string(40, 'Z') + std::string("longSTRING");
    std::string longStringSwap = std::string(40, 'A') + std::string(10, '\t') + std::string(60, 'b')
            + std::string(15, ' ') + std::string("**!!") +  std::string(40, 'z') + std::string("LONGstring");
    const char *ls = longString.c_str();
    auto res4 = strSwapcase(ls, strlen(ls) + 1);
    EXPECT_EQ(std::string(res4), longStringSwap);

    rtfree_all();
    freeRunTimeMemory();
}

TEST(Runtime, strReplace) {
    setRunTimeMemory(1024 * 1024 * 10, 0); // use 10MB

    int64_t new_size = 0;
    auto res = strReplace("what a wonderful globe", "globe", "world", &new_size);
    EXPECT_EQ(std::string(res), std::string("what a wonderful world"));
    EXPECT_EQ(new_size, strlen("what a wonderful world") + 1);


    auto res2 = strReplace("this,is,a,boring,list", ",", "", &new_size);
    EXPECT_EQ(std::string(res2), std::string("thisisaboringlist"));
    EXPECT_EQ(new_size, strlen("thisisaboringlist") + 1);

    rtfree_all();
    freeRunTimeMemory();
}

TEST(Runtime, strFormat) {
    setRunTimeMemory(1024 * 1024 * 10, 0); // use 10MB

    // notice in this test how to use ll suffix to make int64_t integers!
    // also bools are encoded as 64bit integers! because that's how the compiler does it internally...

    int64_t new_size1 = 0;
    auto res1 = strFormat("{} {} {} {} {}", &new_size1, "ddsss", 12ll, 42ll, "test", "string", "formatting");
    auto expect_res1 = std::string("12 42 test string formatting");
    EXPECT_EQ(res1, expect_res1);
    EXPECT_EQ(new_size1, expect_res1.size() + 1);

    // long test
    int64_t new_size2 = 0;
    auto res2 = strFormat("{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}", &new_size2,
            "sfbdsfbdsfbdsfbd", "abc", 12.29, 1ll, 100ll,
            "efg", 1.12, 0ll, 101ll,
            "ijk", 13.33, 1ll, 102ll,
            "mno", 2.255, 0ll, 104ll);
    auto expect_res2 = std::string("abc 12.29 True 100 efg 1.12 False 101 ijk 13.33 True 102 mno 2.255 False 104");
    EXPECT_EQ(res2, expect_res2);
    EXPECT_EQ(new_size2, expect_res2.size() + 1);

    // empty test
    int64_t new_size3 = 0;
    auto res3 = strFormat("hello", &new_size3, "");
    auto expect_res3 = std::string("hello");
    EXPECT_EQ(res3, expect_res3);
    EXPECT_EQ(new_size3, expect_res3.size() + 1);

    // 0 decimal part test
    int64_t new_size4 = 0;
    auto res4 = strFormat("{} {} {}", &new_size4, "fff", 1.1, 1.0, 0.0);
    auto expect_res4 = std::string("1.1 1.0 0.0");
    EXPECT_EQ(res4, expect_res4);
    EXPECT_EQ(new_size4, expect_res4.size() + 1);

    rtfree_all();
    freeRunTimeMemory();
}

// Note: if compiled with -Ofast, the compiler will disable INF/NAN support in order
// to support faster math. This breaks this test.
TEST(Runtime, strFormatFloatSpecialVals) {
    // special float values
    setRunTimeMemory(1024 * 1024 * 10, 0); // use 10MB

    int64_t sizeNaN = 0;
    auto resNaN = strFormat("{}", &sizeNaN, "f", std::numeric_limits<double>::quiet_NaN());
    auto expectNaN = std::string("nan");

    EXPECT_EQ(std::string(resNaN), expectNaN);

    int64_t sizeSNaN = 0;
    auto resSNaN = strFormat("{}", &sizeSNaN, "f", std::numeric_limits<double>::signaling_NaN());
    auto expectSNaN = std::string("nan");

    EXPECT_EQ(std::string(resSNaN), expectSNaN);

    int64_t sizeInf = 0;
    auto resInf = strFormat("{}", &sizeInf, "f", std::numeric_limits<double>::infinity());
    auto expectInf = std::string("inf");

    EXPECT_EQ(std::string(resInf), expectInf);

    int64_t sizeNegInf = 0;
    auto resNegInf = strFormat("{}", &sizeNegInf, "f", -1.0 * std::numeric_limits<double>::infinity());
    auto expectNegInf = std::string("-inf");

    EXPECT_EQ(std::string(resNegInf), expectNegInf);

    // 0 decimal part test
    int64_t new_size4 = 0;
    auto res4 = strFormat("{} {} {}", &new_size4, "fff", 1.1, 1.0, 0.0);
    auto expect_res4 = std::string("1.1 1.0 0.0");
    EXPECT_EQ(res4, expect_res4);
    EXPECT_EQ(new_size4, expect_res4.size() + 1);

    rtfree_all();
    freeRunTimeMemory();
}

TEST(Runtime, csvquote) {
    setRunTimeMemory(1024 * 1024, 0); // use 1MB

    EXPECT_EQ(std::string(quoteForCSV("", 1, nullptr, ',', '"')), "");
    EXPECT_EQ(std::string(quoteForCSV("hello", 6, nullptr, ',', '"')), "hello");
    EXPECT_EQ(std::string(quoteForCSV(",,,,", 5, nullptr, ',', '"')), "\",,,,\"");
    EXPECT_EQ(std::string(quoteForCSV("\n\r", 3, nullptr, ',', '"')), "\"\n\r\"");
    EXPECT_EQ(std::string(quoteForCSV("\"", 2, nullptr, ',', '"')), "\"\"\"\"");
    EXPECT_EQ(std::string(quoteForCSV("\"\"", 3, nullptr, ',', '"')), "\"\"\"\"\"\"");
    EXPECT_EQ(std::string(quoteForCSV(",\"a\"\n", 6, nullptr, ',', '"')), "\",\"\"a\"\"\n\"");

}

// capitalizing words in python
// two options: import string; string.capwords(...) or ''.title()

// Note:
// "there's a need to capitalize every word".title() ==> "There'S A Need To Capitalize Every Word"
// string.capwords("there's a need to capitalize every word") ==> "There's A Need To Capitalize Every Word"

// original python title implementation
// static Py_ssize_t
//do_title(int kind, void *data, Py_ssize_t length, Py_UCS4 *res, Py_UCS4 *maxchar)
//{
//    Py_ssize_t i, k = 0;
//    int previous_is_cased;
//
//    previous_is_cased = 0;
//    for (i = 0; i < length; i++) {
//        const Py_UCS4 c = PyUnicode_READ(kind, data, i);
//        Py_UCS4 mapped[3];
//        int n_res, j;
//
//        if (previous_is_cased)
//            n_res = lower_ucs4(kind, data, length, i, c, mapped);
//        else
//            n_res = _PyUnicode_ToTitleFull(c, mapped);
//
//        for (j = 0; j < n_res; j++) {
//            *maxchar = Py_MAX(*maxchar, mapped[j]);
//            res[k++] = mapped[j];
//        }
//
//        previous_is_cased = _PyUnicode_IsCased(c);
//    }
//    return k;
//}

// check out https://github.com/python/cpython/blob/master/Objects/unicodeobject.c

// @TODO: correctly fix importing etc. for compilation, for now quick hack to get things working...
TEST(Runtime, Capwords) {
    using namespace std;
    setRunTimeMemory(1024 * 1024, 0); // use 1MB

    int64_t res_size = 0;
    string test_strI(" \t  there's a nEEd to caPitalize  \tevery Word\t\t");
    EXPECT_EQ(std::string(stringCapwords(test_strI.c_str(), test_strI.length() + 1, &res_size)), "There's A Need To Capitalize Every Word");
    EXPECT_EQ(strlen("There's A Need To Capitalize Every Word") + 1, res_size);

    string test_strII(" \t  a b  \tc");
    EXPECT_EQ(std::string(stringCapwords(test_strII.c_str(), test_strII.length() + 1, &res_size)), "A B C");
    EXPECT_EQ(strlen("A B C") + 1, res_size);

    string test_strIII("AB C   \t\t\t\t");
    EXPECT_EQ(std::string(stringCapwords(test_strIII.c_str(), test_strIII.length() + 1, &res_size)), "Ab C");
    EXPECT_EQ(strlen("AB C") + 1, res_size);

    string test_strIV("");
    EXPECT_EQ(std::string(stringCapwords(test_strIV.c_str(), test_strIV.length() + 1, &res_size)), "");
    EXPECT_EQ(strlen("") + 1, res_size);
}

TEST(Runtime, strCenter) {
    using namespace std;
    setRunTimeMemory(1024 * 1024, 0); // use 1MB

    int64_t res_size;

    ////////////////////////////////////////
    // Case: test when input width <= input string length
    ////////////////////////////////////////

    // 'a'.center(0) -> 'a'
    res_size = 1;
    string test_str1("a");
    auto res1 = strCenter(test_str1.c_str(), test_str1.length() + 1, 0, &res_size, ' ');
    EXPECT_EQ(res1, std::string("a"));

    // 'a'.center(1) -> 'a'
    res_size = 1;
    string test_str2("a");
    auto res2 = strCenter(test_str2.c_str(), test_str2.length() + 1, 1, &res_size, ' ');
    EXPECT_EQ(res2, std::string("a"));


    // ''.center(0) -> ''
    res_size = 1;
    string test_str3("");
    auto res3 = strCenter(test_str3.c_str(), test_str3.length() + 1, 0, &res_size, ' ');
    EXPECT_EQ(res3, std::string(""));

    ////////////////////////////////////////
    // Case: input width and string length have same parity
    ////////////////////////////////////////
    
    // 'a'.center(3) -> ' a '
    res_size = 3;
    string test_str4("a");
    auto res4 = strCenter(test_str4.c_str(), test_str4.length() + 1, 3, &res_size, ' ');
    EXPECT_EQ(res4, std::string(" a "));

    // 'a'.center(3, '|') -> '|a|'
    res_size = 3;
    string test_str5("a");
    auto res5 = strCenter(test_str5.c_str(), test_str5.length() + 1, 3, &res_size, '|');
    EXPECT_EQ(res5, std::string("|a|"));

    // 'a'.center(5, '|') -> '||a||'
    res_size = 5;
    string test_str6("a");
    auto res6 = strCenter(test_str6.c_str(), test_str6.length() + 1, 5, &res_size, '|');
    EXPECT_EQ(res6, std::string("||a||"));

    // 'ab'.center(4, '|') -> '|ab|'
    res_size = 4;
    string test_str7("ab");
    auto res7 = strCenter(test_str7.c_str(), test_str7.length() + 1, 4, &res_size, '|');
    EXPECT_EQ(res7, std::string("|ab|"));

    // 'ab'.center(6, '|') -> '||ab||'
    res_size = 6;
    string test_str8("ab");
    auto res8 = strCenter(test_str8.c_str(), test_str8.length() + 1, 6, &res_size, '|');
    EXPECT_EQ(res8, std::string("||ab||"));

    ////////////////////////////////////////
    // Case: input width and string length have different parity
    ////////////////////////////////////////

    // 'ab'.center(3, '|') -> '|ab'
    res_size = 3;
    string test_str9("ab");
    auto res9 = strCenter(test_str9.c_str(), test_str9.length() + 1, 3, &res_size, '|');
    EXPECT_EQ(res9, std::string("|ab"));

    // 'ab'.center(5, '|') -> '||ab|'
    res_size = 5;
    string test_str10("ab");
    auto res10 = strCenter(test_str10.c_str(), test_str10.length() + 1, 5, &res_size, '|');
    EXPECT_EQ(res10, std::string("||ab|"));

    // 'abc'.center(4, '|') -> 'abc|'
    res_size = 4;
    string test_str11("abc");
    auto res11 = strCenter(test_str11.c_str(), test_str11.length() + 1, 4, &res_size, '|');
    EXPECT_EQ(res11, std::string("abc|"));

    // 'abc'.center(6, '|') -> '|abc||'
    res_size = 6;
    string test_str12("abc");
    auto res12 = strCenter(test_str12.c_str(), test_str12.length() + 1, 6, &res_size, '|');
    EXPECT_EQ(res12, std::string("|abc||"));

    ////////////////////////////////////////
    // Case: empty string input
    ////////////////////////////////////////

    // ''.center(6, '|') -> '||||||'
    res_size = 6;
    string test_str13("");
    auto res13 = strCenter(test_str13.c_str(), test_str13.length() + 1, 6, &res_size, '|');
    EXPECT_EQ(res13, std::string("||||||"));

    // ''.center(1, '|') -> '|'
    res_size = 1;
    string test_str14("");
    auto res14 = strCenter(test_str14.c_str(), test_str14.length() + 1, 1, &res_size, '|');
    EXPECT_EQ(res14, std::string("|"));

    // ''.center(0, '|') -> ''
    res_size = 0;
    string test_str15("");
    auto res15 = strCenter(test_str15.c_str(), test_str15.length() + 1, 0, &res_size, '|');
    EXPECT_EQ(res15, std::string(""));

    rtfree_all();
    freeRunTimeMemory();
}

std::string gen_random_string(int length) {
    assert(length >= 1);
    std::string s(length, ' ');
    // 33 - 126 incl.
    std::default_random_engine gen;
    std::uniform_int_distribution<char> u_dist(33,126);
    for(int i = 0; i < length; ++i)
        s[i] = u_dist(gen);
    s[length - 1] = 0;
    return s;
}


// does not work in CI, probably misaligned
#ifndef BUIlD_FOR_CI

//TEST(Runtime, strLowerSIMD) {
//    using namespace std;
//    setRunTimeMemory(1024 * 1024, 0); // use 32MB
//
//    // empty string test
//    EXPECT_EQ(std::string(strLowerSIMD("", 1)), std::string(""));
//
//    // test through full 7 bit chars, repeated string lengths from 1 to 512
//    for(int c = 0; c < 128; ++c) {
//        for(int L = 1; L <= 768; ++L) {
//            auto str = new char[L];
//            for(int i = 0; i < L; ++i)
//                str[i] = c;
//            str[L - 1] = '\0';
//
//            // test
//            std::string ref(str);
//            std::transform(ref.begin(), ref.end(), ref.begin(),
//                                 [](char c){ return std::tolower(c); });
//            EXPECT_EQ(std::string(strLowerSIMD(str, L)), std::string(ref.c_str()));
//            delete [] str;
//            freeRunTimeMemory();
//        }
//    }
//
//    // randomized testing using random strings from printable range
//    int N_per_trial = 10;
//    for(int L = 1; L <= 768; ++L) {
//        for(int i = 0; i < N_per_trial; ++i) {
//            std::string str = gen_random_string(L);
//            std::string ref(str);
//            std::transform(ref.begin(), ref.end(), ref.begin(),
//                           [](char c){ return std::tolower(c); });
//            EXPECT_EQ(std::string(strLowerSIMD(str.c_str(), L)), std::string(ref.c_str()));
//            freeRunTimeMemory();
//        }
//    }
//}

#endif