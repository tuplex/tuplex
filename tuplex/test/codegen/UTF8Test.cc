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
#include <UTF8.h>

TEST(UTF8Helpers, ASCIICompatibility) {
    EXPECT_EQ(static_cast<char>(utf8toi("a")), 'a');

    EXPECT_EQ(static_cast<char>(utf8toi("0")), '0');
    EXPECT_EQ(static_cast<char>(utf8toi("1")), '1');
    EXPECT_EQ(static_cast<char>(utf8toi("2")), '2');
    EXPECT_EQ(static_cast<char>(utf8toi("3")), '3');
    EXPECT_EQ(static_cast<char>(utf8toi("4")), '4');
    EXPECT_EQ(static_cast<char>(utf8toi("5")), '5');
    EXPECT_EQ(static_cast<char>(utf8toi("6")), '6');
    EXPECT_EQ(static_cast<char>(utf8toi("7")), '7');
    EXPECT_EQ(static_cast<char>(utf8toi("8")), '8');
    EXPECT_EQ(static_cast<char>(utf8toi("9")), '9');
}

TEST(UTF8Helpers, NonAscii) {

    for(int i = 0; i < 256; i++) {
        unsigned char c = static_cast<char>(i & 0xFF);
        EXPECT_EQ(isNonASCII(c), i >= 128);
    }
}

TEST(UTF8Helpers, ByteCount) {
    // [0b0000 0000 - 0b1000 0000 )
    for(int i = 0; i < 128; i++) {
        unsigned char c = static_cast<char>(i & 0xFF);
        EXPECT_EQ(utf8ByteCount(c), 1);
    }
    // [0b1100 0000 - 0b1110 0000 )
    for(int i = 0xC0; i < 0xDF; i++) {
        unsigned char c = static_cast<char>(i & 0xFF);
        EXPECT_EQ(utf8ByteCount(c), 2);
    }
    // [0b1110 0000 - 0b1111 0000 )
    for(int i = 0xE0; i < 0xF0; i++) {
        unsigned char c = static_cast<char>(i & 0xFF);
        EXPECT_EQ(utf8ByteCount(c), 3);
    }
    // [0b1111 0000 - 0b1111 0111 ]
    for(int i = 0xF0; i < 0xF7; i++) {
        unsigned char c = static_cast<char>(i & 0xFF);
        EXPECT_EQ(utf8ByteCount(c), 4);
    }
}

TEST(UTF8Helpers, SimpleConversion) {
    // 1 char encoding
    int c = 'a';
    EXPECT_EQ(utf8toi("a"), c);

    // 2 char encoding
    unsigned char c2[3] = {0xC0, 0x80, 0x0};
    EXPECT_EQ(utf8toi(reinterpret_cast<const char*>(c2)), 0xC080);

    // 3 char encoding
    unsigned char c3[4] = {0xE2, 0x81, 0x82, 0x0};
    EXPECT_EQ(utf8toi(reinterpret_cast<const char*>(c3)), 0xE28182);

    // 4 char encoding
    unsigned char c4[5] = {0xF7, 0x81, 0x82, 0x83, 0x0};
    EXPECT_EQ(utf8toi(reinterpret_cast<const char*>(c4)), 0xF7818283);
}

TEST(UTF8Helpers, Invariant) {
    std::cout<<utf8toi("X")<<std::endl;

    EXPECT_EQ(utf8itostr(utf8toi("X")), "X");

    EXPECT_EQ(utf8toi(utf8itostr(0xF7818283).c_str()), 0xF7818283);
}