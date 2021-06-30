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
#include <StringUtils.h>
#include <CSVUtils.h>
#include <third_party/i64toa_sse2.h>

TEST(StringUtils, fromCharPointers) {
    using namespace tuplex;

    std::string test = "Hello world!";
    auto ptr = test.c_str();
    EXPECT_EQ(fromCharPointers(ptr, ptr + test.length()), test);
    EXPECT_EQ(fromCharPointers(ptr, ptr), "");
    EXPECT_EQ(fromCharPointers(ptr, ptr + 5), "Hello");
}

TEST(StringUtils, IntegerRecognition) {
    using namespace tuplex;

    EXPECT_FALSE(isIntegerString("100-200"));

    EXPECT_TRUE(isIntegerString("0"));
    EXPECT_TRUE(isIntegerString("0234"));
    EXPECT_TRUE(isIntegerString("-20"));
    EXPECT_TRUE(isIntegerString("42"));
    EXPECT_TRUE(isIntegerString("99999999"));
    EXPECT_TRUE(isIntegerString(""));
    EXPECT_FALSE(isIntegerString("10.5"));
    EXPECT_FALSE(isIntegerString(".4"));
    EXPECT_FALSE(isIntegerString("Hello world"));
    EXPECT_FALSE(isIntegerString("10e-6"));
    EXPECT_FALSE(isIntegerString("--10"));
}

TEST(StringUtils, FloatRecognition) {
    using namespace tuplex;
    EXPECT_FALSE(isFloatString("0-30"));
    EXPECT_TRUE(isFloatString("0"));
    EXPECT_TRUE(isFloatString("0234"));
    EXPECT_TRUE(isFloatString("-20"));
    EXPECT_TRUE(isFloatString("42"));
    EXPECT_TRUE(isFloatString("99999999"));
    EXPECT_TRUE(isFloatString(""));
    EXPECT_TRUE(isFloatString("10.5"));
    EXPECT_TRUE(isFloatString(".4"));
    EXPECT_TRUE(isFloatString(".4e-30"));
    EXPECT_TRUE(isFloatString(".4E90"));
    EXPECT_FALSE(isFloatString("..4"));
    EXPECT_FALSE(isIntegerString("3.141e9e20"));
    EXPECT_FALSE(isFloatString("Hello world"));
}

TEST(StringUtils, ReplaceLineBreaks) {
    using namespace tuplex;
    EXPECT_EQ(replaceLineBreaks("Hello\nWorld\n!"), "Hello\\nWorld\\n!");
    EXPECT_EQ(replaceLineBreaks("nobreaks"), "nobreaks");
    EXPECT_EQ(replaceLineBreaks(""), "");
    EXPECT_EQ(replaceLineBreaks("\n"), "\\n");
}

TEST(StringUtils, LengthWithLineBreaks) {
    using namespace tuplex;
    EXPECT_EQ(getMaxLineLength("\n\n"), 0);
    EXPECT_EQ(getMaxLineLength("Hello\nwonderful World\n!"), strlen("wonderful World"));
    EXPECT_EQ(getMaxLineLength("hello world"), strlen("hello world"));
}

TEST(StringUtils, FastDequote) {
    using namespace tuplex;

    EXPECT_EQ(dequote("\"\""), "\"");
    EXPECT_EQ(dequote("\"\"\"\""), "\"\"");
    EXPECT_EQ(dequote("\"\"\"\"\"\""), "\"\"\"");
    EXPECT_EQ(dequote(" \"\""), " \"");
    EXPECT_EQ(dequote(" \"\"\"\""), " \"\"");
    EXPECT_EQ(dequote(" \"\"\"\"\"\""), " \"\"\"");
    EXPECT_EQ(dequote("\"\" "), "\" ");
    EXPECT_EQ(dequote("\"\"\"\" "), "\"\" ");
    EXPECT_EQ(dequote("\"\"\"\"\"\" "), "\"\"\" ");
    EXPECT_EQ(dequote("nothing"), "nothing");
    EXPECT_EQ(dequote("\"\" hello \"\""), "\" hello \"");
    EXPECT_EQ(dequote("speaking in \"\" is stupid"), "speaking in \" is stupid");
    EXPECT_EQ(dequote("test\"\" this"), "test\" this");
}


TEST(CSVUtils, newCSVLineStart) {
    using namespace std;
    using namespace tuplex;

    // new test from real data
    string test_strX = "\"SOLD: $350,000\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"
                       "For Sale by Owner,11 Watson St,SOMERVILLE,MA,2144.0,\"$1,"; // 10 columns
    // start searching at O, i.e. within quoted field
    // => first field on next row is unquoted
    auto infoX = findLineStart(test_strX.c_str(), test_strX.length() + 1, 3, 10);
    EXPECT_TRUE(infoX.valid);
    EXPECT_EQ(infoX.offset, strlen("LD: $350,000\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"));

    // => first field on next row is quoted
    string test_strXI = "\"SOLD: $350,000\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"
                       "\"For Sale by Owner\",11 Watson St,SOMERVILLE,MA,2144.0,\"$1,"; // 10 columns
    // start searching at O, i.e. within quoted field
    auto infoXI = findLineStart(test_strXI.c_str(), test_strXI.length() + 1, 3, 10);
    EXPECT_TRUE(infoX.valid);
    EXPECT_EQ(infoX.offset, strlen("LD: $350,000\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"));

    // => fist char encountered is closing char of quoted field
    // should work as well
    string test_strXII = "\"SOLD: $350,000\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"
                       "For Sale by Owner,11 Watson St,SOMERVILLE,MA,2144.0,\"$1,"; // 10 columns
    auto infoXII = findLineStart(test_strXII.c_str(), test_strXII.length() + 1, 15, 10);
    EXPECT_TRUE(infoXII.valid);
    EXPECT_EQ(infoXII.offset, strlen("\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"));

    // start is delimiter ,
    string test_strXIII = ", MA\",\"MA\",\"25\",\"Massachusetts\",13,\"1633\",\"1630\",-3.00,0.00,0.00,-1,\"1600-1659\",33.00,\"1703\",\"1747\",5.00,\"1800\",\"1752\",-8.00,0.00,0.00,-1,\"1800-1859\",0.00,\"\",0.00,87.00,82.00,44.00,1.00,187.00,1,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n"
                          "2010,1,1,29,5,2010-01-29,\"B6\",20409,\"B6\",\"N558JB\",\"1014\",12478,1247801,31703,\"JFK\",\"New York, NY\",\"NY\",\"36\",\"New York\",22,10721,1072101,30721,\"BOS\",\"Boston, MA\",\"MA\",\"25\",\"Massachusetts\",13,\"1730\",\"1727\",-3.00,0.00,0.00,-1,\"1700-1759\",21.00,\"1748\",\"1830\",4.00,\"1900\",\"1834\",-26.00,0.00,0.00,-2,\"1900-1959\",0.00,\"\",0.00,90.00,67.00,42.00,1.00,187.00,1,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n"
                          "2010,1,1,29,5,2010-01-29,\"B6\",20409,\"B6\",\"N598JB\",\"1018\",12478,1247801,31703,\"JFK\",\"New York, NY\",\"NY\",\"36\",\"New York\",22,10721,1072101,30721,\"BOS\",\"Boston, MA\",\"MA\",\"25\",\"Massachusetts\",13,\"2235\",\"2225\",-10.00,0.00,0.00,-1,\"2200-2259\",14.00,\"2239\",\"2318\",3.00,\"2344\",\"2321\",-23.00,0.";
    auto infoXIII = findLineStart(test_strXIII.c_str(), test_strXIII.length() + 1, 16, 110);
    EXPECT_TRUE(infoXIII.valid);
    EXPECT_EQ(infoXIII.offset, strlen("\"Massachusetts\",13,\"1633\",\"1630\",-3.00,0.00,0.00,-1,\"1600-1659\",33.00,\"1703\",\"1747\",5.00,\"1800\",\"1752\",-8.00,0.00,0.00,-1,\"1800-1859\",0.00,\"\",0.00,87.00,82.00,44.00,1.00,187.00,1,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n"));
}

TEST(CSVUtils, anotherProblematicLineStart) {
    using namespace std;
    using namespace tuplex;
    string test_str = "old 01/03/2018\n"
                      "\"SOLD: $528,000\",70 School St # 1,Roxbury,MA,2119.0,,\"Price/sqft: $468 , 3 bds , 1 ba , 1,128 sqft\",,https://www.zillow.com/homedetails/70-School-St-1-Roxbury-MA-02119/2097617441_zpid/,Sold 12/21/2017\n";
    auto start = findLineStart(test_str.c_str(), test_str.length() + 1, 16, 10);
    EXPECT_EQ(start.offset, 200);
}

TEST(CSVUtils, csvFindLineStart) {
    using namespace std;
    using namespace tuplex;

    // new test from real data
    string test_strX = "\"SOLD: $350,000\",19 Stellman Rd,Boston,MA,2131.0,NULL,\"Price/sqft: $109 , 6 bds , 3 ba , 3,201 sqft\",NULL,https://www.zillow.com/homedetails/19-Stellman-Rd-Boston-MA-02131/59145589_zpid/,Sold 06/23/2016\n"
                       "For Sale by Owner,11 Watson St,SOMERVILLE,MA,2144.0,\"$1,"; // 10 columns

    auto info = findLineStart(test_strX.c_str(), test_strX.length() + 1, 3, 10);

    // check start line start is recognized correctly

    string test_strI = "0,,"; // 3 columns
    EXPECT_EQ(0, csvFindLineStart(test_strI.c_str(), test_strI.length() + 1, 3));

    string test_strII = "0,,3418309,0,70s3v5qRyCO/1PCdI6fVXnrW8FU/w+5CKRSa72xgcIo=,3,IHgtoxEBuUTHNbUeVs4hzptMY4n8rZKLbZg+Jh5fNG4=,wAmgn2H74cdoMuSFwJF3NaUEaudVBTZ0/HaNZBwIpEQ=";

    EXPECT_EQ(0, csvFindLineStart(test_strII.c_str(), test_strII.length() + 1, 8));

    string test_strIII = "0,,3418309,0,70s3v5qRyCO/1PCdI6fVXnrW8FU/w+5CKRSa72xgcIo=,3,IHgtoxEBuUTHNbUeVs4hzptMY4n8rZKLbZg+Jh5fNG4=,wAmgn2H74cdoMuSFwJF3NaUEaudVBTZ0/HaNZBwIpEQ=\n";
    EXPECT_EQ(0, csvFindLineStart(test_strIII.c_str(), test_strIII.length() + 1, 8));

    string test_strIV = "0,,2\n";
    EXPECT_EQ(0, csvFindLineStart(test_strIV.c_str(), test_strIV.length() + 1, 3));


    // check if test string starts with quotechar \"
    // or double quote char \"\"
    string test_strV = "\"\n,\"\"\n";
    EXPECT_EQ(2, csvFindLineStart(test_strV.c_str(), test_strV.length() + 1, 2));


    string test_strIX = "\"\",\n"
                       ",\"\",\n";
    EXPECT_EQ(4, csvFindLineStart(test_strIX.c_str(), test_strIX.length() + 1, 3));

    // start here within quoted field/end-of-quoted field.
    string test_strVI = "\",\n"
                        ",\"\",\n";
    EXPECT_EQ(3, csvFindLineStart(test_strVI.c_str(), test_strVI.length() + 1, 3));

    // test based on problematic slice of flight data
    string test_strVII = "\",\"21\",\"Kentucky\",52,11618,1161802,31703,\"EWR\",\"Newark, NJ\",\"NJ\",\"34\",\"New Jersey\",21,\"1233\",\"1224\",-9.00,0.00,0.00,-1,\"1200-1259\",9.00,\"1233\",\"1409\",11.00,\"1436\",\"1420\",-16.00,0.00,0.00,-2,\"1400-1459\",0.00,\"\",0.00,123.00,116.00,96.00,1.00,642.00,3,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n"
                       "2015,1,2,14,6,2015-02-14,\"EV\",20366,\"EV\",\"N16183\",\"4500\",14730,1473003,33044,\"SDF\",\"Louisville, KY\",\"KY\",\"21\",\"Kentucky\",52,11618,1161802,31703,\"EWR\",\"Newark, NJ\",\"NJ\",\"34\",\"New Jersey\",21,\"1350\",\"\",,,,,\"1300-1359\",,\"\",\"\",,\"1552\",\"\",,,,,\"1500-1559\",1.00,\"C\",0.00,122.00,,,1.00,642.00,3,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n"
                       "2015,1,2,3,2,2015-02-03,\"EV\",20366,\"EV\",\"N13903\",\"4500\",14730,1473003,33044,\"SDF\",\"Louisville, KY\",\"KY\",\"21\",\"Kentucky\",52,11618,1161802,31703,\"EWR\",\"Newark, NJ\",\"NJ\",\"34\",\"New Jersey\",21,\"1233\",\"1226\",-7.00,0.00,0.00,-1,\"1200-1259\",8.00,\"1234\",\"1411\",10.00,\"1436\",\"1421\",-15.00,0.00,0.00,-1,\"140\"";

    int start_VII = csvFindLineStart(test_strVII.c_str(), test_strVII.length() + 1, 110);

    EXPECT_TRUE(start_VII > 0);
    EXPECT_EQ(strlen("\",\"21\",\"Kentucky\",52,11618,1161802,31703,\"EWR\",\"Newark, NJ\",\"NJ\",\"34\",\"New Jersey\",21,\"1233\",\"1224\",-9.00,0.00,0.00,-1,\"1200-1259\",9.00,\"1233\",\"1409\",11.00,\"1436\",\"1420\",-16.00,0.00,0.00,-2,\"1400-1459\",0.00,\"\",0.00,123.00,116.00,96.00,1.00,642.00,3,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n"), start_VII);

    string test_strVIII = "\"2015\",1,2,14,6,2015-02-14,\"EV\",20366,\"EV\",\"N16183\",\"4500\",14730,1473003,33044,\"SDF\",\"Louisville, KY\",\"KY\",\"21\",\"Kentucky\",52,11618,1161802,31703,\"EWR\",\"Newark, NJ\",\"NJ\",\"34\",\"New Jersey\",21,\"1350\",\"\",,,,,\"1300-1359\",,\"\",\"\",,\"1552\",\"\",,,,,\"1500-1559\",1.00,\"C\",0.00,122.00,,,1.00,642.00,3,,,,,,\"\",,,0,,,,,\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\"\",,,\"\",,,\"\",\"\",\n";
    EXPECT_EQ(0, csvFindLineStart(test_strVIII.c_str(), test_strVIII.length() + 1, 110));
}

TEST(CSVUtils, I64ToString) {
    char buf[21];
    EXPECT_EQ(i64toa_sse2(100,buf), 3);
    EXPECT_EQ(std::string(buf), "100");

    EXPECT_EQ(i64toa_sse2(1000000000,buf), 10);
    EXPECT_EQ(std::string(buf), "1000000000");

    EXPECT_EQ(i64toa_sse2(100000000000000000,buf), 18);
    EXPECT_EQ(std::string(buf), "100000000000000000");
}