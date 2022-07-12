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
#include <ast/AnnotatedAST.h>
#include "Helper.h"

// TODO to be done...

//// basic functions to compile
//TEST(PythonFunctions, SimpleReturnNoParams) {
//    auto code = "def test():\n"
//                "   return 42";
//
//    testDefCode(code, std::map<std::string, python::Type>(), python::Type::I64);
//}


//TEST(PythonFunctions, SimpleIdentity) {
//    auto code = "def test(x):\n"
//                "   return x";
//
//    testDefCode(code);
//}
//
//TEST(PythonFunctions, SimpleReturnExpression) {
//    auto code = "def test(x):\n"
//                "   return x";
//
//    testDefCode(code);
//}
//
//TEST(PythonFunctions, SimpleReturnNoValue) {
//    auto code = "def test():\n"
//                "   return";
//
//    testDefCode(code);
//}
//
//TEST(PythonFunctions, SimpleAssignmentStmt) {
//    auto code = "def test(x):\n"
//                "   y = x * x\n"
//                "   return y";
//
//    testDefCode(code);
//}
//
//TEST(PythonFunctions, Haversine) {
//
//    // note: power operator still missing here...
//
////    auto code = "def haversine(lat1, lon1, lat2, lon2):\n"
////                "    r = 6378137.0\n"
////                "    lathalfdiff = 0.5 * (lat2 - lat1)\n"
////                "    lonhalfdiff = 0.5 * (lon2 - lon1)\n"
////                "    sinlat = sin(lathalfdiff)\n"
////                "    sinlon = sin(lonhalfdiff)\n"
////                "    h = sinlat**2 + cos(lat1) * cos(lat2) * sinlon**2\n"
////                "    return 2.0 * r * asin(sqrt(h))";
//
//    // this parses at least now...
//    auto code = "def haversine(lat1, lon1, lat2, lon2):\n"
//                "    r = 6378137.0\n"
//                "    lathalfdiff = 0.5 * (lat2 - lat1)\n"
//                "    lonhalfdiff = 0.5 * (lon2 - lon1)\n"
//                "    sinlat = sin(lathalfdiff)\n"
//                "    sinlon = sin(lonhalfdiff)\n"
//                "    h = sinlat * sinlat + cos(lat1) * cos(lat2) * sinlon * sinlon\n"
//                "    return 2.0 * r";
//
//    // check that the lexer works
//    auto tokens = lex(code);
//
//    testDefCode(code);
//}
//
//TEST(PythonFunctions, SimpleCall) {
////    auto code = "lambda x: sin(x)";
////
//    auto code = "def test():\n"
//                "    x = sin(10)\n"
//                "    return 10";
//
//    // check that the lexer works
//    auto tokens = lex(code);
//
//    testDefCode(code);
//}
//
//
//// assignment special cases
//// x = y = 2
//
//// x, y = y, x
//
//// (x, y) = 1, 2