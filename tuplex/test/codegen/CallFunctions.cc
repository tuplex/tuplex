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
#include <AnnotatedAST.h>
#include "Helper.h"

TEST(PythonCallFunctions, SimpleCall) {
    auto code = "lambda x: len(x)";
    std::map<std::string, python::Type> typeMap{{"x", python::Type::STRING }};

    testDefCode(code, typeMap, python::Type::I64);
}


// int and so on will produce errors, hence add stuff there...
// also make sure fallback solution works...

TEST(PythonCallFunctions, SymbolTableOverrideBuiltin) {
    // this is a check that the symbol table works correctly.
    // I.e. if as arg of the function an identifier is used, than no builtin should override that!
    auto code = "lambda len: len";
    std::map<std::string, python::Type> typeMap{{"len", python::Type::STRING }};

    testDefCode(code, typeMap, python::Type::STRING);
}

// @TODO: what happens in error case len(len)??

TEST(PythonCallFunctions, SimpleMethodCall) {
    auto code = "lambda x: x.upper()";
    std::map<std::string, python::Type> typeMap{{"x", python::Type::STRING }};

    testDefCode(code, typeMap, python::Type::STRING);
}

TEST(PythonCallFunctions, SimpleMethodCallII) {
    auto code = "lambda x: x.upper().lower()";
    std::map<std::string, python::Type> typeMap{{"x", python::Type::STRING }};

    testDefCode(code, typeMap, python::Type::STRING);
}


// future work...
// Lambdas as first class citizens!!!
//TEST(PythonCallFunctions, NestedLambda) {
//    auto code = "lambda y: (lambda a, b: a * b)(y, 2.0)";
//    std::map<std::string, python::Type> typeMap{{"y", python::Type::I64 }};
//
//    testDefCode(code, typeMap, python::Type::F64);
//}
//
//TEST(PythonCallFunctions, NestedLambdaNameConflict) {
//    auto code = "lambda y: (lambda a, y: a * y)(y, 2.0)";
//    std::map<std::string, python::Type> typeMap{{"y", python::Type::I64 }};
//
//    testDefCode(code, typeMap, python::Type::F64);
//}


// bool() == False
// abs(True) == 1
// abs(False) == 0
// float() == 0.0
// int() == 0

// note that float conversion actually needs to account for inf/nan and so on...
// Return a floating point number constructed from a number or string x.
//
//If the argument is a string, it should contain a decimal number, optionally preceded by a sign, and optionally embedded in whitespace. The optional sign may be '+' or '-'; a '+' sign has no effect on the value produced. The argument may also be a string representing a NaN (not-a-number), or a positive or negative infinity. More precisely, the input must conform to the following grammar after leading and trailing whitespace characters are removed:
//
//sign           ::=  "+" | "-"
//infinity       ::=  "Infinity" | "inf"
//nan            ::=  "nan"
//numeric_value  ::=  floatnumber | infinity | nan
//numeric_string ::=  [sign] numeric_value
//Here floatnumber is the form of a Python floating-point literal, described in Floating point literals. Case is not significant, so, for example, “inf”, “Inf”, “INFINITY” and “iNfINity” are all acceptable spellings for positive infinity.


// for bool: https://docs.python.org/3/library/stdtypes.html#truth