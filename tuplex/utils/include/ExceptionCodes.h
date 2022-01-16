//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_EXCEPTIONS_H
#define TUPLEX_EXCEPTIONS_H

#include <cstddef>
#include <cstdint>
#include <string>

#include <unordered_map>

namespace tuplex {

    //! enum holding the exception codes supported by the framework
    //! make sure these match codes defined in exceptions.py
    enum class ExceptionCode : int32_t {
        UNKNOWN=-1,
        SUCCESS = 0, // important to set this to zero to speed up things.
        OUTPUT_LIMIT_REACHED=2, // dummy code to return/check for when output limit has been reached (aggregate limit!)

        // Python exceptions from https://docs.python.org/3/library/exceptions.html
        BASEEXCEPTION=100,
        EXCEPTION=101,
        ARITHMETICERROR=102,
        BUFFERERROR=103,
        LOOKUPERROR=104,
        ASSERTIONERROR=105,
        ATTRIBUTEERROR=106,
        EOFERROR=107,
        GENERATOREXIT=108,
        IMPORTERROR=109,
        MODULENOTFOUNDERROR=110,
        INDEXERROR=111,
        KEYERROR=112,
        KEYBOARDINTERRUPT=113,
        MEMORYERROR=114,
        NAMEERROR=115,
        NOTIMPLEMENTEDERROR=116,
        OSERROR=117,
        OVERFLOWERROR=118,
        RECURSIONERROR=119,
        REFERENCEERROR=120,
        RUNTIMEERROR=121,
        STOPITERATION=122,
        STOPASYNCITERATION=123,
        SYNTAXERROR=124,
        INDENTATIONERROR=125,
        TABERROR=126,
        SYSTEMERROR=127,
        SYSTEMEXIT=128,
        TYPEERROR=129,
        UNBOUNDLOCALERROR=130,
        UNICODEERROR=131,
        UNICODEENCODEERROR=132,
        UNICODEDECODEERROR=133,
        UNICODETRANSLATEERROR=134,
        VALUEERROR=135,
        ZERODIVISIONERROR=136,
        ENVIRONMENTERROR=137,
        IOERROR=138,
        BLOCKINGIOERROR=139,
        CHILDPROCESSERROR=140,
        CONNECTIONERROR=141,
        BROKENPIPEERROR=142,
        CONNECTIONABORTEDERROR=143,
        CONNECTIONREFUSEDERROR=144,
        FILEEXISTSERROR=145,
        FILENOTFOUNDERROR=146,
        INTERRUPTEDERROR=147,
        ISADIRECTORYERROR=148,
        NOTADIRECTORYERROR=149,
        PERMISSIONERROR=150,
        PROCESSLOOKUPERROR=151,
        TIMEOUTERROR=152,

        // Python 3.8, 3.9
        FLOATINGPOINTERROR=153,
        WARNING=154,
        DEPRECATIONWARNING=155,
        RUNTIMEWARNING=156,
        SYNTAXWARNING=157,
        USERWARNING=158,
        FUTUREWARNING=159,
        IMPORTWARNING=160,
        UNICODEWARNING=161,
        BYTESWARNING=162,
        RESOURCEWARNING=163,
        PENDINGDEPRECATIONWARNING=164,
        CONNECTIONRESETERROR=165,

        // Regex error class (re.error)
        // ==> i.e. can be raised too via raise re.error('hello')
        // cf. for this https://github.com/python/cpython/blob/3.9/Lib/re.py
        RE_ERROR=200,

        // FRAMEWORK specific errors
        NORMALCASEVIOLATION=07, // when the normal case is violated (e.g. in if branches)
        FILENOTFOUND = 10,
        CSV_UNDERRUN = 20,  //! too few columns
        CSV_OVERRUN = 21, //! too many columns
        BADSERIALIZATION = 30,
        NULLERROR=50, //! used when null was encountered but non-null was expected!
        SCHEMAERROR=51, //! output schema does not match expected schema.
        I64PARSE_ERROR=52, // general parsing error for an integer (fast atoi)
        F64PARSE_ERROR=53, // parsing error as double (fast atod)
        BOOLPARSE_ERROR=54, // parsing error as bool.
        DOUBLEQUOTEERROR=55, // may happen whenever there is a single quote in a quoted field
        PYTHONFALLBACK_SERIALIZATION=60, // happens when callWithExceptionHandler is executed and serialization to user mem fails
        BADPARSE_STRING_INPUT=70, // used to signal that parsing didn't work, exception input will be then stored as length/string field.
        PYTHON_PARALLELIZE=80
    };


    inline int32_t ecToI32(const ExceptionCode& ec) { return static_cast<int32_t >(ec); }
    inline ExceptionCode i32ToEC(const int32_t i) { return static_cast<ExceptionCode>(i); }
    inline int64_t ecToI64(const ExceptionCode& ec) { return static_cast<int64_t >(ec); }
    inline ExceptionCode i64ToEC(const int64_t i) { return static_cast<ExceptionCode>(i); }


    inline std::string exceptionCodeToString(const ExceptionCode& ec) {
        switch(ec) {
            case ExceptionCode::SUCCESS:
                return "success";
            case ExceptionCode::INDEXERROR:
                return "index error";
            case ExceptionCode::CSV_UNDERRUN:
                return "csv parser underrun, could not parse line fully";
            case ExceptionCode::CSV_OVERRUN:
                return "csv parser overrrun, more columns than expected";
            case ExceptionCode::ZERODIVISIONERROR:
                return "div by zero";
            case ExceptionCode::I64PARSE_ERROR:
                return "integer parse error";
            case ExceptionCode::F64PARSE_ERROR:
                return "float parse error";
            case ExceptionCode::BOOLPARSE_ERROR:
                return "bool parse error";
            case ExceptionCode::VALUEERROR:
                return "value error";
            default:
                return "UNKNOWN exception (Code: " + std::to_string(ecToI32(ec)) + ")";
        }
    }


    inline ExceptionCode pythonClassToExceptionCode(const std::string& name) {
        static std::unordered_map<std::string, ExceptionCode> m{{"BaseException", ExceptionCode::BASEEXCEPTION},
                                                                {"SystemExit", ExceptionCode::SYSTEMEXIT},
                                                                {"KeyboardInterrupt", ExceptionCode::KEYBOARDINTERRUPT},
                                                                {"GeneratorExit", ExceptionCode::GENERATOREXIT},
                                                                {"Exception", ExceptionCode::EXCEPTION},
                                                                {"StopIteration", ExceptionCode::STOPITERATION},
                                                                {"StopAsyncIteration", ExceptionCode::STOPASYNCITERATION},
                                                                {"ArithmeticError", ExceptionCode::ARITHMETICERROR},
                                                                {"FloatingPointError", ExceptionCode::FLOATINGPOINTERROR},
                                                                {"OverflowError", ExceptionCode::OVERFLOWERROR},
                                                                {"ZeroDivisionError", ExceptionCode::ZERODIVISIONERROR},
                                                                {"AssertionError", ExceptionCode::ASSERTIONERROR},
                                                                {"AttributeError", ExceptionCode::ATTRIBUTEERROR},
                                                                {"BufferError", ExceptionCode::BUFFERERROR},
                                                                {"EOFError", ExceptionCode::EOFERROR},
                                                                {"ImportError", ExceptionCode::IMPORTERROR},
                                                                {"ModuleNotFoundError", ExceptionCode::MODULENOTFOUNDERROR},
                                                                {"LookupError", ExceptionCode::LOOKUPERROR},
                                                                {"IndexError", ExceptionCode::INDEXERROR},
                                                                {"KeyError", ExceptionCode::KEYERROR},
                                                                {"MemoryError", ExceptionCode::MEMORYERROR},
                                                                {"NameError", ExceptionCode::NAMEERROR},
                                                                {"UnboundLocalError", ExceptionCode::UNBOUNDLOCALERROR},
                                                                {"OSError", ExceptionCode::OSERROR},
                                                                {"BlockingIOError", ExceptionCode::BLOCKINGIOERROR},
                                                                {"ChildProcessError", ExceptionCode::CHILDPROCESSERROR},
                                                                {"ConnectionError", ExceptionCode::CONNECTIONERROR},
                                                                {"BrokenPipeError", ExceptionCode::BROKENPIPEERROR},
                                                                {"ConnectionAbortedError", ExceptionCode::CONNECTIONABORTEDERROR},
                                                                {"ConnectionRefusedError", ExceptionCode::CONNECTIONREFUSEDERROR},
                                                                {"ConnectionResetError", ExceptionCode::CONNECTIONRESETERROR},
                                                                {"FileExistsError", ExceptionCode::FILEEXISTSERROR},
                                                                {"FileNotFoundError", ExceptionCode::FILENOTFOUNDERROR},
                                                                {"InterruptedError", ExceptionCode::INTERRUPTEDERROR},
                                                                {"IsADirectoryError", ExceptionCode::ISADIRECTORYERROR},
                                                                {"NotADirectoryError", ExceptionCode::NOTADIRECTORYERROR},
                                                                {"PermissionError", ExceptionCode::PERMISSIONERROR},
                                                                {"ProcessLookupError", ExceptionCode::PROCESSLOOKUPERROR},
                                                                {"TimeoutError", ExceptionCode::TIMEOUTERROR},
                                                                {"ReferenceError", ExceptionCode::REFERENCEERROR},
                                                                {"RuntimeError", ExceptionCode::RUNTIMEERROR},
                                                                {"NotImplementedError", ExceptionCode::NOTIMPLEMENTEDERROR},
                                                                {"RecursionError", ExceptionCode::RECURSIONERROR},
                                                                {"SyntaxError", ExceptionCode::SYNTAXERROR},
                                                                {"IndentationError", ExceptionCode::INDENTATIONERROR},
                                                                {"TabError", ExceptionCode::TABERROR},
                                                                {"SystemError", ExceptionCode::SYSTEMERROR},
                                                                {"TypeError", ExceptionCode::TYPEERROR},
                                                                {"ValueError", ExceptionCode::VALUEERROR},
                                                                {"UnicodeError", ExceptionCode::UNICODEERROR},
                                                                {"UnicodeDecodeError", ExceptionCode::UNICODEDECODEERROR},
                                                                {"UnicodeEncodeError", ExceptionCode::UNICODEENCODEERROR},
                                                                {"UnicodeTranslateError", ExceptionCode::UNICODETRANSLATEERROR},
                                                                {"Warning", ExceptionCode::WARNING},
                                                                {"DeprecationWarning", ExceptionCode::DEPRECATIONWARNING},
                                                                {"PendingDeprecationWarning", ExceptionCode::PENDINGDEPRECATIONWARNING},
                                                                {"RuntimeWarning", ExceptionCode::RUNTIMEWARNING},
                                                                {"SyntaxWarning", ExceptionCode::SYNTAXWARNING},
                                                                {"UserWarning", ExceptionCode::USERWARNING},
                                                                {"FutureWarning", ExceptionCode::FUTUREWARNING},
                                                                {"ImportWarning", ExceptionCode::IMPORTWARNING},
                                                                {"UnicodeWarning", ExceptionCode::UNICODEWARNING},
                                                                {"BytesWarning", ExceptionCode::BYTESWARNING},
                                                                {"ResourceWarning", ExceptionCode::RESOURCEWARNING},
                                                                {"re.error", ExceptionCode::RE_ERROR}};

       auto it = m.find(name);
       if(it == m.end())
           return ExceptionCode::UNKNOWN;
       else return it->second;
    }

    inline std::string exceptionCodeToPythonClass(const ExceptionCode& ec) {

        // code generated through script
        // # C++ code
        // code = 'switch(ec) {\n'
        // for k, v in lookup.items():
        // name = k.__name__
        // code += '\tcase ExceptionCode::{}:\n'.format(name.upper())
        // code += '\t\treturn "{}";\n'.format(name)
        // code += '\tdefault:\n\t\treturn "Exception";'
        // code += '\n}'

        switch(ec) {
            case ExceptionCode::BASEEXCEPTION:
                return "BaseException";
            case ExceptionCode::SYSTEMEXIT:
                return "SystemExit";
            case ExceptionCode::KEYBOARDINTERRUPT:
                return "KeyboardInterrupt";
            case ExceptionCode::GENERATOREXIT:
                return "GeneratorExit";
            case ExceptionCode::EXCEPTION:
                return "Exception";
            case ExceptionCode::STOPITERATION:
                return "StopIteration";
            case ExceptionCode::STOPASYNCITERATION:
                return "StopAsyncIteration";
            case ExceptionCode::ARITHMETICERROR:
                return "ArithmeticError";
            case ExceptionCode::FLOATINGPOINTERROR:
                return "FloatingPointError";
            case ExceptionCode::OVERFLOWERROR:
                return "OverflowError";
            case ExceptionCode::ZERODIVISIONERROR:
                return "ZeroDivisionError";
            case ExceptionCode::ASSERTIONERROR:
                return "AssertionError";
            case ExceptionCode::ATTRIBUTEERROR:
                return "AttributeError";
            case ExceptionCode::BUFFERERROR:
                return "BufferError";
            case ExceptionCode::EOFERROR:
                return "EOFError";
            case ExceptionCode::IMPORTERROR:
                return "ImportError";
            case ExceptionCode::MODULENOTFOUNDERROR:
                return "ModuleNotFoundError";
            case ExceptionCode::LOOKUPERROR:
                return "LookupError";
            case ExceptionCode::INDEXERROR:
                return "IndexError";
            case ExceptionCode::KEYERROR:
                return "KeyError";
            case ExceptionCode::MEMORYERROR:
                return "MemoryError";
            case ExceptionCode::NAMEERROR:
                return "NameError";
            case ExceptionCode::UNBOUNDLOCALERROR:
                return "UnboundLocalError";
            case ExceptionCode::OSERROR:
                return "OSError";
            case ExceptionCode::BLOCKINGIOERROR:
                return "BlockingIOError";
            case ExceptionCode::CHILDPROCESSERROR:
                return "ChildProcessError";
            case ExceptionCode::CONNECTIONERROR:
                return "ConnectionError";
            case ExceptionCode::BROKENPIPEERROR:
                return "BrokenPipeError";
            case ExceptionCode::CONNECTIONABORTEDERROR:
                return "ConnectionAbortedError";
            case ExceptionCode::CONNECTIONREFUSEDERROR:
                return "ConnectionRefusedError";
            case ExceptionCode::CONNECTIONRESETERROR:
                return "ConnectionResetError";
            case ExceptionCode::FILEEXISTSERROR:
                return "FileExistsError";
            case ExceptionCode::FILENOTFOUNDERROR:
                return "FileNotFoundError";
            case ExceptionCode::INTERRUPTEDERROR:
                return "InterruptedError";
            case ExceptionCode::ISADIRECTORYERROR:
                return "IsADirectoryError";
            case ExceptionCode::NOTADIRECTORYERROR:
                return "NotADirectoryError";
            case ExceptionCode::PERMISSIONERROR:
                return "PermissionError";
            case ExceptionCode::PROCESSLOOKUPERROR:
                return "ProcessLookupError";
            case ExceptionCode::TIMEOUTERROR:
                return "TimeoutError";
            case ExceptionCode::REFERENCEERROR:
                return "ReferenceError";
            case ExceptionCode::RUNTIMEERROR:
                return "RuntimeError";
            case ExceptionCode::NOTIMPLEMENTEDERROR:
                return "NotImplementedError";
            case ExceptionCode::RECURSIONERROR:
                return "RecursionError";
            case ExceptionCode::SYNTAXERROR:
                return "SyntaxError";
            case ExceptionCode::INDENTATIONERROR:
                return "IndentationError";
            case ExceptionCode::TABERROR:
                return "TabError";
            case ExceptionCode::SYSTEMERROR:
                return "SystemError";
            case ExceptionCode::TYPEERROR:
                return "TypeError";
            case ExceptionCode::VALUEERROR:
                return "ValueError";
            case ExceptionCode::UNICODEERROR:
                return "UnicodeError";
            case ExceptionCode::UNICODEDECODEERROR:
                return "UnicodeDecodeError";
            case ExceptionCode::UNICODEENCODEERROR:
                return "UnicodeEncodeError";
            case ExceptionCode::UNICODETRANSLATEERROR:
                return "UnicodeTranslateError";
            case ExceptionCode::WARNING:
                return "Warning";
            case ExceptionCode::DEPRECATIONWARNING:
                return "DeprecationWarning";
            case ExceptionCode::PENDINGDEPRECATIONWARNING:
                return "PendingDeprecationWarning";
            case ExceptionCode::RUNTIMEWARNING:
                return "RuntimeWarning";
            case ExceptionCode::SYNTAXWARNING:
                return "SyntaxWarning";
            case ExceptionCode::USERWARNING:
                return "UserWarning";
            case ExceptionCode::FUTUREWARNING:
                return "FutureWarning";
            case ExceptionCode::IMPORTWARNING:
                return "ImportWarning";
            case ExceptionCode::UNICODEWARNING:
                return "UnicodeWarning";
            case ExceptionCode::BYTESWARNING:
                return "BytesWarning";
            case ExceptionCode::RESOURCEWARNING:
                return "ResourceWarning";

            // special tuplex codes
            case ExceptionCode::CSV_OVERRUN:
                return "CSVOverrunError";
            case ExceptionCode::CSV_UNDERRUN:
                return "CSVUnderrunError";
            case ExceptionCode::I64PARSE_ERROR:
                return "IntegerParseError";

            case ExceptionCode::RE_ERROR:
                return "re.error";

            // internal exception codes
            case ExceptionCode::BADPARSE_STRING_INPUT:
                return "tuplex.internal.BadParseStringInput";
            case ExceptionCode::NORMALCASEVIOLATION:
                return "tuplex.internal.normalCaseViolation";
            default:
                return "Exception";
        }
    }
}

#endif //TUPLEX_EXCEPTIONS_H