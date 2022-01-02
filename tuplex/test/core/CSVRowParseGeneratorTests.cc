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
#include <physical/CSVParseRowGenerator.h>
#include <JITCompiler.h>
#include <Context.h>

using namespace tuplex;

class CSVRowParseTest : public TuplexTest {
protected:
    void SetUp() override {
        TuplexTest::SetUp();
        Context c(testOptions());

        // init result Object with enough memory (1KB should be sufficient
        resultObject = malloc(1024);
        env.reset(new codegen::LLVMEnvironment);
    }

    void TearDown() override {
        free(resultObject);
    }

    void* resultObject;

    std::unique_ptr<codegen::LLVMEnvironment> env;
    JITCompiler compiler;

    llvm::Function* _funcGetNumBytes;
    llvm::Function* _funcGetLineStart;
    llvm::Function* _funcGetLineEnd;



    // returns parse code (exception code), first param is result, second is input ptr, then comes endptr
    typedef int32_t(*rowparse_f)(void*, uint8_t*, uint8_t*);
    typedef char*(*helper_f)(void*);
    typedef void*(*get_col_f)(void*, int);

    get_col_f _getColumn;

    // compile & return function pointer
    // signature of the function is void*, uint8*, uint8*
    rowparse_f getFunctionPointer(const std::string& name) {
        return (rowparse_f)compiler.getAddrOfSymbol(name);
    }

    // helper function to execute parse row and return results
    struct ParseResult {
        ExceptionCode ec;
        size_t numBytesParsed;
        char* lineStart;
        char* lineEnd;
    };

    void createGetColumn(const tuplex::codegen::CSVParseRowGenerator& gen) {
        using namespace std;
        using namespace llvm;

        auto& ctx = env->getContext();

        // getColumn helper ()
        FunctionType *FT = FunctionType::get(env->i8ptrType(), {env->i8ptrType(),
                                                                tuplex::codegen::ctypeToLLVM<int>(ctx)}, false);
        auto func = Function::Create(FT, Function::ExternalLinkage, "getColumn", env->getModule().get());

        BasicBlock* bbBody = BasicBlock::Create(ctx, "body",func);
        IRBuilder<> builder(bbBody);

        auto argMap = tuplex::codegen::mapLLVMFunctionArgs(func, {"result", "column"});

        vector<tuplex::codegen::SerializableValue> vals;

        auto num_cols = gen.serializedType().parameters().size();
        auto res_ptr = builder.CreatePointerCast(argMap["result"], gen.resultType()->getPointerTo(0));
        for(int i = 0; i < num_cols; ++i) {
            vals.emplace_back(gen.getColumnResult(builder, i, res_ptr));
        }

        // create dummy struct
       auto arr = builder.CreateAlloca(ArrayType::get(env->i8ptrType(), num_cols));

        // store in struct and then retrieve via column arg!
        for(int i = 0; i < num_cols; ++i) {
            auto dummy = vals[i].val;

            // cast to i8*
            if(dummy->getType() == env->doubleType()) { // needs to come first b.c. of fall through
                // fool LLVM to allow hard byte cast to pointer
                // i.e. go to godbolt.org and check code for auto ival = *((uint64_t*)&foo);

                auto d = builder.CreateAlloca(env->doubleType());
                builder.CreateStore(dummy, d);
                dummy = builder.CreateLoad(builder.CreateBitOrPointerCast(d, env->i64ptrType()));
            }

            if(dummy->getType()->isIntegerTy())
                dummy = builder.CreateIntToPtr(dummy, env->i8ptrType());



            builder.CreateStore(dummy, builder.CreateGEP(arr, {env->i32Const(0), env->i32Const(i)}));
        }

        auto val = builder.CreateLoad(builder.CreateGEP(arr, {env->i32Const(0), argMap["column"]}));

////        Value *retval = val;
////
////        if(retval->getType()->isIntegerTy())
////            retval = builder.CreateIntToPtr(retval, env->i8ptrType());
////        if(retval->getType()->isFloatTy()) {
//
//            // i.e. char* test(double x) {
//            //    return (char*)*reinterpret_cast<char*>(&x);
//            //}
//         throw std::runtime_error("not yet supported");
//        }


        builder.CreateRet(val);
    }

    // add helper function to get results back
    void createHelpers(const tuplex::codegen::CSVParseRowGenerator& gen) {
        using namespace llvm;
        auto& context = env->getContext();
        auto i8ptr_type = Type::getInt8PtrTy(context, 0);

        auto retType = i8ptr_type;
        std::vector<Type*> paramTypes{gen.resultType()->getPointerTo(0)};
        FunctionType *FT = FunctionType::get(retType, paramTypes, false);
        FunctionType *FT_nb = FunctionType::get(env->i64Type(), paramTypes, false);
        auto linkage = Function::ExternalLinkage;

        _funcGetNumBytes = Function::Create(FT_nb, linkage, "getNumBytes", env->getModule().get());
        _funcGetLineStart = Function::Create(FT, linkage, "getLineStart", env->getModule().get());
        _funcGetLineEnd = Function::Create(FT, linkage, "getLineEnd", env->getModule().get());

        BasicBlock *bNumBytes = BasicBlock::Create(context, "body", _funcGetNumBytes);
        BasicBlock *bLineStart = BasicBlock::Create(context, "body", _funcGetLineStart);
        BasicBlock *bLineEnd = BasicBlock::Create(context, "body", _funcGetLineEnd);

        std::vector<llvm::Argument*> vNumBytesArgs;
        std::vector<llvm::Argument*> vLineStartArgs;
        std::vector<llvm::Argument*> vLineEndArgs;
        for(auto& arg : _funcGetNumBytes->args())
            vNumBytesArgs.push_back(&arg);
        for(auto& arg : _funcGetLineStart->args())
            vLineStartArgs.push_back(&arg);
        for(auto& arg : _funcGetLineEnd->args())
            vLineEndArgs.push_back(&arg);


        IRBuilder<> builder(bNumBytes);
        builder.CreateRet(builder.CreateLoad(builder.CreateGEP(vLineStartArgs[0], {env->i32Const(0),env->i32Const(0)})));

        builder.SetInsertPoint(bLineStart);
        builder.CreateRet(builder.CreateLoad(builder.CreateGEP(vLineStartArgs[0], {env->i32Const(0),env->i32Const(1)})));

        builder.SetInsertPoint(bLineEnd);
        builder.CreateRet(builder.CreateLoad(builder.CreateGEP(vLineEndArgs[0], {env->i32Const(0),env->i32Const(2)})));


        // magical retrieve column function
        createGetColumn(gen);
    }

    int64_t getInt(size_t index) {

        uint8_t* ptr = (uint8_t*)resultObject;
        assert(_getColumn);

        auto res = _getColumn(ptr, (int32_t)index);
        int64_t val = reinterpret_cast<int64_t>(res);
        return val;
    }

    double getDouble(size_t index) {

        uint8_t* ptr = (uint8_t*)resultObject;
        double val = 0.0;

        auto ival = getInt(index);
        val = *((double*)&ival);

        return val;
    }

    bool getBool(size_t index) {
        return getInt(index) > 0;
    }

    // most difficult one is string
    std::string getString(size_t index) {
        uint8_t* ptr = (uint8_t*)resultObject;

        char* start = (char*)_getColumn(ptr, index);
        // strings in memory should be zero terminated.
        // s.t. here return std::string(start); could be used
        // however, this would require additional memcpying. To avoid this,
        // in the generated code, string are zero terminated WHEN they are written out to memory.

        // to artifically produce this here, simply -1 and use the string from non-zero memory function
        // fromCharPointers
        return start;
    }

    ParseResult parse(const tuplex::codegen::CSVParseRowGenerator& gen, const std::string& s) {
        ParseResult pr;

        // create helpers
        createHelpers(gen);

        // compile module
        auto ir_code = codegen::moduleToString(*env->getModule().get());
        std::cout<<ir_code<<std::endl;
        bool bCompileResult = compiler.compile(ir_code);
        assert(bCompileResult);

        // get main function
        auto fun = getFunctionPointer(gen.functionName());
        assert(fun);


        // get helper funcs
        auto getNumBytes = (int64_t(*)(void*))compiler.getAddrOfSymbol("getNumBytes");
        auto getLineStart = (helper_f)compiler.getAddrOfSymbol("getLineStart");
        auto getLineEnd = (helper_f)compiler.getAddrOfSymbol("getLineEnd");

        _getColumn = (get_col_f)compiler.getAddrOfSymbol("getColumn");

        // copy stuff
        auto secure_length = s.length() + 512;
        char* data = new char[secure_length];
        memset(data, 0, secure_length);
        memcpy(data, s.c_str(), s.length());

        pr.ec = i32ToEC(fun(resultObject, (uint8_t*)data, (uint8_t*)data + s.length()));

        pr.numBytesParsed = getNumBytes(resultObject);
        pr.lineStart = getLineStart(resultObject);
        pr.lineEnd = getLineEnd(resultObject);
        return pr;
    }
};


TEST_F(CSVRowParseTest, EmptyString) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 0);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "");
}

TEST_F(CSVRowParseTest, SingleIntegerCell) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 2);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10");
}

TEST_F(CSVRowParseTest, SingleIntegerCellNewLineSkipAtBeginning) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\n\r\n10");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 5);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10");
}

TEST_F(CSVRowParseTest, SingleIntegerCellLineBreak) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10\n");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 2); // line stops before first '\n' or '\r;. When parse row is called again, there is an empty line.
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10"); // \n is trimmed away from result
}

TEST_F(CSVRowParseTest, SingleIntegerCellOverrun) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10,");

    EXPECT_TRUE(res.ec == ExceptionCode::CSV_OVERRUN);
    EXPECT_EQ(res.numBytesParsed, 3);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10,");
}

TEST_F(CSVRowParseTest, SingleIntegerCellOverrunLineBreak) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10,\n");

    EXPECT_TRUE(res.ec == ExceptionCode::CSV_OVERRUN);
    EXPECT_EQ(res.numBytesParsed, 3);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10,");
}


TEST_F(CSVRowParseTest, SingleIntegerCellOverrunLineBreakWin) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10,\r");

    EXPECT_TRUE(res.ec == ExceptionCode::CSV_OVERRUN);
    EXPECT_EQ(res.numBytesParsed, 3);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10,");
}

TEST_F(CSVRowParseTest, SingleIntegerCellConvErrorI) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10$");

    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be I64Parse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 3);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10$");
}

TEST_F(CSVRowParseTest, SingleIntegerCellConvErrorII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    // cell will not be serialized, hence test will pass
    rg.addCell(python::Type::I64, false).build(false);

    auto res = parse(rg, "10$");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 3);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10$");
}

TEST_F(CSVRowParseTest, SingleIntegerCellConvErrorIII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "$10");

    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be I64Parse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 3);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "$10");
}

// whitespace errors.
TEST_F(CSVRowParseTest, SingleIntegerCellConvErrorIV) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\t10   \n"); // parsing ignores whitespace before/after because that's python behavior...

    EXPECT_EQ(res.numBytesParsed, 6);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\t10   ");
}
// -----------------------------------------------------------------------
// all tests one more time but this time quoted!
TEST_F(CSVRowParseTest, QuotedEmptyString) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"\"");

    // old version, where ,, is considered as exception/NULL value.
    //    EXPECT_TRUE(res.ec == ExceptionCode::NULLVALUE);
    //    EXPECT_EQ(res.numBytesParsed, 2);
    //    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    //    EXPECT_EQ(s, "\"\"");

    // parse as empty string (no auto-null value)
    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be I64Parse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 2);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"\"");
}

TEST_F(CSVRowParseTest, QuotedSingleIntegerCell) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"10\"");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 4);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10\"");
}

// need to take this also as example
//     auto res = parse(rg, "\"10\",\"20\"\n");

TEST_F(CSVRowParseTest, QuotedSingleIntegerCellLineBreak) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"10\"\n");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 4); // line stops before first '\n' or '\r;. When parse row is called again, there is an empty line.
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10\""); // \n is trimmed away from result
}

TEST_F(CSVRowParseTest, QuotedSingleIntegerCellOverrun) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"10\",\"\"");

    EXPECT_TRUE(res.ec == ExceptionCode::CSV_OVERRUN);
    EXPECT_EQ(res.numBytesParsed, 7);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10\",\"\"");
}

TEST_F(CSVRowParseTest, QuotedSingleIntegerCellOverrunLineBreak) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"10\",\n");

    EXPECT_TRUE(res.ec == ExceptionCode::CSV_OVERRUN);
    EXPECT_EQ(res.numBytesParsed, 5);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10\",");
}


TEST_F(CSVRowParseTest, QuotedSingleIntegerCellOverrunLineBreakWin) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"10\",\r");

    EXPECT_TRUE(res.ec == ExceptionCode::CSV_OVERRUN);
    EXPECT_EQ(res.numBytesParsed, 5);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10\",");
}

TEST_F(CSVRowParseTest, QuotedSingleIntegerCellConvErrorI) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"10$\"");

    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be I64Parse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 5);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10$\"");
}

TEST_F(CSVRowParseTest, QuotedSingleIntegerCellConvErrorII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    // cell will not be serialized, hence test will pass
    rg.addCell(python::Type::I64, false).build(false);

    auto res = parse(rg, "\"10$\"");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 5);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"10$\"");
}

TEST_F(CSVRowParseTest, QuotedSingleIntegerCellConvErrorIII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"$10\"");

    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be I64Parse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 5);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"$10\"");
}

// whitespace => no error, because that's how python works...
TEST_F(CSVRowParseTest, QuotedSingleIntegerCellConvErrorIV) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "\"\t10   \"\n");
    EXPECT_EQ(res.numBytesParsed, 8);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "\"\t10   \"");
    EXPECT_EQ(getInt(0), 10);
}

// Integer value ---

TEST_F(CSVRowParseTest, SingleIntegerValue) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 2);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "10");

    EXPECT_EQ(getInt(0), 10);
}

TEST_F(CSVRowParseTest, MultiIntegerValueI) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true)
      .addCell(python::Type::I64, true)
     .addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10,\"20\",30");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 10);

    EXPECT_EQ(getInt(0), 10);
    EXPECT_EQ(getInt(1), 20);
    EXPECT_EQ(getInt(2), 30);
}

TEST_F(CSVRowParseTest, MultiIntegerValueII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true)
            .addCell(python::Type::I64, true)
            .addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10,20,30");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 8);

    EXPECT_EQ(getInt(0), 10);
    EXPECT_EQ(getInt(1), 20);
    EXPECT_EQ(getInt(2), 30);
}

TEST_F(CSVRowParseTest, MultiIntegerValueMasked) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true)
            .addCell(python::Type::I64, false) // do not serialize second entry
            .addCell(python::Type::I64, true).build(false);

    auto res = parse(rg, "10,20,\"30\"");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 10);

    EXPECT_EQ(getInt(0), 10);
    EXPECT_EQ(getInt(1), 30);
}

TEST_F(CSVRowParseTest, SingleDigitIntegersI) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true)
            .addCell(python::Type::I64, true)
            .addCell(python::Type::I64, true)
            .addCell(python::Type::I64, true)
            .addCell(python::Type::I64, true)
            .build(false);

    auto res = parse(rg, "0,\"1\",2,3,\"4\"\n");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 13);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "0,\"1\",2,3,\"4\"");

    EXPECT_EQ(getInt(0), 0);
    EXPECT_EQ(getInt(1), 1);
    EXPECT_EQ(getInt(2), 2);
    EXPECT_EQ(getInt(3), 3);
    EXPECT_EQ(getInt(4), 4);
}

TEST_F(CSVRowParseTest, SingleDigitIntegersII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true)
            .build(false);

    auto res = parse(rg, "7");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 1);
    auto s = fromCharPointers(res.lineStart, res.lineEnd);
    EXPECT_EQ(s, "7");

    EXPECT_EQ(getInt(0), 7);
}

// Double value ---

TEST_F(CSVRowParseTest, MultiDoubleValueI) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::F64, true)
            .addCell(python::Type::F64, true)
            .addCell(python::Type::F64, true).build(false);

    auto res = parse(rg, "12.5,\"7.5\",1.0");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 14);

    EXPECT_EQ(getDouble(0), 12.5);
    EXPECT_EQ(getDouble(1), 7.5);
    EXPECT_EQ(getDouble(2), 1.0);
}

TEST_F(CSVRowParseTest, MultiDoubleValueII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::F64, true)
            .addCell(python::Type::F64, true)
            .addCell(python::Type::F64, true).build(false);

    auto res = parse(rg, "\n\r\n12.5,7.5,1.0");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 15);

    EXPECT_EQ(getDouble(0), 12.5);
    EXPECT_EQ(getDouble(1), 7.5);
    EXPECT_EQ(getDouble(2), 1.0);
}

TEST_F(CSVRowParseTest, MultiDoubleValueMasked) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::F64, false)
            .addCell(python::Type::F64, true)
            .addCell(python::Type::F64, false).build(false);

    auto res = parse(rg, "10,20,\"30\"");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 10);

    EXPECT_EQ(getDouble(0), 20);
}

TEST_F(CSVRowParseTest, DoubleParseException) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::F64, false)
            .addCell(python::Type::F64, true)
            .addCell(python::Type::F64, false).build(false);

    auto res = parse(rg, "10,20.34$,\"30\"");

    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be F64Parse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 14);
}

// Boolean -----

TEST_F(CSVRowParseTest, MultiBoolValueI) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::BOOLEAN, true)
            .addCell(python::Type::BOOLEAN, true)
            .addCell(python::Type::BOOLEAN, true).build(false);

    auto res = parse(rg, "TRUE,\"false\",y");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 14);

    EXPECT_EQ(getBool(0), true);
    EXPECT_EQ(getBool(1), false);
    EXPECT_EQ(getBool(2), true);
}

TEST_F(CSVRowParseTest, MultiBoolValueII) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::BOOLEAN, true)
            .addCell(python::Type::BOOLEAN, true)
            .addCell(python::Type::BOOLEAN, true)
            .addCell(python::Type::BOOLEAN, true).build(false);

    auto res = parse(rg, "\n\r\nYes,no,T,f");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 13);

    EXPECT_EQ(getBool(0), true);
    EXPECT_EQ(getBool(1), false);
    EXPECT_EQ(getBool(2), true);
    EXPECT_EQ(getBool(3), false);
}

TEST_F(CSVRowParseTest, MultiBoolValueMasked) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::BOOLEAN, false)
            .addCell(python::Type::BOOLEAN, false)
            .addCell(python::Type::BOOLEAN, true).build(false);

    auto res = parse(rg, "\"TRUE\",false,NO");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 15);

    EXPECT_EQ(getBool(0), false);
}

TEST_F(CSVRowParseTest, BoolParseException) {
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::BOOLEAN, false)
            .addCell(python::Type::BOOLEAN, false)
            .addCell(python::Type::BOOLEAN, true).build(false);

    // note that parsing doesn't happen for fields that are anyways ignored.
    auto res = parse(rg, "true,20.34$,\"falsch!\"");

    EXPECT_TRUE(res.ec == ExceptionCode::VALUEERROR); // used to be BoolParse err, no valuerror b.c. of python
    EXPECT_EQ(res.numBytesParsed, 21);
}

// String value----
TEST_F(CSVRowParseTest, SimpleQuotedString) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true).build(false);
    std::string sText = "\"test\"\" this\"";
    auto res = parse(rg, sText);

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, sText.length());

    auto s = getString(0);
    EXPECT_EQ(getString(0), "test\" this");
}


TEST_F(CSVRowParseTest, SingleQuotedString) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true).build(false);
    std::string sText = "\"quoted text can contain \n \r or \"\"\"";
    auto res = parse(rg, sText);

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, sText.length());

    auto s = getString(0);
    EXPECT_EQ(getString(0), "quoted text can contain \n \r or \"");
}

TEST_F(CSVRowParseTest, SingleUnquotedString) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true).build(false);
    auto res = parse(rg, "hello");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 5);

    auto s = getString(0);
    EXPECT_EQ(getString(0), "hello");
}

TEST_F(CSVRowParseTest, SingleCharacterString) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true).build(false);
    auto res = parse(rg, "a");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 1);

    auto s = getString(0);
    EXPECT_EQ(getString(0), "a");
}

TEST_F(CSVRowParseTest, SingleQuotedCharacterString) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true).build(false);
    auto res = parse(rg, "\"a\"");

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, 3);

    auto s = getString(0);
    EXPECT_EQ(getString(0), "a");
}


TEST_F(CSVRowParseTest, MultiStringValueI) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true)
            .addCell(python::Type::STRING, true)
            .addCell(python::Type::STRING, true).build(false);
    std::string sText = "some text here,\"quoted text can contain \n \r or \"\"\",Hello world!";
    auto res = parse(rg, sText);

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, sText.length());

    EXPECT_EQ(getString(0), "some text here");
    EXPECT_EQ(getString(1), "quoted text can contain \n \r or \"");
    EXPECT_EQ(getString(2), "Hello world!");
}

TEST_F(CSVRowParseTest, MultiDequote) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true)
            .addCell(python::Type::STRING, true)
            .addCell(python::Type::STRING, true).build(false);
    std::string sText = "\"ab\"\"\",\"\n\"\"\",\"\"\"haha\"\"\"";
    auto res = parse(rg, sText);

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, sText.length());

    EXPECT_EQ(getString(0), "ab\"");
    EXPECT_EQ(getString(1), "\n\"");
    EXPECT_EQ(getString(2), "\"haha\"");
}

TEST_F(CSVRowParseTest, MultiStringMasked) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true)
            .addCell(python::Type::STRING, false)
            .addCell(python::Type::STRING, true).build(false);
    std::string sText = "some text here,ignore this,\"speaking in \"\" is stupid\"";
    auto res = parse(rg, sText);

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, sText.length());

    EXPECT_EQ(getString(0), "some text here");
    EXPECT_EQ(getString(1), "speaking in \" is stupid");
}

TEST_F(CSVRowParseTest, DoubleQuoteError) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::STRING, true).build(false);
    auto res = parse(rg, "\"user forgot to close doublequote");

    EXPECT_TRUE(res.ec == ExceptionCode::DOUBLEQUOTEERROR);
    EXPECT_EQ(res.numBytesParsed, strlen("\"user forgot to close doublequote"));
}

TEST_F(CSVRowParseTest, LargeMultiValTest) {
    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
    tuplex::codegen::CSVParseRowGenerator rg(env.get());

    rg.addCell(python::Type::I64, true)
            .addCell(python::Type::STRING, false)
            .addCell(python::Type::F64, true)
            .addCell(python::Type::BOOLEAN, false)
            .addCell(python::Type::STRING, true).build(false);
    std::string sText = "1234, dhfgj,-20,WRONG,\"\"\"hello!\"\"\"\n\rjsdhhgjdshg";
    auto res = parse(rg, sText);

    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
    EXPECT_EQ(res.numBytesParsed, strlen("1234, dhfgj,-20,WRONG,\"\"\"hello!\"\"\""));

    EXPECT_EQ(getInt(0), 1234);
    EXPECT_EQ(getDouble(1), -20.0);
    EXPECT_EQ(getString(2), "\"hello!\"");
}

// Notes: update parser with recent version from csvmonkey.hpp
// --> if startPtr=EndPtr this should be a CSV underrun
// --> empty string, i.e. endPtr = startPtr + 1 and *startPtr = '\0' is ok






#warning "this test here does not work yet, fix later.."
//// @Todo: this test fails...
//// should be fixed for benchmarking
//// because Tuplex profits against this file...
//TEST_F(CSVRowParseTest, SingleQuotedJSONString) {
//    tuplex::Context c(microTestOptions()); // important for the runtime string allocations!
//    CSVParseRowGenerator rg(env.get());
//
//    rg.addCell(python::Type::STRING, true).build(false);
//    std::string sText = "\"{\n"
//                        "   \"\"results\"\" : [\n"
//                        "      {\n"
//                        "         \"\"address_components\"\" : [\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"7\"\",\n"
//                        "               \"\"short_name\"\" : \"\"7\"\",\n"
//                        "               \"\"types\"\" : [ \"\"street_number\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Parker Street\"\",\n"
//                        "               \"\"short_name\"\" : \"\"Parker St\"\",\n"
//                        "               \"\"types\"\" : [ \"\"route\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Woburn\"\",\n"
//                        "               \"\"short_name\"\" : \"\"Woburn\"\",\n"
//                        "               \"\"types\"\" : [ \"\"locality\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Middlesex County\"\",\n"
//                        "               \"\"short_name\"\" : \"\"Middlesex County\"\",\n"
//                        "               \"\"types\"\" : [ \"\"administrative_area_level_2\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Massachusetts\"\",\n"
//                        "               \"\"short_name\"\" : \"\"MA\"\",\n"
//                        "               \"\"types\"\" : [ \"\"administrative_area_level_1\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"United States\"\",\n"
//                        "               \"\"short_name\"\" : \"\"US\"\",\n"
//                        "               \"\"types\"\" : [ \"\"country\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"01801\"\",\n"
//                        "               \"\"short_name\"\" : \"\"01801\"\",\n"
//                        "               \"\"types\"\" : [ \"\"postal_code\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"5934\"\",\n"
//                        "               \"\"short_name\"\" : \"\"5934\"\",\n"
//                        "               \"\"types\"\" : [ \"\"postal_code_suffix\"\" ]\n"
//                        "            }\n"
//                        "         ],\n"
//                        "         \"\"formatted_address\"\" : \"\"7 Parker St, Woburn, MA 01801, USA\"\",\n"
//                        "         \"\"geometry\"\" : {\n"
//                        "            \"\"bounds\"\" : {\n"
//                        "               \"\"northeast\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4625328,\n"
//                        "                  \"\"lng\"\" : -71.1781141\n"
//                        "               },\n"
//                        "               \"\"southwest\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4624087,\n"
//                        "                  \"\"lng\"\" : -71.1782546\n"
//                        "               }\n"
//                        "            },\n"
//                        "            \"\"location\"\" : {\n"
//                        "               \"\"lat\"\" : 42.4624557,\n"
//                        "               \"\"lng\"\" : -71.17820139999999\n"
//                        "            },\n"
//                        "            \"\"location_type\"\" : \"\"ROOFTOP\"\",\n"
//                        "            \"\"viewport\"\" : {\n"
//                        "               \"\"northeast\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4638197302915,\n"
//                        "                  \"\"lng\"\" : -71.17683536970851\n"
//                        "               },\n"
//                        "               \"\"southwest\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4611217697085,\n"
//                        "                  \"\"lng\"\" : -71.17953333029152\n"
//                        "               }\n"
//                        "            }\n"
//                        "         },\n"
//                        "         \"\"partial_match\"\" : true,\n"
//                        "         \"\"place_id\"\" : \"\"ChIJdZ-Seu5144kR5v7QlCjTD7g\"\",\n"
//                        "         \"\"types\"\" : [ \"\"premise\"\" ]\n"
//                        "      }\n"
//                        "   ],\n"
//                        "   \"\"status\"\" : \"\"OK\"\"\n"
//                        "}\n"
//                        "\"\n"
//                        "\"{\n"
//                        "   \"\"results\"\" : [\n"
//                        "      {\n"
//                        "         \"\"address_components\"\" : [\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"88\"\",\n"
//                        "               \"\"short_name\"\" : \"\"88\"\",\n"
//                        "               \"\"types\"\" : [ \"\"street_number\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Burlington Street\"\",\n"
//                        "               \"\"short_name\"\" : \"\"Burlington St\"\",\n"
//                        "               \"\"types\"\" : [ \"\"route\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Woburn\"\",\n"
//                        "               \"\"short_name\"\" : \"\"Woburn\"\",\n"
//                        "               \"\"types\"\" : [ \"\"locality\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Middlesex County\"\",\n"
//                        "               \"\"short_name\"\" : \"\"Middlesex County\"\",\n"
//                        "               \"\"types\"\" : [ \"\"administrative_area_level_2\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"Massachusetts\"\",\n"
//                        "               \"\"short_name\"\" : \"\"MA\"\",\n"
//                        "               \"\"types\"\" : [ \"\"administrative_area_level_1\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"United States\"\",\n"
//                        "               \"\"short_name\"\" : \"\"US\"\",\n"
//                        "               \"\"types\"\" : [ \"\"country\"\", \"\"political\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"01801\"\",\n"
//                        "               \"\"short_name\"\" : \"\"01801\"\",\n"
//                        "               \"\"types\"\" : [ \"\"postal_code\"\" ]\n"
//                        "            },\n"
//                        "            {\n"
//                        "               \"\"long_name\"\" : \"\"3915\"\",\n"
//                        "               \"\"short_name\"\" : \"\"3915\"\",\n"
//                        "               \"\"types\"\" : [ \"\"postal_code_suffix\"\" ]\n"
//                        "            }\n"
//                        "         ],\n"
//                        "         \"\"formatted_address\"\" : \"\"88 Burlington St, Woburn, MA 01801, USA\"\",\n"
//                        "         \"\"geometry\"\" : {\n"
//                        "            \"\"bounds\"\" : {\n"
//                        "               \"\"northeast\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4788356,\n"
//                        "                  \"\"lng\"\" : -71.1708421\n"
//                        "               },\n"
//                        "               \"\"southwest\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4787259,\n"
//                        "                  \"\"lng\"\" : -71.1710051\n"
//                        "               }\n"
//                        "            },\n"
//                        "            \"\"location\"\" : {\n"
//                        "               \"\"lat\"\" : 42.4787819,\n"
//                        "               \"\"lng\"\" : -71.17091429999999\n"
//                        "            },\n"
//                        "            \"\"location_type\"\" : \"\"ROOFTOP\"\",\n"
//                        "            \"\"viewport\"\" : {\n"
//                        "               \"\"northeast\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.48012973029149,\n"
//                        "                  \"\"lng\"\" : -71.1695746197085\n"
//                        "               },\n"
//                        "               \"\"southwest\"\" : {\n"
//                        "                  \"\"lat\"\" : 42.4774317697085,\n"
//                        "                  \"\"lng\"\" : -71.17227258029151\n"
//                        "               }\n"
//                        "            }\n"
//                        "         },\n"
//                        "         \"\"partial_match\"\" : true,\n"
//                        "         \"\"place_id\"\" : \"\"ChIJ75qL-ph144kRvFjAL_NRJvQ\"\",\n"
//                        "         \"\"types\"\" : [ \"\"premise\"\" ]\n"
//                        "      }\n"
//                        "   ],\n"
//                        "   \"\"status\"\" : \"\"OK\"\"\n"
//                        "}\n"
//                        "\"";
//    auto res = parse(rg, sText);
//
//    EXPECT_TRUE(res.ec == ExceptionCode::SUCCESS);
//    EXPECT_EQ(res.numBytesParsed, sText.length());
//
//    auto s = getString(0);
//    EXPECT_EQ(getString(0), "quoted text can contain \n \r or \"");
//}