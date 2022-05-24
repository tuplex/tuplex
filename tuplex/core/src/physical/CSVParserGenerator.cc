//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "physical/CSVParserGenerator.h"
#include <Logger.h>
#include <ExceptionCodes.h>
#include <Base.h>

namespace tuplex {

    namespace codegen {
        CSVParserGenerator& CSVParserGenerator::addCell(const python::Type &type, bool serialize) {
            _rowGenerator.addCell(type, serialize);
            return *this;
        }

        void CSVParserGenerator::build(IExceptionableTaskGenerator::reqMemory_f requestOutputMemory) {
            using namespace llvm;
            auto& context = _env->getContext();
            auto i8ptr_type = Type::getInt8PtrTy(context, 0);

            // create parse row function
            _rowGenerator.build();

            createTask("parse_csv", nullptr); // no exception handling yet.

            auto parseRowF = _rowGenerator.getFunction();
            auto oldBuilder = getBuilder();

            // create all basic blocks for the outcomes of a line parse
            BasicBlock *bBody = BasicBlock::Create(context, "setup", getFunction());
            BasicBlock *bLoopCond = BasicBlock::Create(context, "loopCond", getFunction());
            BasicBlock *bLoopDone = BasicBlock::Create(context, "loopDone", getFunction());
            BasicBlock *bLoopBody = BasicBlock::Create(context, "loopBody", getFunction());
            BasicBlock *bRowDone = BasicBlock::Create(context, "row_done", getFunction());


            // create some preliminary things
            auto endPtr = oldBuilder.CreateGEP(getInputPtrArg(), getInputSizeArg());

            oldBuilder.CreateBr(bBody);

            IRBuilder builder(bBody);

            // setup here all variables necessary for the parsing
            _resStructVar = builder.CreateAlloca(_rowGenerator.resultType(), 0, nullptr, "resultVar");
            _currentPtrVar = builder.CreateAlloca(i8ptr_type, 0, nullptr, "currentPtr");
            builder.CreateStore(getInputPtrArg(), _currentPtrVar);
            _endPtr = endPtr;


            // if skipHeader is true, skip first row
            // !!! there is no header validation/order etc. here.
            if(_skipHeader) {
                auto parseCode = builder.CreateCall(parseRowF, {_resStructVar, builder.CreateLoad(_currentPtrVar), _endPtr});
                auto numParsedBytes = builder.CreateLoad(builder.CreateGEP(_resStructVar, {_env->i32Const(0), _env->i32Const(0)}));

                // inc ptr & go to loop cond
                builder.CreateStore(builder.CreateGEP(builder.CreateLoad(_currentPtrVar), numParsedBytes), _currentPtrVar);
            }

            builder.CreateBr(bLoopCond);


            // loop condition, i.e. p < endp
            builder.SetInsertPoint(bLoopCond);
            auto cond = builder.get().CreateICmpULT(builder.get().CreatePtrToInt(builder.CreateLoad(_currentPtrVar), _env->i64Type()),
                                              builder.get().CreatePtrToInt(_endPtr, _env->i64Type()));
            builder.CreateCondBr(cond, bLoopBody, bLoopDone);


            // loop body
            builder.SetInsertPoint(bLoopBody);
            //call func and advance ptr

            auto parseCode = builder.CreateCall(parseRowF, {_resStructVar, builder.CreateLoad(_currentPtrVar), _endPtr});
            _env->debugPrint(builder.get(), "parseCode is ", parseCode);
            auto numParsedBytes = builder.CreateLoad(builder.CreateGEP(_resStructVar, {_env->i32Const(0), _env->i32Const(0)}));

            // inc ptr & go to loop cond with next blocks
            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(_currentPtrVar), numParsedBytes), _currentPtrVar);

            // ignore empty results at end
            // maybe add assert that lineEnd is >= endPtr
            auto emptyResultCond = builder.CreateICmpEQ(numParsedBytes, _env->i64Const(0));
            BasicBlock *bNonEmpty = BasicBlock::Create(context, "non_empty_result", getFunction());
            builder.CreateCondBr(emptyResultCond, bLoopCond, bNonEmpty);
            builder.SetInsertPoint(bNonEmpty);

            // can only stuff if bytes were parsed!
            auto lineStart = builder.CreateLoad(builder.CreateGEP(_resStructVar, {_env->i32Const(0), _env->i32Const(1)}));
            auto lineEnd = builder.CreateLoad(builder.CreateGEP(_resStructVar, {_env->i32Const(0), _env->i32Const(2)}));

            // check result code, if zero all ok. Else, go into exception handling
            BasicBlock *bNoException = BasicBlock::Create(context, "no_exception", getFunction());
            BasicBlock *bException = BasicBlock::Create(context, "exception", getFunction());

            auto exceptionCond = builder.CreateICmpEQ(parseCode, _env->i32Const(ecToI32(ExceptionCode::SUCCESS)));

            builder.CreateCondBr(exceptionCond, bNoException, bException);

            // exception block
            builder.SetInsertPoint(bException);

            // call handler
            if(_handler) {
                // create call to exception handler
                std::vector<Type *> eh_argtypes{Type::getInt8PtrTy(context, 0),
                                                _env->i64Type(),
                                                _env->i64Type(),
                                                _env->i64Type(),
                                                Type::getInt8PtrTy(context, 0),
                                                _env->i64Type()};
                FunctionType *eh_FT = FunctionType::get(Type::getVoidTy(context), eh_argtypes, false);

                auto eh_func_ptr_type = PointerType::get(eh_FT, 0);

                auto eh_func_addr = _env->i64Const(reinterpret_cast<int64_t>(_handler));

                auto eh_func = builder.get().CreateIntToPtr(eh_func_addr, eh_func_ptr_type, "exceptionHandler");


                // parameters for the call
                auto ehcode = builder.get().CreateSExt(parseCode, _env->i64Type());
                auto ehopid = _env->i64Const(_operatorID);
                auto row = getRowNumber(builder);
                auto inputlength = builder.CreateSub(builder.get().CreatePtrToInt(lineEnd, _env->i64Type()),
                                                     builder.get().CreatePtrToInt(lineStart, _env->i64Type()));

                auto inputptr = lineStart;
                std::vector<llvm::Value *> eh_parameters{getUserDataArg(), ehcode, ehopid, row, inputptr,
                                                         inputlength};
                builder.CreateCall(eh_func, eh_parameters);
            }

            // row done block, i.e. inc row counter!
            // row done goes to bLoopCond
            // branch back to loop
            builder.CreateBr(bRowDone);

            // no exception block
            builder.SetInsertPoint(bNoException);

            // serialize what needs to be serialized
            auto resStructVal = _resStructVar;
            auto stype = _rowGenerator.serializedType();
            bool varFieldEncountered = false;
            int pos = 0;
            auto numFields = stype.parameters().size();

            FlattenedTuple ft(_env.get());
            ft.init(stype);

#warning "this here is outdated... should not be used. Remove code"
            for(const auto& t : stype.parameters()) {
                Value* val = builder.CreateLoad(builder.CreateGEP(resStructVal, {_env->i32Const(0), _env->i32Const(3 + 2 * pos)}));
                Value* size = builder.CreateLoad(builder.CreateGEP(resStructVal, {_env->i32Const(0), _env->i32Const(3 + 2 * pos + 1)}));

                // !!! zero terminated string
                if(python::Type::STRING == t)
                    // safely zero terminate strings before further processing...
                    // this will lead to some copies that are unavoidable...
                    val = _env->zeroTerminateString(builder.get(), val, size);

                ft.set(builder.get(), {pos}, val, size, nullptr);
                pos++;
            }

            // note that in order to avoid memcpy, strings that are not dequoted (hence runtime allocated) are not zero terminated.
            // need to force zero termination!
            ft.enableForcedZeroTerminatedStrings();

            assert(requestOutputMemory);
            setLastBlock(builder.GetInsertBlock());
            toMemory(requestOutputMemory, ft);
            builder.SetInsertPoint(lastBlock());

            // normal case serialization done, inc Row
            builder.CreateBr(bRowDone);

            builder.SetInsertPoint(bLoopDone);
            builder.CreateBr(taskSuccessBlock());

            // row done block (--> actually can save this block but would require to insert command twice)
            builder.SetInsertPoint(bRowDone);
            incRowNumber(builder);
            builder.CreateBr(bLoopCond);
        }
    }
}