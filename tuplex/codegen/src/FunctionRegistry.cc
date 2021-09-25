//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <FunctionRegistry.h>
#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#include <pcre2.h>
#include <cmath>

namespace llvm {
    // helper functions

    static CallInst *createCallHelper(Function *Callee, ArrayRef<Value*> Ops,
                                      IRBuilder<>& builder,
                                      const Twine &Name = "",
                                      Instruction *FMFSource = nullptr) {
        CallInst *CI = CallInst::Create(Callee, Ops, Name);
        if (FMFSource)
            CI->copyFastMathFlags(FMFSource);
        builder.GetInsertBlock()->getInstList().insert(builder.GetInsertPoint(), CI);
        builder.SetInstDebugLocation(CI);
        return CI;
    }

    CallInst* createUnaryIntrinsic(IRBuilder<>& builder,
                                   Intrinsic::ID ID,
                                   Value *V,
                                   const Twine& Name="",
                                   Instruction *FMFSource = nullptr) {
        Module *M = builder.GetInsertBlock()->getModule();
        Function *Fn = Intrinsic::getDeclaration(M, ID, {V->getType()});
        return createCallHelper(Fn, {V}, builder, Name, FMFSource);
    }

    CallInst* createBinaryIntrinsic(IRBuilder<>& builder,
                                    Intrinsic::ID ID,
                                    Value *LHS, Value* RHS,
                                    const Twine& Name="",
                                    Instruction *FMFSource = nullptr) {
        Module *M = builder.GetInsertBlock()->getModule();
        assert(M);
        Function *Fn = Intrinsic::getDeclaration(M, ID, {LHS->getType()});
        assert(Fn);
        return createCallHelper(Fn, {LHS, RHS}, builder, Name, FMFSource);
    }
}

namespace tuplex {
    namespace codegen {

        // helper functions:

        // a function is constructed in the following standard way in Tuplex:
        // i64 func(rettype* ptr, arg1, arg2, ..., argn, arg1_size, ..., argn_size)
        // this allows for failures as well.
        // that general model is basically required for true exception handling...
        // maybe give details in implementation...

        // @Todo: this sucks. Should be different. Should be, create call for functions & then directly code stuff...

        llvm::Function* createStringLenFunction(LLVMEnvironment& env) {
            using namespace llvm;

            // simple function:
            // Taking i8* as input and i64 for size of i8*

            FunctionType *ft = FunctionType::get(env.i64Type(), {env.i8ptrType(), env.i64Type()}, false);

            Function *func = Function::Create(ft, Function::InternalLinkage, "strLen", env.getModule().get());
            // set inline attributes
            AttrBuilder ab;
            ab.addAttribute(Attribute::AlwaysInline);
            func->addAttributes(llvm::AttributeList::FunctionIndex, ab);


            std::vector<llvm::Argument*> args;
            for(auto& arg : func->args())
                args.push_back(&arg);
            assert(args.size() == 2);

            args[0]->setName("ptr");
            args[1]->setName("ptr_size");

            // create basic block & simple return
            BasicBlock* bb = BasicBlock::Create(env.getContext(), "body", func);
            IRBuilder<> builder(bb);

            // simple return: just size - 1
            llvm::Value* size = args[1];
            builder.CreateRet(builder.CreateSub(size, env.i64Const(1)));

            return func;
        }

        llvm::Function* createStringUpperFunction(LLVMEnvironment& env) {
            using namespace llvm;

            // implemeneted in runtime
            FunctionType *ft = FunctionType::get(env.i8ptrType(), {env.i64Type()}, false);

            auto func = env.getModule()->getOrInsertFunction("strUpper", ft);

            return nullptr;
        }


        SerializableValue FunctionRegistry::createLenCall(llvm::IRBuilder<>& builder,
                const python::Type &argsType,
                const python::Type &retType,
                const std::vector<tuplex::codegen::SerializableValue> &args) {
            auto& logger = Logger::instance().logger("codegen");

            llvm::Value* i64Size = _env.i64Const(sizeof(int64_t));

            // check correct types
            if(argsType.parameters().size() != 1) {
                Logger::instance().defaultLogger().error("len call needs single argument");
                return SerializableValue(nullptr, nullptr);
            }

            python::Type argType = argsType.parameters().front();

            // Option[...] case
            if(argType.isOptionType())
                argType = argType.elementType();

            // only two things supported:
            // string and tuple
            // can both be done easily!
            if(argType == python::Type::STRING) {
                // easy, return size - 1
                // i.e. done via complete inlining...
                return SerializableValue(builder.CreateSub(args.front().size, _env.i64Const(1)), i64Size);

            } else if(argType.isTupleType()) {
                // simple constant
                return SerializableValue(_env.i64Const(argType.parameters().size()), i64Size);
            } else if (argType.isDictionaryType() || argType == python::Type::GENERICDICT) {
                auto obj_size = builder.CreateCall(
                        cJSONGetArraySize_prototype(_env.getContext(), _env.getModule().get()),
                        {args.front().val});
                return SerializableValue(obj_size, i64Size);
            } else if(argType.isListType() || argType == python::Type::GENERICLIST) {
                if(argType == python::Type::EMPTYLIST) {
                    return SerializableValue(_env.i64Const(0), _env.i64Const(8));
                }
                assert(args.front().val);
                auto name = _env.getLLVMTypeName(args.front().val->getType());
                return SerializableValue(_env.getListSize(builder, args.front().val, argType), _env.i64Const(sizeof(int64_t)));
            } else {
                logger.error("TypeError: object of type " + argType.desc() + " has no len()");
                return SerializableValue();
            }
        }


        SerializableValue FunctionRegistry::createIntCast(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                          llvm::IRBuilder<> &builder, python::Type argsType,
                                                          const std::vector<tuplex::codegen::SerializableValue> &args) {

            auto& logger = Logger::instance().logger("codegen");

            llvm::Value* i64Size = _env.i64Const(sizeof(int64_t));

            // trivial case:
            // int(), i.e. constructor
            if(args.empty())
                return SerializableValue(_env.i64Const(0), i64Size);

            // first check how many args.
            // different bases not supported
            if(args.size() != 1) {
                logger.error("no support for base in int(...) call yet");
                return SerializableValue();
            }

            auto type = argsType.parameters().front();

            if(python::Type::BOOLEAN == type) {
                // zero extent to 64bit
                auto new_val = builder.CreateZExt(args.front().val, _env.i64Type());
                return SerializableValue(new_val, i64Size);

            } else if(python::Type::I64 == type) {
                // nothing todo, simply return
                return args.front();

            } else if(python::Type::F64 == type) {

                // cast fp to int
                auto new_val = builder.CreateFPToSI(args.front().val, _env.i64Type());
                return SerializableValue(new_val, i64Size);

            } else if(python::Type::STRING == type) {

                // most interesting example, simply use fast_atoi64 function from runtime to make this here happen
                auto i8ptr_type = _env.i8ptrType();
                std::vector<llvm::Type*> argtypes{i8ptr_type, i8ptr_type, _env.i64Type()->getPointerTo(0)};
                llvm::FunctionType *FT = llvm::FunctionType::get(_env.i32Type(), argtypes, false);
                auto func = _env.getModule().get()->getOrInsertFunction("fast_atoi64", FT);

                auto value = builder.CreateAlloca(_env.i64Type(), 0, nullptr);

                auto strBegin = args.front().val;
                auto strEnd = builder.CreateGEP(strBegin, builder.CreateSub(args.front().size, _env.i64Const(1)));
                auto resCode = builder.CreateCall(func, {strBegin, strEnd, value});

                // Option I: use internal Tuplex codes
                // lfb.addException(builder, resCode, nullptr);

                // Option II: always return ValueError as in python originally
                auto cond = builder.CreateICmpNE(resCode, _env.i32Const(ecToI32(ExceptionCode::SUCCESS)));
                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);

                // changed builder, now return normal/positive result
                return SerializableValue(builder.CreateLoad(value), i64Size);
            } else {
                logger.error("not support for objects of type " + type.desc() + " in int(...) call");
                return SerializableValue();
            }

        }

        SerializableValue FunctionRegistry::createDictConstructor(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                          llvm::IRBuilder<> &builder, python::Type argsType,
                                                          const std::vector<tuplex::codegen::SerializableValue> &args) {
            auto& logger = Logger::instance().logger("codegen");

            // constructor:
            if(args.empty()) {
                auto emptydict = builder.CreateCall(cJSONCreateObject_prototype(_env.getContext(), _env.getModule().get()), {});
                auto dictsize = _env.i64Const(sizeof(cJSON));
                return SerializableValue(emptydict, dictsize);
            }

            logger.error("no support for nonempty dict() constructor yet");
            return SerializableValue();
        }

        void FunctionRegistry::getValueFromcJSON(llvm::IRBuilder<> &builder, llvm::Value* cjson_val, python::Type retType,
                llvm::Value* retval, llvm::Value* retsize) {
            llvm::Value *val, *size;
            if(retType == python::Type::BOOLEAN) {
                // BOOL: in type
                auto isTrue = builder.CreateCall(
                        cJSONIsTrue_prototype(_env.getContext(), _env.getModule().get()),
                        {cjson_val});
                val = _env.upcastToBoolean(builder, builder.CreateICmpEQ(isTrue, _env.i64Const(1)));
                size = _env.i64Const(8);;
            }
            else if(retType == python::Type::STRING) {
                // STRING: 32 bytes offset
                auto valaddr = builder.CreateGEP(cjson_val, _env.i64Const(32));
                auto valptr = builder.CreatePointerCast(valaddr, llvm::Type::getInt64PtrTy(_env.getContext()));
                auto valload = builder.CreateLoad(valptr);
                val = builder.CreateCast(llvm::Instruction::CastOps::IntToPtr, valload, _env.i8ptrType());
                auto len = builder.CreateCall(strlen_prototype(_env.getContext(), _env.getModule().get()), {val});
                size = builder.CreateAdd(len, _env.i64Const(1));
            }
            else if(retType == python::Type::I64) {
                // Integer: 40 bytes offset
                auto valaddr = builder.CreateGEP(cjson_val, _env.i64Const(40));
                auto valptr = builder.CreatePointerCast(valaddr, llvm::Type::getInt64PtrTy(_env.getContext()));
                val = builder.CreateLoad(llvm::Type::getInt64Ty(_env.getContext()), valptr);
                size = _env.i64Const(8);
            }
            else if(retType == python::Type::F64) {
                // Double: 48 bytes offset
                auto valaddr = builder.CreateGEP(cjson_val, _env.i64Const(48));
                auto valptr = builder.CreatePointerCast(valaddr, llvm::Type::getDoublePtrTy(_env.getContext()));
                val = builder.CreateLoad(llvm::Type::getDoubleTy(_env.getContext()), valptr);
                size = _env.i64Const(8);
            }
            else throw "Invalid return type for dict.pop(): " + retType.desc();
            builder.CreateStore(val, retval);
            builder.CreateStore(size, retsize);
        }

        // TODO: probably need to use cJSON_DetachItemFromObjectCaseSensistive to make sure pop deletes the item - then we need to recalculate the serialized size
        SerializableValue FunctionRegistry::createCJSONPopCall(LambdaFunctionBuilder& lfb,
                                                          llvm::IRBuilder<> &builder,
                                                          const tuplex::codegen::SerializableValue &caller,
                                                          const std::vector<tuplex::codegen::SerializableValue> &args,
                                                          const std::vector<python::Type> &argsTypes,
                                                          const python::Type& retType) {
            assert(args.size() == 1 || args.size() == 2);
            if(args.size() == 2) assert(argsTypes[1] == retType);
            auto key = dictionaryKey(_env.getContext(), _env.getModule().get(), builder, args[0].val, argsTypes[0], retType);
            if(key == nullptr) return SerializableValue();

            auto cjson_val = builder.CreateCall(
                    cJSONGetObjectItem_prototype(_env.getContext(), _env.getModule().get()), {caller.val, key});

            // blocks
            auto keyExistBlock = llvm::BasicBlock::Create(_env.getContext(), "keyexists", builder.GetInsertBlock()->getParent());
            auto keyDNEBlock = llvm::BasicBlock::Create(_env.getContext(), "keydne", builder.GetInsertBlock()->getParent());
            auto retBlock = llvm::BasicBlock::Create(_env.getContext(), "retblock", builder.GetInsertBlock()->getParent());
            // local variables
            auto retsize = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
            llvm::AllocaInst* retval;
            // allocate retval properly
            if(retType == python::Type::BOOLEAN) retval = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);
            else if(retType == python::Type::STRING) retval = builder.CreateAlloca(_env.i8ptrType(), 0, nullptr);
            else if(retType == python::Type::I64) retval = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
            else if(retType == python::Type::F64) retval = builder.CreateAlloca(_env.doubleType(), 0, nullptr);
            else throw "Invalid return type for dict.pop(): " + retType.desc();

            auto keyExists = builder.CreateIsNotNull(cjson_val);
            builder.CreateCondBr(keyExists, keyExistBlock, keyDNEBlock);

            builder.SetInsertPoint(keyExistBlock);
            getValueFromcJSON(builder, cjson_val, retType, retval, retsize);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(keyDNEBlock);
            if(args.size() == 1) {
                lfb.addException(builder, ExceptionCode::KEYERROR, llvm::ConstantInt::get(_env.getContext(), llvm::APInt(1, 1)));
            }
            else if(args.size() == 2) {
                builder.CreateStore(args[1].val, retval);
                builder.CreateStore(args[1].size, retsize);
            }
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(retBlock);
            auto ret = SerializableValue(builder.CreateLoad(retval), builder.CreateLoad(retsize));
            lfb.setLastBlock(retBlock);
            return ret;
        }

        SerializableValue FunctionRegistry::createCJSONPopItemCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller,
                                            const python::Type &retType) {
            // local variables
            auto retsize = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
            llvm::AllocaInst *retval;
            // allocate retval properly
            if (retType.parameters()[1] == python::Type::BOOLEAN)
                retval = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);
            else if (retType.parameters()[1] == python::Type::STRING)
                retval = builder.CreateAlloca(_env.i8ptrType(), 0, nullptr);
            else if (retType.parameters()[1] == python::Type::I64)
                retval = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
            else if (retType.parameters()[1] == python::Type::F64)
                retval = builder.CreateAlloca(_env.doubleType(), 0, nullptr);
            else throw "Invalid return type for dict.pop(): " + retType.parameters()[1].desc();

            // retrieve child pointer
            auto valobjaddr = builder.CreateGEP(caller.val, _env.i64Const(16));
            auto valobjptr = builder.CreatePointerCast(valobjaddr, llvm::Type::getInt64PtrTy(_env.getContext()));
            auto valobjload = builder.CreateLoad(valobjptr);
            auto valobj = builder.CreateCast(llvm::Instruction::CastOps::IntToPtr, valobjload,
                                             _env.i8ptrType()); // child pointer
            auto nonempty_dict = builder.CreateIsNull(valobj);
            lfb.addException(builder, ExceptionCode::KEYERROR, nonempty_dict);

            // there is a value to return
            builder.CreateCall(cJSONDetachItemViaPointer_prototype(_env.getContext(), _env.getModule().get()),
                               {caller.val, valobj});
            getValueFromcJSON(builder, valobj, retType.parameters()[1], retval, retsize);
            // get key of removed item
            auto keyaddr = builder.CreateGEP(valobj, _env.i64Const(56));
            auto keyptr = builder.CreatePointerCast(keyaddr, llvm::Type::getInt64PtrTy(_env.getContext()));
            auto keyload = builder.CreateLoad(keyptr);
            auto keystr = builder.CreateCast(llvm::Instruction::CastOps::IntToPtr, keyload,
                                          _env.i8ptrType()); // key string
            auto key = dictionaryKeyCast(_env.getContext(), _env.getModule().get(), builder, keystr, retType.parameters()[0]);
            // create tuple (key, val)
            FlattenedTuple ft(&_env);
            ft.init(retType);
            ft.setElement(builder, 0, key.val, key.size, key.is_null);
            ft.setElement(builder, 1, builder.CreateLoad(retval), builder.CreateLoad(retsize), nullptr); // non-null result!

            auto ret = ft.getLoad(builder);
            assert(ret->getType()->isStructTy());
            auto size = ft.getSize(builder);
            return SerializableValue(ret, size);
        }

        SerializableValue FunctionRegistry::createFloatCast(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                          llvm::IRBuilder<> &builder, python::Type argsType,
                                                          const std::vector<tuplex::codegen::SerializableValue> &args) {

            auto& logger = Logger::instance().logger("codegen");
            llvm::Value* f64Size = _env.i64Const(sizeof(double));

            // constructor
            if(args.empty())
                return SerializableValue(_env.f64Const(0.0), f64Size);

            // first check how many args - multiple args is a syntax error
            if(args.size() != 1) {
                logger.error("float([x]) takes at most 1 argument");
                return SerializableValue();
            }

            auto type = argsType.parameters().front();

            assert(args.front().val);

            if(python::Type::BOOLEAN == type) { // cast to float
                auto new_val = builder.CreateSIToFP(args.front().val, _env.doubleType());
                return SerializableValue(new_val, f64Size);

            } else if(python::Type::I64 == type) { // cast int to fp
                auto new_val = builder.CreateSIToFP(args.front().val, _env.doubleType());
                return SerializableValue(new_val, f64Size);

            } else if(python::Type::F64 == type) { // return value as is
                return args.front();

            } else if(python::Type::STRING == type) { // use fast_atod
                auto i8ptr_type = _env.i8ptrType();
                std::vector<llvm::Type*> argtypes{i8ptr_type, i8ptr_type, _env.doubleType()->getPointerTo(0)};
                llvm::FunctionType *FT = llvm::FunctionType::get(_env.i32Type(), argtypes, false);
                auto func = _env.getModule().get()->getOrInsertFunction("fast_atod", FT);

                auto value = builder.CreateAlloca(_env.doubleType(), 0, nullptr);

                auto strBegin = args.front().val;
                auto strEnd = builder.CreateGEP(strBegin, builder.CreateSub(args.front().size, _env.i64Const(1)));
                auto resCode = builder.CreateCall(func, {strBegin, strEnd, value});

                auto cond = builder.CreateICmpNE(resCode, _env.i32Const(ecToI32(ExceptionCode::SUCCESS)));
                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);

                // changed builder, now return normal/positive result
                return SerializableValue(builder.CreateLoad(value), f64Size);
            } else {
                logger.error("objects of type " + type.desc() + " are not supported in float(...) call");
                return SerializableValue();
            }

        }

        SerializableValue FunctionRegistry::createBoolCast(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                          llvm::IRBuilder<> &builder, python::Type argsType,
                                                          const std::vector<tuplex::codegen::SerializableValue> &args) {

            auto& logger = Logger::instance().logger("codegen");

            llvm::Value* boolSize = _env.i64Const(_env.getBooleanType()->getIntegerBitWidth());

            // constructor
            if(args.empty())
                return SerializableValue(_env.boolConst(false), boolSize);

            // first check how many args.
            if(args.size() != 1) {
                logger.error("bool([x]) takes at most 1 argument");
                return SerializableValue();
            }

            auto type = argsType.parameters().front();

            if(python::Type::BOOLEAN == type) { // return as is
                return args.front();

            } else if(python::Type::I64 == type) {
                auto is_zero = builder.CreateICmpEQ(args.front().val, _env.i64Const(0));
                auto new_val = builder.CreateSelect(is_zero, _env.boolConst(false), _env.boolConst(true));
                return SerializableValue(new_val, boolSize);

            } else if(python::Type::F64 == type) {
                auto is_zero = builder.CreateFCmpOEQ(args.front().val, _env.f64Const(0));
                auto new_val = builder.CreateSelect(is_zero, _env.boolConst(false), _env.boolConst(true));
                return SerializableValue(new_val, boolSize);

            } else if(python::Type::STRING == type) {
                auto is_empty = builder.CreateICmpEQ(args.front().size, _env.i64Const(1));
                auto new_val = builder.CreateSelect(is_empty, _env.boolConst(false), _env.boolConst(true));
                return SerializableValue(new_val, boolSize);

            } else {
                logger.error("objects of type " + type.desc() + " are not supported yet in bool(...) call");
                return SerializableValue();
            }

        }

        SerializableValue FunctionRegistry::createStrCast(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                           llvm::IRBuilder<> &builder, python::Type argsType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args) {

            using namespace std;
            using namespace llvm;

            auto& logger = Logger::instance().logger("codegen");

            // constructor
            if(args.empty()) {
                auto retstr = builder.CreatePointerCast(builder.CreateGlobalStringPtr(""), _env.i8ptrType());
                return SerializableValue(retstr, _env.i64Const(1));
            }

            // first check how many args.
            if(args.size() != 1) {
                logger.error("multiple arguments not supported yet for str(...)");
                return SerializableValue();
            }

            auto type = argsType.parameters().front();

            // special case: option type
            // ==> i.e. null check!
            if(type.isOptionType()) {

                auto val = args.front(); assert(val.is_null);
                // if block via phi
                auto curBlock = builder.GetInsertBlock();
                assert(curBlock);

                // create new block for testing and new block to continue
                BasicBlock *bbNull = BasicBlock::Create(builder.getContext(), "opt_null",
                                                        builder.GetInsertBlock()->getParent());
                BasicBlock *bbNotNull = BasicBlock::Create(builder.getContext(), "opt_not_null",
                                                        builder.GetInsertBlock()->getParent());
                BasicBlock *bbDone = BasicBlock::Create(builder.getContext(), "str_done",
                                                            builder.GetInsertBlock()->getParent());

                // vars
                auto valVar = _env.CreateFirstBlockAlloca(builder, _env.i8ptrType());
                auto sizeVar = _env.CreateFirstBlockAlloca(builder, _env.i64Type());

                // then create phi node. ==> if None, ret false. I.e. when coming from curBlock
                // else, ret the result of the truth test in the newly created block!
                builder.CreateCondBr(val.is_null, bbNull, bbNotNull);

                // null block
                builder.SetInsertPoint(bbNull);
                auto nullRes = createStrCast(lfb, builder, python::Type::propagateToTupleType(python::Type::NULLVALUE), vector<SerializableValue>{SerializableValue()});
                builder.CreateStore(nullRes.val, valVar);
                builder.CreateStore(nullRes.size, sizeVar);
                builder.CreateBr(bbDone);

                // string block
                builder.SetInsertPoint(bbNotNull);
                auto res = createStrCast(lfb, builder, python::Type::makeTupleType({type.withoutOptions()}), args);
                builder.CreateStore(res.val, valVar);
                builder.CreateStore(res.size, sizeVar);
                builder.CreateBr(bbDone);

                // set insert point
                builder.SetInsertPoint(bbDone);

                // phi nodes as result
                lfb.setLastBlock(bbDone);
                return SerializableValue(builder.CreateLoad(valVar), builder.CreateLoad(sizeVar));
            }


            // null has constant
            if(python::Type::NULLVALUE == type)
                return SerializableValue(_env.strConst(builder, "None"), _env.i64Const(strlen("None") + 1));
            if(python::Type::EMPTYTUPLE == type)
                return SerializableValue(_env.strConst(builder, "()"), _env.i64Const(strlen("()") + 1));
            if(python::Type::EMPTYDICT == type)
                return SerializableValue(_env.strConst(builder, "{}"), _env.i64Const(strlen("{}") + 1));


            // if it's a string, just return it
            if(python::Type::STRING == type) {
                return args.front();
            }


            // Special case floating point numbers
            if(python::Type::F64 == type) {
                // python converts str(1.) to '1.0' so at least one digit after .
                // this is behavior not supported by sprintf, hence use fmt for doubles!
                //fmtString += "%g"; // use %g to get rid off trailing zeros,
                //fmtSize = builder.CreateAdd(fmtSize, _env.i64Const(20)); // roughly estimate formatted size with 20 bytes


                // call runtime function which implements this special behavior
                auto floatfmt_func = floatToStr_prototype(_env.getContext(), _env.getModule().get());

                // alloc for size
                auto sizeVar = builder.CreateAlloca(_env.i64Type(), 0, nullptr);

                // build arguments
                std::vector<llvm::Value*> valargs;
                valargs.push_back(args.front().val);
                valargs.push_back(sizeVar);

                // make call
                auto replaced_str = builder.CreateCall(floatfmt_func, valargs);

                return {replaced_str, builder.CreateLoad(sizeVar)};
            }


            // Note: casting non-string var to string is similar to formatStr
            // @Todo: refactor both functions into one nice one

            // for strings: need to escape!
            // i.e. escaped size
            // call helper function from runtime for this!
            string fmtString = "";
            std::vector<Value*> spf_args;
            // 3 dummy args to be filled later
            spf_args.emplace_back(nullptr);
            spf_args.emplace_back(nullptr);
            spf_args.emplace_back(nullptr);

            Value* fmtSize = _env.i64Const(1);
            auto val = args.front().val;
            auto size = args.front().size;

            // make sure no weird types encountered!!
            if(python::Type::BOOLEAN == type) {
                fmtString += "%s";
                auto boolCond = builder.CreateICmpNE(_env.boolConst(false), val);
                // select
                val = builder.CreateSelect(boolCond, _env.strConst(builder, "True"), _env.strConst(builder, "False"));
                fmtSize = builder.CreateAdd(fmtSize, _env.i64Const(5));

            } else if(python::Type::I64 == type) {
                fmtString += "%lld";
                fmtSize = builder.CreateAdd(fmtSize, _env.i64Const(20)); // roughly estimate formatted size with 20 bytes
            } else if(python::Type::STRING == type) {
                throw runtime_error("case should be short-circuited above");
            } else {
                // throw exception!
                throw std::runtime_error("no support for strcast for type " + type.desc());
            }

            spf_args.emplace_back(val);

            // use sprintf and speculate a bit on size upfront!
            // then do logic to extend buffer if necessary
            BasicBlock *bbCastDone = BasicBlock::Create(_env.getContext(), "castDone_block", builder.GetInsertBlock()->getParent());
            BasicBlock *bbLargerBuf = BasicBlock::Create(_env.getContext(), "strformat_realloc", builder.GetInsertBlock()->getParent());

            auto bufVar = builder.CreateAlloca(_env.i8ptrType());
            builder.CreateStore(_env.malloc(builder, fmtSize), bufVar);
            auto snprintf_func = snprintf_prototype(_env.getContext(), _env.getModule().get());

            //{csvRow, fmtSize, env().strConst(builder, fmtString), ...}
            spf_args[0] = builder.CreateLoad(bufVar); spf_args[1] = fmtSize; spf_args[2] = _env.strConst(builder, fmtString);
            auto charsRequired = builder.CreateCall(snprintf_func, spf_args);
            auto sizeWritten = builder.CreateAdd(builder.CreateZExt(charsRequired, _env.i64Type()), _env.i64Const(1));

            // now condition, is this greater than allocSize + 1?
            auto notEnoughSpaceCond = builder.CreateICmpSGT(sizeWritten, fmtSize);

            // two checks: if size is too small, alloc larger buffer!!!
            builder.CreateCondBr(notEnoughSpaceCond, bbLargerBuf, bbCastDone);

            // -- block begin --
            builder.SetInsertPoint(bbLargerBuf);
            // realloc with sizeWritten
            // store new malloc in bufVar
            builder.CreateStore(_env.malloc(builder, sizeWritten), bufVar);
            spf_args[0] = builder.CreateLoad(bufVar);
            spf_args[1] = sizeWritten;
            builder.CreateCall(snprintf_func, spf_args);

            builder.CreateBr(bbCastDone);
            builder.SetInsertPoint(bbCastDone);

            // lfb builder set last block too!
            lfb.setLastBlock(bbCastDone);
            builder.SetInsertPoint(bbCastDone);
            return SerializableValue(builder.CreateLoad(bufVar), sizeWritten);
        }

        codegen::SerializableValue createMathSinCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                     const python::Type &retType,
                                                     const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::sin, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcSinCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                     const python::Type &retType,
                                                     const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C asin function
            FunctionType *asin_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *asin_func = cast<Function>(M->getOrInsertFunction("asin", asin_type));
#else
            Function *asin_func = cast<Function>(M->getOrInsertFunction("asin", asin_type).getCallee());
#endif

            auto resVal = builder.CreateCall(asin_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathTanCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                     const python::Type &retType,
                                                     const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C tan function
            FunctionType *tan_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *tan_func = cast<Function>(M->getOrInsertFunction("tan", tan_type));
#else
            Function *tan_func = cast<Function>(M->getOrInsertFunction("tan", tan_type).getCallee());
#endif

            auto resVal = builder.CreateCall(tan_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcTanCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                        const python::Type &retType,
                                                        const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C atan function
            FunctionType *atan_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *atan_func = cast<Function>(M->getOrInsertFunction("atan", atan_type));
#else
            Function *atan_func = cast<Function>(M->getOrInsertFunction("atan", atan_type).getCallee());
#endif

            auto resVal = builder.CreateCall(atan_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcTan2Call(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                         const python::Type &retType,
                                                         const tuplex::codegen::SerializableValue&arg1,
                                                         const tuplex::codegen::SerializableValue&arg2) {
            using namespace llvm;

            auto val1 = arg1;
            auto val2 = arg2;
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C atan2 function
            FunctionType *atan2_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context), ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *atan2_func = cast<Function>(M->getOrInsertFunction("atan2", atan2_type));
#else
            Function *atan2_func = cast<Function>(M->getOrInsertFunction("atan2", atan2_type).getCallee());
#endif

            auto resVal = builder.CreateCall(atan2_func, {codegen::upCast(builder, val1.val, llvm::Type::getDoubleTy(context)), codegen::upCast(builder, val2.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathTanHCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C tanh function
            FunctionType *tanh_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *tanh_func = cast<Function>(M->getOrInsertFunction("tanh", tanh_type));
#else
            Function *tanh_func = cast<Function>(M->getOrInsertFunction("tanh", tanh_type).getCallee());
#endif

            auto resVal = builder.CreateCall(tanh_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcTanHCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                         const python::Type &retType,
                                                         const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C atanh function
            FunctionType *atanh_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *atanh_func = cast<Function>(M->getOrInsertFunction("atanh", atanh_type));
#else
            Function *atanh_func = cast<Function>(M->getOrInsertFunction("atanh", atanh_type).getCallee());
#endif

            auto resVal = builder.CreateCall(atanh_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcCosCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                        const python::Type &retType,
                                                        const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C acos function
            FunctionType *acos_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *acos_func = cast<Function>(M->getOrInsertFunction("acos", acos_type));
#else
            Function *acos_func = cast<Function>(M->getOrInsertFunction("acos", acos_type).getCallee());
#endif

            auto resVal = builder.CreateCall(acos_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathCosHCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C cosh function
            FunctionType *cosh_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *cosh_func = cast<Function>(M->getOrInsertFunction("cosh", cosh_type));
#else
            Function *cosh_func = cast<Function>(M->getOrInsertFunction("cosh", cosh_type).getCallee());
#endif

            auto resVal = builder.CreateCall(cosh_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcCosHCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                         const python::Type &retType,
                                                         const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C acosh function
            FunctionType *acosh_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *acosh_func = cast<Function>(M->getOrInsertFunction("acosh", acosh_type));
#else
            Function *acosh_func = cast<Function>(M->getOrInsertFunction("acosh", acosh_type).getCallee());
#endif

            auto resVal = builder.CreateCall(acosh_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathSinHCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C sinh function
            FunctionType *sinh_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *sinh_func = cast<Function>(M->getOrInsertFunction("sinh", sinh_type));
#else
            Function *sinh_func = cast<Function>(M->getOrInsertFunction("sinh", sinh_type).getCallee());
#endif

            auto resVal = builder.CreateCall(sinh_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathArcSinHCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                         const python::Type &retType,
                                                         const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C asinh function
            FunctionType *asinh_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *asinh_func = cast<Function>(M->getOrInsertFunction("asinh", asinh_type));
#else
            Function *asinh_func = cast<Function>(M->getOrInsertFunction("asinh", asinh_type).getCallee());
#endif

            auto resVal = builder.CreateCall(asinh_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue FunctionRegistry::createMathToRadiansCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                           const python::Type &retType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;
            auto& context = builder.GetInsertBlock()->getContext();
            auto val = args.front();
            llvm::Value* F64Coefficient = _env.f64Const(M_PI / 180.0);
            auto resVal = builder.CreateFMul(val.val, F64Coefficient);
            auto resSize = _env.i64Const(sizeof(double));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue FunctionRegistry::createMathToDegreesCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                           const python::Type &retType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;
            auto& context = builder.GetInsertBlock()->getContext();
            auto val = args.front();
            llvm::Value* F64Coefficient = _env.f64Const(180.0 / M_PI );
            auto resVal = builder.CreateFMul(val.val, F64Coefficient);
            auto resSize = _env.i64Const(sizeof(double));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathCosCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                     const python::Type &retType,
                                                     const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::cos, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathSqrtCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                     const python::Type &retType,
                                                     const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::sqrt, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathExpCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::exp, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathLogCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::log, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathLog1pCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C log1p function
            FunctionType * log1p_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function * log1p_func = cast<Function>(M->getOrInsertFunction("log1p", log1p_type));
#else
            Function * log1p_func = cast<Function>(M->getOrInsertFunction("log1p", log1p_type).getCallee());
#endif

            auto resVal = builder.CreateCall(log1p_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathLog2Call(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::log2, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathLog10Call(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            // call llvm intrinsic
            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();

            // cast to f64
            auto resVal = llvm::createUnaryIntrinsic(builder, llvm::Intrinsic::ID::log10, codegen::upCast(builder, val.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathPowCall(llvm::IRBuilder<>& builder,
                                                     const python::Type &argsType,
                                                     const python::Type &retType,
                                                     const tuplex::codegen::SerializableValue&base,
                                                     const tuplex::codegen::SerializableValue&power) {
            // call llvm intrinsic
            auto val1 = base;
            auto val2 = power;
            auto& context = builder.GetInsertBlock()->getContext();
            // cast to f64
            auto resVal = llvm::createBinaryIntrinsic(builder, llvm::Intrinsic::ID::pow, codegen::upCast(builder, val1.val, llvm::Type::getDoubleTy(context)), codegen::upCast(builder, val2.val, llvm::Type::getDoubleTy(context)));
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }

        codegen::SerializableValue createMathExpm1Call(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;

            auto val = args.front();
            auto& context = builder.GetInsertBlock()->getContext();
            Module *M = builder.GetInsertBlock()->getModule();

            // call standard C expm1 function
            FunctionType *expm1_type = FunctionType::get(ctypeToLLVM<double>(context), {ctypeToLLVM<double>(context)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function *expm1_func = cast<Function>(M->getOrInsertFunction("expm1", expm1_type));
#else
            Function *expm1_func = cast<Function>(M->getOrInsertFunction("expm1", expm1_type).getCallee());
#endif

            auto resVal = builder.CreateCall(expm1_func, {codegen::upCast(builder, val.val, Type::getDoubleTy(context))});
            auto resSize = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(context), llvm::APInt(64, sizeof(double)));
            return SerializableValue(resVal, resSize);
        }




        codegen::SerializableValue FunctionRegistry::createGlobalSymbolCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                            llvm::IRBuilder<> &builder,
                                                                            const std::string &symbol,
                                                                            const python::Type &argsType,
                                                                            const python::Type &retType,
                                                                            const std::vector<tuplex::codegen::SerializableValue> &args) {

            // check what symbols are supported, else return nullptr
            if (symbol == "len") {
                return createLenCall(builder, argsType, retType, args);
            }

            if (symbol == "dict") {
                return createDictConstructor(lfb, builder, argsType, args);
            }

            if (symbol == "int") {
                return createIntCast(lfb, builder, argsType, args);
            }

            if (symbol == "float") {
                return createFloatCast(lfb, builder, argsType, args);
            }

            if (symbol == "bool") {
                return createBoolCast(lfb, builder, argsType, args);
            }

            if (symbol == "str") {
                return createStrCast(lfb, builder, argsType, args);
            }

            // math module
            if (symbol == "math.sin")
                return createMathSinCall(builder, argsType, retType, args);
            if (symbol == "math.cos")
                return createMathCosCall(builder, argsType, retType, args);
            if (symbol == "math.sqrt")
                return createMathSqrtCall(builder, argsType, retType, args);
            if (symbol == "math.asin")
                return createMathArcSinCall(builder, argsType, retType, args);
            if(symbol == "math.floor" || symbol ==  "math.ceil")
                return createMathCeilFloorCall(lfb, builder, symbol, args.front());
            if (symbol == "math.exp")
                return createMathExpCall(builder, argsType, retType, args);
            if (symbol == "math.log")
                return createMathLogCall(builder, argsType, retType, args);
            if (symbol == "math.log1p")
                return createMathLog1pCall(builder, argsType, retType, args);
            if (symbol == "math.log2")
                return createMathLog2Call(builder, argsType, retType, args);
            if (symbol == "math.log10")
                return createMathLog10Call(builder, argsType, retType, args);
            if (symbol == "math.pow")
                return createMathPowCall(builder, argsType, retType, args[0], args[1]);
            if (symbol == "math.expm1")
                return createMathExpm1Call(builder, argsType, retType, args);
            if (symbol == "math.tan")
                return createMathTanCall(builder, argsType, retType, args);
            if (symbol == "math.atan")
                return createMathArcTanCall(builder, argsType, retType, args);
            if (symbol == "math.atan2")
                return createMathArcTan2Call(builder, argsType, retType, args[0], args[1]);
            if (symbol == "math.tanh")
                return createMathTanHCall(builder, argsType, retType, args);
            if (symbol == "math.atanh")
                return createMathArcTanHCall(builder, argsType, retType, args);

            if (symbol == "math.acos")
                return createMathArcCosCall(builder, argsType, retType, args);
            if (symbol == "math.cosh")
                return createMathCosHCall(builder, argsType, retType, args);
            if (symbol == "math.acosh")
                return createMathArcCosHCall(builder, argsType, retType, args);

            if (symbol == "math.sinh")
                return createMathSinHCall(builder, argsType, retType, args);
            if (symbol == "math.asinh")
                return createMathArcSinHCall(builder, argsType, retType, args);

            if (symbol == "math.radians")
                return createMathToRadiansCall(builder, argsType, retType, args);

            if (symbol == "math.degrees")
                return createMathToDegreesCall(builder, argsType, retType, args);


            // re module
            if (symbol == "re.search")
                return createReSearchCall(lfb, builder, argsType, args);
            if (symbol == "re.sub") {
                if(args.size() != 3 || argsType != python::Type::makeTupleType({python::Type::STRING, python::Type::STRING, python::Type::STRING})) {
                    throw std::runtime_error("Only support re.sub(str, str, str)");
                }
                return createReSubCall(lfb, builder, argsType, args);
            }

            // random module
            if (symbol == "random.choice") {
                if(args.size() != 1 || !argsType.isTupleType() || argsType.parameters().size() != 1) {
                    throw std::runtime_error("random.choice only takes a single (iterable) argument");
                }
                return createRandomChoiceCall(lfb, builder, argsType.parameters()[0], args[0]);
            }

            // string module
            if(symbol == "string.capwords") {
                if(args.size() != 1)
                    throw std::runtime_error("string.capwords() takes exactly 1 argument");
                return createCapwordsCall(lfb, builder, args[0]);
            }

            return SerializableValue(nullptr, nullptr);
        }

        SerializableValue FunctionRegistry::createCenterCall(LambdaFunctionBuilder& lfb,
                                                            llvm::IRBuilder<> &builder,
                                                            const tuplex::codegen::SerializableValue &caller,
                                                            const tuplex::codegen::SerializableValue &width,
                                                            const tuplex::codegen::SerializableValue *fillchar){            
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            auto casted_width_val = _env.upCast(builder, width.val, _env.i64Type());
            assert(casted_width_val->getType() == _env.i64Type());

            llvm::Value *fillchar_val = _env.i8Const(' ');
            if(fillchar != nullptr) {
                assert(fillchar->val->getType() == _env.i8ptrType());

                auto cond = builder.CreateICmpNE(fillchar->size, _env.i64Const(2)); // fillchar must be size 2, indicating length 1
                lfb.addException(builder, ExceptionCode::TYPEERROR, cond);

                fillchar_val = builder.CreateLoad(fillchar->val);
            }

            FunctionType *ft = FunctionType::get(_env.i8ptrType(), {_env.i8ptrType(), _env.i64Type(), _env.i64Type(), llvm::Type::getInt64PtrTy(_env.getContext(), 0), _env.i8Type()}, false);

            auto func = _env.getModule()->getOrInsertFunction("strCenter", ft);
            auto res_size = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
            auto new_val = builder.CreateCall(func, {caller.val, caller.size, casted_width_val, res_size, fillchar_val});
            return SerializableValue(new_val, builder.CreateLoad(res_size));
        }

        SerializableValue FunctionRegistry::createLowerCall(llvm::IRBuilder<> &builder,
                                                            const tuplex::codegen::SerializableValue &caller) {
            // simple, use helper function
            // call strLower from runtime
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());

            // implemeneted in runtime
            FunctionType *ft = FunctionType::get(_env.i8ptrType(), {_env.i8ptrType(), _env.i64Type()}, false);
            auto func = _env.getModule()->getOrInsertFunction("strLower", ft);

            auto new_val = builder.CreateCall(func, {caller.val, caller.size});

            // size doesn't change when applying lower to str
            return SerializableValue(new_val, caller.size);
        }

        SerializableValue FunctionRegistry::createMathCeilFloorCall(LambdaFunctionBuilder &lfb,
                                                                    llvm::IRBuilder<> &builder,
                                                                    const std::string &qual_name,
                                                                    const SerializableValue &arg) {
            assert(qual_name == "math.ceil" || qual_name == "math.floor");
            assert(arg.is_null == nullptr); // no NULL allowed here!

            if(arg.val->getType()->isDoubleTy()) {
                // TODO: if we allow undefined behavior, optimize these checks away...

                // these functions pretty much do the same.
                // first check inf, -inf which results in overflow error
                auto overflow_cond = builder.CreateOr(builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OEQ, arg.val, _env.f64Const(INFINITY)),
                                                      builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OEQ, arg.val, _env.f64Const(-INFINITY)));
                lfb.addException(builder, ExceptionCode::OVERFLOWERROR, overflow_cond);

                // then check nan, which results in ValueError
                auto nan_cond = builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OEQ, arg.val, _env.f64Const(NAN));
                lfb.addException(builder, ExceptionCode::VALUEERROR, nan_cond);

                // call corresponding intrinsic
                auto intrinsic = (qual_name == "math.ceil") ? (llvm::Intrinsic::ceil) : (llvm::Intrinsic::floor);
                auto val = builder.CreateFPToSI(llvm::createUnaryIntrinsic(builder, intrinsic, arg.val),
                _env.i64Type());
                return SerializableValue(val, _env.i64Const(sizeof(int64_t)));
            } else {
                // in python functions hold integer as result.
                // hence upcast boolean
                assert(arg.val->getType() == _env.i64Type() || arg.val->getType() == _env.getBooleanType());
                if(arg.val->getType() == _env.getBooleanType()) {
                    return SerializableValue(builder.CreateZExtOrTrunc(arg.val, _env.i64Type()), _env.i64Const(sizeof(int64_t)));
                } else {
                    return arg;
                }
            }
        }

        SerializableValue FunctionRegistry::createUpperCall(llvm::IRBuilder<> &builder,
                                                            const tuplex::codegen::SerializableValue &caller) {
            // simple, use helper function
            // call strLower from runtime
            using namespace llvm;

            assert(caller.val->getType() == _env.i8ptrType());

            // implemented in runtime
            FunctionType *ft = FunctionType::get(_env.i8ptrType(), {_env.i8ptrType(), _env.i64Type()}, false);
            auto func = _env.getModule()->getOrInsertFunction("strUpper", ft);

            auto new_val = builder.CreateCall(func, {caller.val, caller.size});

            // size doesn't change when applying lower to str
            return SerializableValue(new_val, caller.size);
        }

        SerializableValue FunctionRegistry::createSwapcaseCall(llvm::IRBuilder<> &builder,
                                                               const tuplex::codegen::SerializableValue &caller) {
            using namespace llvm;

            assert(caller.val->getType() == _env.i8ptrType());

            FunctionType *ft = FunctionType::get(_env.i8ptrType(), {_env.i8ptrType(), _env.i64Type()}, false);
            auto func = _env.getModule()->getOrInsertFunction("strSwapcase", ft);

            auto new_val = builder.CreateCall(func, {caller.val, caller.size});

            // size doesn't change when applying swapcase for str
            return SerializableValue(new_val, caller.size);
        }

        // TODO: fix with optional sep! https://docs.python.org/3/library/string.html#string.capwords
        SerializableValue FunctionRegistry::createCapwordsCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller) {
            // simple, use helper function
            // call strLower from runtime
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());

            //@TODO: because capwords is actually
            // # Capitalize the words in a string, e.g. " aBc  dEf " -> "Abc Def".
            //def capwords(s, sep=None):
            //    """capwords(s [,sep]) -> string
            //    Split the argument into words using split, capitalize each
            //    word using capitalize, and join the capitalized words using
            //    join.  If the optional second argument sep is absent or None,
            //    runs of whitespace characters are replaced by a single space
            //    and leading and trailing whitespace are removed, otherwise
            //    sep is used to split and join the words.
            //    """
            //    return (sep or ' ').join(x.capitalize() for x in s.split(sep))
            // depending on sep/x attributerror or typeerror needs to be raised.
            // we only support the 1 keyword version.
            // Note: this function is the perfect test candidate for (pre)compiling external module functions!

            // no option type here supported!
            if(caller.is_null) {
                // add exception for None.capitalize()
                lfb.addException(builder, ExceptionCode::ATTRIBUTEERROR, caller.is_null);
            }
            assert(caller.val && caller.size);

            // implemeneted in runtime
            FunctionType *ft = FunctionType::get(_env.i8ptrType(), {_env.i8ptrType(), _env.i64Type(), _env.i64ptrType()}, false);
            auto func = _env.getModule()->getOrInsertFunction("stringCapwords", ft);

            auto res_size = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
            builder.CreateStore(_env.i64Const(0), res_size);
            auto new_val = builder.CreateCall(func, {caller.val, caller.size, res_size});

            // size doesn't change when applying lower to str
            return SerializableValue(new_val, builder.CreateLoad(res_size));
        }


        SerializableValue FunctionRegistry::createReSearchCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                               const python::Type &argsType,
                                                               const std::vector<tuplex::codegen::SerializableValue> &args) {
            assert(argsType.parameters().size() == 2 && argsType.parameters()[0] == python::Type::STRING &&
                   argsType.parameters()[1] == python::Type::STRING);
            auto& logger = Logger::instance().logger("codegen");

            if(args.size() == 2) {
                llvm::Value *general_context, *match_context, *compile_context;
                if(_sharedObjectPropagation) {
                    // create runtime contexts that are allocated on regular heap: general, compile, match (in order to pass rtmalloc/rtfree)
                    auto contexts = _env.addGlobalPCRE2RuntimeContexts();
                    general_context = builder.CreateLoad(std::get<0>(contexts));
                    match_context = builder.CreateLoad(std::get<1>(contexts));
                    compile_context = builder.CreateLoad(std::get<2>(contexts));
                } else {
                    // create runtime contexts for the row
                    general_context = builder.CreateCall(pcre2GetLocalGeneralContext_prototype(_env.getContext(), _env.getModule().get()));
                    match_context = builder.CreateCall(pcre2MatchContextCreate_prototype(_env.getContext(), _env.getModule().get()), {general_context});
                    compile_context = builder.CreateCall(pcre2CompileContextCreate_prototype(_env.getContext(), _env.getModule().get()), {general_context});
                }

                // get the compiled pattern
                llvm::Value* compiled_pattern;
                bool global_pattern = llvm::isa<llvm::ConstantExpr>(args[0].val) && _sharedObjectPropagation;
                if(global_pattern) {
                    auto pattern_str = globalVariableToString(args[0].val);
                    llvm::Value* gVar = _env.addGlobalRegexPattern("re_search", pattern_str);
                    compiled_pattern = builder.CreateLoad(gVar);
                } else {
                    // allocate some error space
                    auto errornumber = builder.CreateAlloca(builder.getInt32Ty());
                    auto erroroffset = builder.CreateAlloca(builder.getInt64Ty());

                    // create the compiled pattern
                    compiled_pattern = builder.CreateCall(
                            pcre2Compile_prototype(_env.getContext(), _env.getModule().get()),
                            {args[0].val, builder.CreateSub(args[0].size, _env.i64Const(1)), _env.i32Const(0),
                             errornumber,
                             erroroffset, compile_context});

                    // perform check whether compile was successful, if not terminate.
                    auto bad_pattern = builder.CreateICmpEQ(compiled_pattern, _env.i8nullptr());
                    lfb.addException(builder, ExceptionCode::RE_ERROR, bad_pattern);
                }

                // allocate space to hold the match data
                auto match_data = builder.CreateCall(
                        pcre2MatchDataCreateFromPattern_prototype(_env.getContext(), _env.getModule().get()),
                        {compiled_pattern, general_context});

                // run the match
                llvm::Value *num_matches;
                if(global_pattern) {
                    // if we precompiled, use the JIT match function
                    num_matches = builder.CreateCall(
                            pcre2JITMatch_prototype(_env.getContext(), _env.getModule().get()),
                            {compiled_pattern, args[1].val,
                             builder.CreateSub(args[1].size, _env.i64Const(1)),
                             _env.i64Const(0), _env.i32Const(0), match_data, match_context});
                } else {
                    num_matches = builder.CreateCall(
                            pcre2Match_prototype(_env.getContext(), _env.getModule().get()),
                            {compiled_pattern, args[1].val,
                             builder.CreateSub(args[1].size, _env.i64Const(1)),
                             _env.i64Const(0), _env.i32Const(0), match_data, match_context});
                }

                // None if the match failed
                auto did_not_match = builder.CreateICmpSLT(num_matches, _env.i32Const(0));

                // get the correct return size/val
                auto retval = builder.CreateAlloca(_env.getMatchObjectPtrType(), 0, nullptr);
                auto retsize = builder.CreateAlloca(_env.i64Type(), 0, nullptr);

                llvm::BasicBlock *did_not_match_BB = llvm::BasicBlock::Create(_env.getContext(), "did_not_match", builder.GetInsertBlock()->getParent());
                llvm::BasicBlock *did_match_BB = llvm::BasicBlock::Create(_env.getContext(), "did_match", builder.GetInsertBlock()->getParent());
                llvm::BasicBlock *return_BB = llvm::BasicBlock::Create(_env.getContext(), "return", builder.GetInsertBlock()->getParent());

                builder.CreateCondBr(did_not_match, did_not_match_BB, did_match_BB);
                builder.SetInsertPoint(did_not_match_BB);
#ifndef NDEBUG // only set to nullptr, 0 if in debug mode
                builder.CreateStore(llvm::ConstantPointerNull::get(
                        static_cast<llvm::PointerType *>(_env.getMatchObjectPtrType())), retval);
                builder.CreateStore(_env.i64Const(0), retsize);
#endif
                builder.CreateBr(return_BB);

                builder.SetInsertPoint(did_match_BB);
                builder.CreateStore(builder.CreateCall(wrapPCRE2MatchObject_prototype(_env.getContext(), _env.getModule().get()), {match_data, args[1].val, args[1].size}), retval);
                builder.CreateStore(_env.i64Const(sizeof(uint8_t*)), retsize);
                builder.CreateBr(return_BB);

                builder.SetInsertPoint(return_BB);
                lfb.setLastBlock(return_BB);

                // return the match object
                return SerializableValue(builder.CreateLoad(retval), builder.CreateLoad(retsize), did_not_match);
            }

            logger.error("no support for re.search flags");
            return SerializableValue();
        }

        SerializableValue FunctionRegistry::createReSubCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const python::Type &argsType,
                        const std::vector<tuplex::codegen::SerializableValue> &args) {
            assert(argsType.parameters().size() == 3 && argsType.parameters()[0] == python::Type::STRING &&
                   argsType.parameters()[1] == python::Type::STRING && argsType.parameters()[2] == python::Type::STRING);
            auto& logger = Logger::instance().logger("codegen");

            if(args.size() == 3) {
                llvm::Value *general_context, *match_context, *compile_context;
                if(_sharedObjectPropagation) {
                    // create runtime contexts that are allocated on regular heap: general, compile, match (in order to pass rtmalloc/rtfree)
                    auto contexts = _env.addGlobalPCRE2RuntimeContexts();
                    general_context = builder.CreateLoad(std::get<0>(contexts));
                    match_context = builder.CreateLoad(std::get<1>(contexts));
                    compile_context = builder.CreateLoad(std::get<2>(contexts));
                } else {
                    // create runtime contexts for the row
                    general_context = builder.CreateCall(pcre2GetLocalGeneralContext_prototype(_env.getContext(), _env.getModule().get()));
                    match_context = builder.CreateCall(pcre2MatchContextCreate_prototype(_env.getContext(), _env.getModule().get()), {general_context});
                    compile_context = builder.CreateCall(pcre2CompileContextCreate_prototype(_env.getContext(), _env.getModule().get()), {general_context});
                }

                // get the compiled pattern
                llvm::Value* compiled_pattern;
                bool global_pattern = llvm::isa<llvm::ConstantExpr>(args[0].val) && _sharedObjectPropagation;
                if(global_pattern) {
                    auto pattern_str = globalVariableToString(args[0].val);
                    llvm::Value* gVar = _env.addGlobalRegexPattern("re_sub", pattern_str);
                    compiled_pattern = builder.CreateLoad(gVar);
                } else {
                    // allocate some error space
                    auto errornumber = builder.CreateAlloca(builder.getInt32Ty());
                    auto erroroffset = builder.CreateAlloca(builder.getInt64Ty());

                    // create the compiled pattern
                    compiled_pattern = builder.CreateCall(
                            pcre2Compile_prototype(_env.getContext(), _env.getModule().get()),
                            {args[0].val, builder.CreateSub(args[0].size, _env.i64Const(1)), _env.i32Const(0),
                             errornumber,
                             erroroffset, compile_context});
                }

                auto pattern = args[0];
                auto repl = args[1];
                auto subject = args[2];

                // create blocks
                llvm::BasicBlock *substitute_BB = llvm::BasicBlock::Create(_env.getContext(), "substitute_re_sub", builder.GetInsertBlock()->getParent());
                llvm::BasicBlock *realloc_output_BB = llvm::BasicBlock::Create(_env.getContext(), "realloc_output_re_sub", builder.GetInsertBlock()->getParent());
                llvm::BasicBlock *errorcheck_BB = llvm::BasicBlock::Create(_env.getContext(), "errorcheck_re_sub", builder.GetInsertBlock()->getParent());
                llvm::BasicBlock *return_BB = llvm::BasicBlock::Create(_env.getContext(), "return_re_sub", builder.GetInsertBlock()->getParent());

                // create variables
                llvm::Value *res = builder.CreateAlloca(_env.i32Type(), 0, nullptr); // the result of calling pcre2_substitute
                llvm::Value *cur_result_size = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
                llvm::Value *result_size = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
                llvm::Value *result_buffer = builder.CreateAlloca(_env.i8ptrType(), 0, nullptr);
                builder.CreateStore(builder.CreateUDiv(builder.CreateMul(subject.size, _env.i64Const(6)), _env.i64Const(5), "re_sub_result", false), cur_result_size); // start at 1.2 * subject length
                builder.CreateBr(substitute_BB);

                builder.SetInsertPoint(substitute_BB);
                // allocate output space
                builder.CreateStore(builder.CreateLoad(cur_result_size), result_size); // result_size = cur_result_size
                builder.CreateStore(builder.CreatePointerCast(_env.malloc(builder, builder.CreateLoad(cur_result_size)), _env.i8ptrType()), result_buffer); // result_buffer = (char*)malloc(result_size);
                // run the substitution
                auto num_matches = builder.CreateCall(
                        pcre2Substitute_prototype(_env.getContext(), _env.getModule().get()),
                        {
                            compiled_pattern, // code
                            subject.val, // subject
                            builder.CreateSub(subject.size, _env.i64Const(1)), // subject length
                            _env.i64Const(0), // start offset
                            _env.i32Const(PCRE2_SUBSTITUTE_GLOBAL), // options
                            _env.i8nullptr(), // match data
                            match_context, // match context
                            repl.val, // replacement
                            builder.CreateSub(repl.size, _env.i64Const(1)), // repl length
                            builder.CreateLoad(result_buffer), // result buffer
                            result_size
                        });
                builder.CreateStore(num_matches, res);
                auto ran_out_of_memory = builder.CreateICmpEQ(builder.CreateLoad(res), _env.i32Const(PCRE2_ERROR_NOMEMORY));
                builder.CreateCondBr(ran_out_of_memory, realloc_output_BB, return_BB);

                builder.SetInsertPoint(realloc_output_BB);
                builder.CreateStore(builder.CreateMul(builder.CreateLoad(cur_result_size), _env.i64Const(2)), cur_result_size); // double cur_result_size
                // TODO: should we error here if the potential output buffer gets too large?
                builder.CreateBr(substitute_BB); // try substituting again

                builder.SetInsertPoint(errorcheck_BB);
                // error if the substitution resulted in an error
                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpSLT(builder.CreateLoad(res), _env.i32Const(0)));
                builder.CreateBr(return_BB);

                builder.SetInsertPoint(return_BB);
                builder.CreateStore(_env.i8Const(0), builder.CreateGEP(builder.CreateLoad(result_buffer), builder.CreateLoad(result_size))); // include null terminator
                lfb.setLastBlock(return_BB);

                // return the match object
                // TODO: should we reallocate the buffer to be exactly the correct size? pcre2_substitute * does * make sure to include space for a null terminator
                return SerializableValue(builder.CreateLoad(result_buffer), builder.CreateAdd(builder.CreateLoad(result_size), _env.i64Const(1)));
            }

            logger.error("no support for re.sub flags");
            return SerializableValue();
        }

        SerializableValue FunctionRegistry::createRandomChoiceCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const python::Type &argType, const SerializableValue &arg) {
            if(argType == python::Type::STRING) {
                lfb.addException(builder, ExceptionCode::INDEXERROR, builder.CreateICmpEQ(arg.size, _env.i64Const(1))); // index error if empty string
                auto random_number = builder.CreateCall(uniformInt_prototype(_env.getContext(), _env.getModule().get()), {_env.i64Const(0), builder.CreateSub(arg.size, _env.i64Const(1))});
                auto retstr = builder.CreatePointerCast(_env.malloc(builder, _env.i64Const(2)), _env.i8ptrType()); // create 1-char string
                builder.CreateStore(builder.CreateLoad(builder.CreateGEP(arg.val, random_number)), retstr); // store the character
                builder.CreateStore(_env.i8Const(0), builder.CreateGEP(retstr, _env.i32Const(1))); // write a null terminator
                return {retstr, _env.i64Const(2)};
            } else if(argType.isListType() && argType != python::Type::EMPTYLIST) {
                auto elementType = argType.elementType();
                if(elementType.isSingleValued()) {
                    lfb.addException(builder, ExceptionCode::INDEXERROR, builder.CreateICmpEQ(arg.val, _env.i64Const(0))); // index error if empty list
                    if(elementType == python::Type::NULLVALUE) {
                        return {nullptr, nullptr, _env.i1Const(true)};
                    } else if(elementType == python::Type::EMPTYTUPLE) {
                        auto alloc = builder.CreateAlloca(_env.getEmptyTupleType(), 0, nullptr);
                        auto load = builder.CreateLoad(alloc);
                        return {load, _env.i64Const(sizeof(int64_t))};
                    } else if(elementType == python::Type::EMPTYDICT) {
                        return {_env.strConst(builder, "{}"), _env.i64Const(strlen("{}") + 1)};
                    }
                } else {
                    auto num_elements = builder.CreateExtractValue(arg.val, {1});
                    lfb.addException(builder, ExceptionCode::INDEXERROR, builder.CreateICmpEQ(num_elements, _env.i64Const(0))); // index error if empty list
                    auto random_number = builder.CreateCall(uniformInt_prototype(_env.getContext(), _env.getModule().get()), {_env.i64Const(0), num_elements});

                    auto subval = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(arg.val, 2), random_number));
                    llvm::Value* subsize = _env.i64Const(sizeof(int64_t));
                    if(elementType == python::Type::STRING) {
                        subsize = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(arg.val, 3), random_number));
                    }
                    return {subval, subsize};
                }
            } else {
                throw std::runtime_error("random.choice() is only supported for string arguments, currently");
            }

            return SerializableValue();
        }

        SerializableValue FunctionRegistry::createIteratorRelatedSymbolCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                            llvm::IRBuilder<> &builder,
                                                                            const std::string &symbol,
                                                                            const python::Type &argsType,
                                                                            const python::Type &retType,
                                                                            const std::vector<tuplex::codegen::SerializableValue> &args,
                                                                            const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            if(symbol == "iter") {
                return createIterCall(lfb, builder, argsType, retType, args);
            }

            if(symbol == "reversed") {
                return createReversedCall(lfb, builder, argsType, retType, args);
            }

            if(symbol == "zip") {
                return createZipCall(lfb, builder, argsType, retType, args, iteratorInfo);
            }

            if(symbol == "enumerate") {
                return createEnumerateCall(lfb, builder, argsType, retType, args, iteratorInfo);
            }

            if(symbol == "next") {
                return createNextCall(lfb, builder, argsType, retType, args, iteratorInfo);
            }

            Logger::instance().defaultLogger().error("unsupported symbol " + symbol + " encountered in createIteratorRelatedSymbolCall");
            return SerializableValue(nullptr, nullptr);
        }

        SerializableValue FunctionRegistry::createIterCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                           const python::Type &argsType, const python::Type &retType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args) {
            if(argsType.parameters().size() != 1) {
                Logger::instance().defaultLogger().error("iter() currently takes single iterable as argument only");
                return SerializableValue(nullptr, nullptr);
            }

            python::Type argType = argsType.parameters().front();
            if(argType.isIteratorType()) {
                // iter() call on a iterator. Simply return the iterator as it is.
                return args.front();
            }
            return _iteratorContextProxy->initIterContext(lfb, builder, argType, args.front());
        }

        SerializableValue FunctionRegistry::createReversedCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                           const python::Type &argsType, const python::Type &retType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args) {
            if(argsType.parameters().size() != 1) {
                Logger::instance().defaultLogger().error("reversed() takes exactly one argument");
                return SerializableValue(nullptr, nullptr);
            }

            python::Type argType = argsType.parameters().front();
            return _iteratorContextProxy->initReversedContext(lfb, builder, argType, args.front());
        }

        SerializableValue FunctionRegistry::createZipCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                           const python::Type &argsType, const python::Type &retType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args,
                                                           const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            return _iteratorContextProxy->initZipContext(lfb, builder, args, iteratorInfo);
        }

        SerializableValue FunctionRegistry::createEnumerateCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                          const python::Type &argsType, const python::Type &retType,
                                                          const std::vector<tuplex::codegen::SerializableValue> &args,
                                                          const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            python::Type argType = argsType.parameters().front();
            auto *ils = new IteratorContextProxy(&_env);

            if(argsType.parameters().size() == 1) {
                return ils->initEnumerateContext(lfb, builder, args[0], _env.i64Const(0), iteratorInfo);
            }

            if(argsType.parameters().size() == 2) {
                return ils->initEnumerateContext(lfb, builder, args[0], args[1].val, iteratorInfo);
            }

            Logger::instance().defaultLogger().error("enumerate() takes 1 or 2 arguments");
            return SerializableValue(nullptr, nullptr);
        }

        SerializableValue FunctionRegistry::createNextCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                           const python::Type &argsType, const python::Type &retType,
                                                           const std::vector<tuplex::codegen::SerializableValue> &args,
                                                           const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            if(argsType.parameters().size() == 1) {
                if(argsType.parameters().front() == python::Type::EMPTYITERATOR) {
                    // always raise exception when next is called on empty iterator
                    lfb.addException(builder, ExceptionCode::STOPITERATION, _env.i1Const(true));
                    return SerializableValue(_env.i64Const(0), _env.i64Const(8));
                }
                return _iteratorContextProxy->createIteratorNextCall(lfb, builder, argsType.parameters().front().yieldType(), args[0].val, SerializableValue(nullptr, nullptr), iteratorInfo);
            }

            if(argsType.parameters().size() == 2) {
                if(argsType.parameters().front() == python::Type::EMPTYITERATOR) {
                    return args[1];
                }
                return _iteratorContextProxy->createIteratorNextCall(lfb, builder, argsType.parameters().front().yieldType(), args[0].val, args[1], iteratorInfo);
            }

            Logger::instance().defaultLogger().error("next() takes 1 or 2 arguments");
            return SerializableValue(nullptr, nullptr);
        }

        /*!
         * create a string representation of the types of the varargs. May throw exception, if unknown type is encountered
         * @param argTypes vector of python types
         * @return string representation
         */
        std::string createVarArgTypeStr(const std::vector<python::Type>& argsTypes) {
            std::string s;
            s.reserve(argsTypes.size());

            for(auto t : argsTypes) {
                if(python::Type::BOOLEAN == t)
                    s += "b";
                else if(python::Type::I64 == t)
                    s += "d";
                else if(python::Type::F64 == t)
                    s += "f";
                else if(python::Type::STRING == t)
                    s += "s";
                else {
                    Logger::instance().defaultLogger().error("unknown type " + t.desc() + " encountered in varargs");
                    return "";
                }
            }
            return s;
        }

        SerializableValue FunctionRegistry::createFormatCall(llvm::IRBuilder<> &builder,
                                                             const tuplex::codegen::SerializableValue& caller,
                                                             const std::vector<tuplex::codegen::SerializableValue>& args,
                                                             const std::vector<python::Type>& argsTypes) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            auto strFormat_func = strFormat_prototype(_env.getContext(), _env.getModule().get());

            // alloc for size
            auto sizeVar = _env.CreateFirstBlockAlloca(builder, _env.i64Type());

            // alloc string for types
            std::string argtypesstr = createVarArgTypeStr(argsTypes);

            // build arguments
            std::vector<llvm::Value*> valargs;
            valargs.push_back(caller.val);
            valargs.push_back(sizeVar);

            assert(argsTypes.size() == args.size());
            for(int i = 0; i < args.size(); ++i) {
                auto sv = args[i];

                if(argsTypes[i] == python::Type::BOOLEAN) {
                    // special case: boolean needs to be extended to int64_t!
                    valargs.push_back(builder.CreateZExt(sv.val, _env.i64Type()));
                } else {
                    // push the variable
                    valargs.push_back(sv.val);
                }

                auto vtype = valargs.back()->getType();
                assert(vtype == _env.doubleType() || vtype == _env.i64Type() || vtype == _env.i8ptrType());
            }

            // create constant string from argtypesstr, and insert into arguments
            auto sconst = builder.CreateGlobalStringPtr(argtypesstr);
            auto argtypes = builder.CreatePointerCast(sconst, _env.i8ptrType()); // need gep to cast
            valargs.insert(valargs.begin() + 2, argtypes);

            // make call
            auto replaced_str = builder.CreateCall(strFormat_func, valargs);

            return {replaced_str, builder.CreateLoad(sizeVar)};
        }

        SerializableValue FunctionRegistry::createFindCall(llvm::IRBuilder<> &builder,
                                                           const tuplex::codegen::SerializableValue &caller,
                                                           const tuplex::codegen::SerializableValue &needle) {

            // simple, use helper function
            // call strstr from runtime
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(needle.val->getType() == _env.i8ptrType());

            // use strstr and some pointer arithmetic
            auto strstr_func = ststr_prototype(_env.getContext(), _env.getModule().get());

            // call with needle
            auto strstr_res = builder.CreateCall(strstr_func, {caller.val, needle.val});

            // use select to return value
            auto i8nullptr = llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(_env.i8ptrType()));
            auto empty_cond = builder.CreateICmpEQ(strstr_res, i8nullptr);

            auto res = builder.CreateSelect(empty_cond, _env.i64Const(-1), builder.CreatePtrDiff(strstr_res, caller.val));

            return SerializableValue(res, _env.i64Const(sizeof(int64_t)));
        }

        SerializableValue FunctionRegistry::createIndexCall(tuplex::codegen::LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& needle) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(needle.val->getType() == _env.i8ptrType());

            auto find_res = createFindCall(builder, caller, needle);

            // check if result == -1
            auto found = builder.CreateICmpEQ(find_res.val, _env.i64Const(-1));
            lfb.addException(builder, ExceptionCode::VALUEERROR, found);

            return find_res;
        }

        SerializableValue FunctionRegistry::createReverseIndexCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& needle) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(needle.val->getType() == _env.i8ptrType());

            auto rfind_res = createReverseFindCall(builder, caller, needle);

            // check if result == -1
            auto found = builder.CreateICmpEQ(rfind_res.val, _env.i64Const(-1));
            lfb.addException(builder, ExceptionCode::VALUEERROR, found);

            return rfind_res;
        }

        SerializableValue FunctionRegistry::createCountCall(
            llvm::IRBuilder<> &builder,
            const tuplex::codegen::SerializableValue &caller,
            const tuplex::codegen::SerializableValue &needle) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(needle.val->getType() == _env.i8ptrType());

            auto count_func = count_prototype(_env.getContext(), _env.getModule().get());
            auto count_res = builder.CreateCall(count_func, {caller.val, needle.val, caller.size, needle.size});

            return SerializableValue(count_res, _env.i64Const(sizeof(int64_t)));
        }

        SerializableValue FunctionRegistry::createStartswithCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                 llvm::IRBuilder<> &builder,
                                                                 const tuplex::codegen::SerializableValue &caller,
                                                                 const tuplex::codegen::SerializableValue &prefix) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(prefix.val->getType() == _env.i8ptrType());

            auto res = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);

            // cond to see if the suffix is longer than the string
            auto greaterCond = builder.CreateICmpUGT(prefix.size, caller.size);
            auto isGreater = [&]() {
                return _env.boolConst(false);
            };

            auto startsWithRes = [&]() {
                auto memcmpFunc = memcmp_prototype(_env.getContext(), _env.getModule().get());
                auto n = builder.CreateSub(prefix.size, _env.i64Const(1));
                auto memcmpRes = builder.CreateICmpEQ(_env.i64Const(0), builder.CreateCall(memcmpFunc, {caller.val, prefix.val, n}));
                return _env.upcastToBoolean(builder, memcmpRes);
            };

            constructIfElse(greaterCond, isGreater, startsWithRes, res, lfb, builder);
            return SerializableValue(builder.CreateLoad(res), _env.i64Const(sizeof(int64_t)));
        }

        SerializableValue FunctionRegistry::createEndswithCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                 llvm::IRBuilder<> &builder,
                                                                 const tuplex::codegen::SerializableValue &caller,
                                                                 const tuplex::codegen::SerializableValue &suffix) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(suffix.val->getType() == _env.i8ptrType());

            auto res = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);

            // cond to see if the suffix is longer than the string
            auto greaterCond = builder.CreateICmpUGT(suffix.size, caller.size);
            auto isGreater = [&]() {
                return _env.boolConst(false);
            };

            auto endsWithRes = [&]() {
                auto memcmpFunc = memcmp_prototype(_env.getContext(), _env.getModule().get());
                auto n = builder.CreateSub(suffix.size, _env.i64Const(1));

                auto callerStart = builder.CreateGEP(caller.val, builder.CreateSub(caller.size, suffix.size));
                auto memcmpRes = builder.CreateICmpEQ(_env.i64Const(0), builder.CreateCall(memcmpFunc, {callerStart, suffix.val, n}));
                return _env.upcastToBoolean(builder, memcmpRes);
            };

            constructIfElse(greaterCond, isGreater, endsWithRes, res, lfb, builder);
            return SerializableValue(builder.CreateLoad(res), _env.i64Const(sizeof(int64_t)));
        }

        SerializableValue FunctionRegistry::createReverseFindCall(
            llvm::IRBuilder<> &builder,
            const tuplex::codegen::SerializableValue &caller,
            const tuplex::codegen::SerializableValue &needle) {
          // simple, use helper function
          // call strRFind from runtime
          using namespace llvm;
          assert(caller.val->getType() == _env.i8ptrType());
          assert(needle.val->getType() == _env.i8ptrType());

          // use directly runtime function for this
          auto rfind_func =
              rfind_prototype(_env.getContext(), _env.getModule().get());

          // call
          auto rfind_res =
              builder.CreateCall(rfind_func, {caller.val, needle.val});

          return SerializableValue(rfind_res, _env.i64Const(sizeof(int64_t)));
        }

        SerializableValue FunctionRegistry::createReplaceCall(llvm::IRBuilder<> &builder,
                                                              const tuplex::codegen::SerializableValue &caller,
                                                              const tuplex::codegen::SerializableValue &from,
                                                              const tuplex::codegen::SerializableValue &to) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());
            assert(from.val->getType() == _env.i8ptrType());
            assert(to.val->getType() == _env.i8ptrType());

            auto replace_func = replace_prototype(_env.getContext(), _env.getModule().get());

            // alloc for size
            auto sizeVar = builder.CreateAlloca(_env.i64Type(), 0, nullptr);

            // debug print
            //  caller:  [i8*] : / ->from
            //  from:  [i8*] : abc -> to
            //  to:  [i8*] : /usr/local/hello -> caller
            //  _env.printValue(builder, caller.val, "caller: "); // i.e. is
            //  _env.printValue(builder, from.val, "from: ");
            //  _env.printValue(builder, to.val, "to: ");

            auto replaced_str = builder.CreateCall(replace_func, {caller.val, from.val, to.val, sizeVar});

            return SerializableValue(replaced_str, builder.CreateLoad(sizeVar));
        }

        SerializableValue FunctionRegistry::createJoinCall(llvm::IRBuilder<> &builder, const tuplex::codegen::SerializableValue &caller, const tuplex::codegen::SerializableValue &list) {
            assert(caller.val->getType() == _env.i8ptrType());
            assert(list.val->getType() == _env.getListType(python::Type::makeListType(python::Type::STRING)));

            auto sizeVar = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
            auto joinedStr = builder.CreateCall(strJoin_prototype(_env.getContext(), _env.getModule().get()),
                                                {caller.val, caller.size, builder.CreateExtractValue(list.val, {1}),
                                                 builder.CreateExtractValue(list.val, {2}),
                                                 builder.CreateExtractValue(list.val, {3}), sizeVar});
            return {joinedStr, builder.CreateLoad(sizeVar)};
        }

        SerializableValue FunctionRegistry::createSplitCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<> &builder, const tuplex::codegen::SerializableValue &caller, const tuplex::codegen::SerializableValue &delimiter) {
            assert(caller.val->getType() == _env.i8ptrType());
            assert(delimiter.val->getType() == _env.i8ptrType());

            auto cond = builder.CreateICmpEQ(delimiter.size, _env.i64Const(1)); // empty string
            lfb.addException(builder, ExceptionCode::VALUEERROR, cond); // error if the delimiter is an empty string

            auto lenArray = builder.CreateAlloca(_env.i64ptrType(), 0, nullptr);
            auto strArray = builder.CreateAlloca(llvm::PointerType::get(_env.i8ptrType(), 0), 0, nullptr);
            auto listLen = builder.CreateAlloca(_env.i64Type());
            auto listSerializedSize = builder.CreateCall(strSplit_prototype(_env.getContext(), _env.getModule().get()),
                                                {caller.val, caller.size, delimiter.val, delimiter.size,
                                                 strArray, lenArray, listLen});

            auto res = _env.CreateFirstBlockAlloca(builder, _env.getListType(python::Type::makeListType(python::Type::STRING)));
            builder.CreateStore(builder.CreateLoad(listLen), builder.CreateStructGEP(res, 0));
            builder.CreateStore(builder.CreateLoad(listLen), builder.CreateStructGEP(res, 1));
            builder.CreateStore(builder.CreateLoad(strArray), builder.CreateStructGEP(res, 2));
            builder.CreateStore(builder.CreateLoad(lenArray), builder.CreateStructGEP(res, 3));
            return {builder.CreateLoad(res), listSerializedSize};
        }

#warning "Doesn't support unicode strings"
        SerializableValue FunctionRegistry::createIsDecimalCall(LambdaFunctionBuilder &lfb,
                                                                llvm::IRBuilder<> &builder,
                                                                const SerializableValue &caller) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());

            auto res = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);
            auto isEmpty = builder.CreateICmpEQ(caller.size, _env.i64Const(1));

            auto isEmptyThunk = [&]() {
                return _env.boolConst(false);
            };
            auto isDecimalThunk = [&]() {
                auto isDecimalFunc = isdecimal_prototype(_env.getContext(), _env.getModule().get());
                return _env.upcastToBoolean(builder, builder.CreateCall(isDecimalFunc, {caller.val}));
            };

            constructIfElse(isEmpty, isEmptyThunk, isDecimalThunk, res, lfb, builder);
            return SerializableValue(builder.CreateLoad(res), _env.i64Const(sizeof(int64_t)));
        }

#warning "Doesn't support unicode strings"
        SerializableValue FunctionRegistry::createIsDigitCall(LambdaFunctionBuilder &lfb,
                                                              llvm::IRBuilder<> &builder,
                                                              const SerializableValue &caller) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());

            auto res = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);
            auto isEmpty = builder.CreateICmpEQ(caller.size, _env.i64Const(1));

            auto isEmptyThunk = [&]() {
                return _env.boolConst(false);
            };
            auto isDigitThunk = [&]() {
                auto isdigit_func = isdigit_prototype(_env.getContext(), _env.getModule().get());
                return _env.upcastToBoolean(builder, builder.CreateCall(isdigit_func, {caller.val}));
            };

            constructIfElse(isEmpty, isEmptyThunk, isDigitThunk, res, lfb, builder);
            return SerializableValue(builder.CreateLoad(res), _env.i64Const(sizeof(int64_t)));
        }

#warning "Doesn't support unicode strings"
        SerializableValue FunctionRegistry::createIsAlphaCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                              llvm::IRBuilder<> &builder,
                                                              const tuplex::codegen::SerializableValue &caller) {
            using namespace llvm;
            assert(caller.val->getType() == _env.i8ptrType());

            auto res = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);
            auto isEmpty = builder.CreateICmpEQ(caller.size, _env.i64Const(1));

            auto isEmptyThunk = [&]() {
                return _env.boolConst(false);
            };
            auto isAlphaThunk = [&]() {
                auto isalpha_func = isalpha_prototype(_env.getContext(), _env.getModule().get());
                return _env.upcastToBoolean(builder, builder.CreateCall(isalpha_func, {caller.val}));
            };

            constructIfElse(isEmpty, isEmptyThunk, isAlphaThunk, res, lfb, builder);
            return SerializableValue(builder.CreateLoad(res), _env.i64Const(sizeof(int64_t)));
        }

#warning "Doesn't support unicode strings"
        SerializableValue FunctionRegistry::createIsAlNumCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                              llvm::IRBuilder<> &builder,
                                                              const tuplex::codegen::SerializableValue &caller) {
            auto res = builder.CreateAlloca(_env.getBooleanType(), 0, nullptr);
            auto isEmpty = builder.CreateICmpEQ(caller.size, _env.i64Const(1));

            auto isEmptyThunk = [&]() {
                return _env.boolConst(false);
            };
            auto isAlNumThunk = [&]() {
                auto isalnum_func = isalnum_prototype(_env.getContext(), _env.getModule().get());
                return _env.upcastToBoolean(builder, builder.CreateCall(isalnum_func, {caller.val}));
            };

            constructIfElse(isEmpty, isEmptyThunk, isAlNumThunk, res, lfb, builder);
            return SerializableValue(builder.CreateLoad(res), _env.i64Const(sizeof(int64_t)));
        }


        SerializableValue FunctionRegistry::createStripCall(llvm::IRBuilder<> &builder, const SerializableValue &caller,
                                          const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;
            // check arguments
            assert(caller.val->getType() == _env.i8ptrType());
            assert(args.size() == 0 || args.size() == 1);
            if(args.size() == 1) assert(args[0].val->getType() == _env.i8ptrType());

            auto strip_func = strip_prototype(_env.getContext(), _env.getModule().get());

            auto res_size = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
            // build chars argument
            llvm::Value *chars;
            if(args.size() == 0) chars = llvm::ConstantPointerNull::get(llvm::PointerType::getInt8PtrTy(_env.getContext(), 0));
            else chars = args[0].val;

            // create call
            auto strip_res = builder.CreateCall(strip_func, {caller.val, chars, res_size});

            return SerializableValue(strip_res, builder.CreateAdd(builder.CreateLoad(res_size), _env.i64Const(1)));
        }

        SerializableValue FunctionRegistry::createLStripCall(llvm::IRBuilder<> &builder, const SerializableValue &caller,
                                                            const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;
            // check arguments
            assert(caller.val->getType() == _env.i8ptrType());
            assert(args.size() == 0 || args.size() == 1);
            if(args.size() == 1) assert(args[0].val->getType() == _env.i8ptrType());

            auto strip_func = lstrip_prototype(_env.getContext(), _env.getModule().get());

            auto res_size = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
            // build chars argument
            llvm::Value *chars;
            if(args.size() == 0) chars = llvm::ConstantPointerNull::get(llvm::PointerType::getInt8PtrTy(_env.getContext(), 0));
            else chars = args[0].val;

            // create call
            auto strip_res = builder.CreateCall(strip_func, {caller.val, chars, res_size});

            return SerializableValue(strip_res, builder.CreateAdd(builder.CreateLoad(res_size), _env.i64Const(1)));
        }

        SerializableValue FunctionRegistry::createRStripCall(llvm::IRBuilder<> &builder, const SerializableValue &caller,
                                                            const std::vector<tuplex::codegen::SerializableValue> &args) {
            using namespace llvm;
            // check arguments
            assert(caller.val->getType() == _env.i8ptrType());
            assert(args.size() == 0 || args.size() == 1);
            if(args.size() == 1) assert(args[0].val->getType() == _env.i8ptrType());

            auto strip_func = rstrip_prototype(_env.getContext(), _env.getModule().get());

            auto res_size = builder.CreateAlloca(_env.i64Type(), 0, nullptr);
            // build chars argument
            llvm::Value *chars;
            if(args.size() == 0) chars = llvm::ConstantPointerNull::get(llvm::PointerType::getInt8PtrTy(_env.getContext(), 0));
            else chars = args[0].val;

            // create call
            auto strip_res = builder.CreateCall(strip_func, {caller.val, chars, res_size});

            return SerializableValue(strip_res, builder.CreateAdd(builder.CreateLoad(res_size), _env.i64Const(1)));
        }

        void FunctionRegistry::constructIfElse(llvm::Value *condition, std::function<llvm::Value*(void)> ifCase,
                                                            std::function<llvm::Value*(void)> elseCase,
                                                            llvm::Value *res,
                                                            tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                            llvm::IRBuilder<> &builder) {
            using namespace llvm;

            BasicBlock *ifBB = BasicBlock::Create(_env.getContext(), "if", builder.GetInsertBlock()->getParent());
            BasicBlock *elseBB = BasicBlock::Create(_env.getContext(), "else", builder.GetInsertBlock()->getParent());
            BasicBlock *mergeBB = BasicBlock::Create(_env.getContext(), "merge", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(condition, ifBB, elseBB);
            builder.SetInsertPoint(ifBB);
            Value *ifVal = ifCase();
            builder.CreateStore(ifVal, res);
            builder.CreateBr(mergeBB);

            builder.SetInsertPoint(elseBB);
            Value *elseVal = elseCase();
            builder.CreateStore(elseVal, res);
            builder.CreateBr(mergeBB);

            builder.SetInsertPoint(mergeBB);
            assert(ifVal->getType() == elseVal->getType());

            lfb.setLastBlock(mergeBB);
        }

        codegen::SerializableValue FunctionRegistry::createAttributeCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                         llvm::IRBuilder<> &builder,
                                                                         const std::string &symbol,
                                                                         const python::Type &callerType,
                                                                         const python::Type &argsType,
                                                                         const python::Type &retType,
                                                                         const tuplex::codegen::SerializableValue &caller,
                                                                         const std::vector<tuplex::codegen::SerializableValue> &args) {
            // check
            if(symbol == "upper") {
                //@Todo: checks here...
                return createUpperCall(builder, caller);
            }

            if(symbol == "lower") {
                //@Todo: checks here...
                return createLowerCall(builder, caller);
            }

            if(symbol == "swapcase") {
                if(args.size() != 0)
                    throw std::runtime_error("swapcase takes 0 arguments");
                
                return createSwapcaseCall(builder, caller);
            }
            
            if(symbol == "format") {
                //@Todo: checks here...

                // extract args
                assert(argsType.isTupleType());
                return createFormatCall(builder, caller, args, argsType.parameters());
            }

            if(symbol == "find") {
                // make sure only 1 arg version
                if(args.size() != 1)
                    throw std::runtime_error("str.find is only implemented for the 1 arg version");

                return createFindCall(builder, caller, args.front());
            }

            if(symbol == "rfind") {
                // make sure only 1 arg version
                if(args.size() != 1)
                    throw std::runtime_error("str.rfind is only implemented for the 1 arg version");

                return createReverseFindCall(builder, caller, args.front());
            }

            if(symbol == "strip") {
                // check args
                if(args.size() != 0 && args.size() != 1)
                    throw std::runtime_error("str.strip([chars]) takes at most one argument.");
                return createStripCall(builder, caller, args);
            }

            if(symbol == "lstrip") {
                // check args
                if(args.size() != 0 && args.size() != 1)
                    throw std::runtime_error("str.lstrip([chars]) takes at most one argument.");

                return createLStripCall(builder, caller, args);
            }

            if(symbol == "rstrip") {
                // check args
                if(args.size() != 0 && args.size() != 1)
                    throw std::runtime_error("str.rstrip([chars]) takes at most one argument.");

                return createRStripCall(builder, caller, args);
            }

            if(symbol == "center") {
                if(args.size() == 1) {
                    return createCenterCall(lfb, builder, caller, args[0], nullptr);
                } else if (args.size() == 2) {
                    return createCenterCall(lfb, builder, caller, args[0], &(args[1]));
                } else {
                    throw std::runtime_error("str.center(width, [fillchar]) takes either one or two arguments.");
                }
            }

            if(symbol == "replace") {
                // make sure only 2 arg version
                if(args.size() != 2)
                    throw std::runtime_error("str.replace is only implemented for the 2 arg version");

                return createReplaceCall(builder, caller, args[0], args[1]);
            }

            if(symbol == "join") {
                // make sure exactly 1 argument
                if(args.size() != 1)
                    throw std::runtime_error("str.join only takes one argument");
                return createJoinCall(builder, caller, args[0]);
            }

            if(symbol == "split") {
                // make sure exactly 1 argument
                if(args.size() != 1)
                    throw std::runtime_error("str.split only takes one argument");
                return createSplitCall(lfb, builder, caller, args[0]);
            }

            if(symbol == "startswith") {
                return createStartswithCall(lfb, builder, caller, args.front());
            }

            if(symbol == "endswith") {
                return createEndswithCall(lfb, builder, caller, args.front());
            }

            if(symbol == "count") {
                return createCountCall(builder, caller, args.front());
            }

            if (symbol == "index") {
                return createIndexCall(lfb, builder, caller, args.front());
            }

            if (symbol == "rindex") {
                return createReverseIndexCall(lfb, builder, caller, args.front());
            }

            if (symbol == "isdecimal") {
                return createIsDecimalCall(lfb, builder, caller);
            }

            if (symbol == "isdigit") {
                return createIsDigitCall(lfb, builder, caller);
            }

            if (symbol == "isalpha") {
                return createIsAlphaCall(lfb, builder, caller);
            }

            if (symbol == "isalnum") {
                return createIsAlNumCall(lfb, builder, caller);
            }

            if(symbol == "pop") {
                if(args.size() != 1 && args.size() != 2)
                    throw std::runtime_error("dict.pop(key[, default]) takes 1 or 2 arguments");
                return createCJSONPopCall(lfb, builder, caller, args, argsType.parameters(), retType);
            }

            if(symbol == "popitem") {
                if(args.size() != 0)
                    throw std::runtime_error("dict.popitem() takes 0 arguments");
                return createCJSONPopItemCall(lfb, builder, caller, retType);
            }

            // throw exception
            throw std::runtime_error("attribute call for " + callerType.desc() + "." + symbol + " not yet implemented");

            // else return nullptr
            return SerializableValue(nullptr, nullptr);
        }
    }
}