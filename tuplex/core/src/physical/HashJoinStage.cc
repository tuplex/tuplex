//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/HashJoinStage.h>
#include <CodegenHelper.h>

namespace tuplex {

    HashJoinStage::HashJoinStage(tuplex::PhysicalPlan *plan, tuplex::IBackend *backend, tuplex::PhysicalStage *left,
                                 tuplex::PhysicalStage *right, int64_t leftKeyIndex, const python::Type &leftRowType,
                                 int64_t rightKeyIndex, const python::Type &rightRowType, const tuplex::JoinType &jt,
                                 int64_t stage_number, int64_t outputDataSetID) : PhysicalStage::PhysicalStage(plan,
                                                                                                               backend,
                                                                                                               stage_number,
                                                                                                               {left,
                                                                                                                right}),
                                                                                  _leftKeyIndex(leftKeyIndex),
                                                                                  _rightKeyIndex(rightKeyIndex),
                                                                                  _leftRowType(leftRowType),
                                                                                  _rightRowType(rightRowType),
                                                                                  _joinType(jt),
                                                                                  _outputDataSetID(outputDataSetID) {

    }

    // @TODO: reuse environment provided to TransformStage as well!
    std::string HashJoinStage::generateCode() {
        using namespace std;
        auto env = make_shared<codegen::LLVMEnvironment>("tuplex_fastCodePath_hash");

        // create probe function
        using namespace llvm;

        auto &context = env->getContext();

        // arguments are 1.) userData 2.) the hashmap 3.) input ptr incl. number of rows...
        FunctionType *FT = FunctionType::get(Type::getVoidTy(context),
                                             {env->i8ptrType(), env->i8ptrType(), env->i8ptrType()}, false);

        auto func = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, probeFunctionName(),
                                     env->getModule().get());
        std::vector<llvm::Argument *> args;
        vector<string> argNames{"userData", "hmap", "inputPtr"};
        map<string, Value *> argMap;
        int counter = 0;
        for (auto &arg : func->args()) {
            // first func arg is the ret Value, store separately!
            arg.setName(argNames[counter]);
            argMap[argNames[counter]] = &arg;
            counter++;
        }

        BasicBlock *bbEntry = BasicBlock::Create(context, "entry", func);
        codegen::IRBuilder builder(bbEntry);

        Value *curPtrVar = builder.CreateAlloca(env->i8ptrType(), 0, nullptr);
        builder.CreateStore(argMap["inputPtr"], curPtrVar);
        Value *rowCounterVar = builder.CreateAlloca(env->i64Type(), 0, nullptr, "rowCounterVar");
        builder.CreateStore(env->i64Const(0), rowCounterVar);

        auto hashed_value = builder.CreateAlloca(env->i8ptrType(), 0, nullptr, "hashed_value");
        builder.CreateStore(env->i8nullptr(), hashed_value);

        // read num rows
        Value *numRows = builder.CreateLoad(builder.CreatePointerCast(builder.CreateLoad(curPtrVar), env->i64ptrType()),
                                            "numInputRows");
        // move ptr by int64_t
        builder.CreateStore(builder.CreateGEP(builder.CreateLoad(curPtrVar), env->i64Const(sizeof(int64_t))),
                            curPtrVar);

        // set up
        BasicBlock *bbLoopCondition = BasicBlock::Create(context, "loop_cond", func);
        BasicBlock *bbLoopBody = BasicBlock::Create(context, "loop_body", func);
        BasicBlock *bbLoopExit = BasicBlock::Create(context, "loop_done", func);

        builder.CreateBr(bbLoopCondition);

        // loop cond counter < numRows
        builder.SetInsertPoint(bbLoopCondition);
        auto cond = builder.CreateICmpSLT(builder.CreateLoad(rowCounterVar), numRows);
        builder.CreateCondBr(cond, bbLoopBody, bbLoopExit);


        // logic here...
        builder.SetInsertPoint(bbLoopBody);

        generateProbingCode(env, builder, argMap["userData"], argMap["hmap"], curPtrVar, hashed_value, rightType(),
                            rightKeyIndex(), leftType(), leftKeyIndex(), _joinType);

        auto row_number = builder.CreateLoad(rowCounterVar);
        //env->debugPrint(builder, "row number: ", row_number);
        builder.CreateStore(builder.CreateAdd(env->i64Const(1), builder.CreateLoad(rowCounterVar)), rowCounterVar);
        builder.CreateBr(bbLoopCondition);
        // loop body done

        builder.SetInsertPoint(bbLoopExit);

        // rtfree all
        env->freeAll(builder.get());
        builder.get().CreateRetVoid();

        return env->getIR();
    }


    void HashJoinStage::generateProbingCode(std::shared_ptr<codegen::LLVMEnvironment> &env, codegen::IRBuilder &builder,
                                            llvm::Value *userData, llvm::Value *hashMap, llvm::Value *ptrVar,
                                            llvm::Value *hashedValueVar, const python::Type &buildType,
                                            int buildKeyIndex, const python::Type &probeType, int probeKeyIndex,
                                            const tuplex::JoinType &jt) {
        using namespace llvm;

        assert(ptrVar->getType() == env->i8ptrType()->getPointerTo(0)); // i8**
        assert(probeType.isTupleType());

        auto &context = env->getContext();

        auto &logger = Logger::instance().logger("codegen");
#ifndef NDEBUG
        std::stringstream ss;
        ss << "probe type: " << probeType.desc() << "\n";
        ss << "build type: " << buildType.desc();
        logger.info(ss.str());
#endif

        if (probeType.parameters().size() < buildType.parameters().size())
            logger.warn("sanity check: buildType seems bigger than probeType, reasonable?");

        // deserialize tuple
        codegen::FlattenedTuple ftIn(env.get());
        ftIn.init(probeType);
        auto curPtr = builder.CreateLoad(ptrVar);

        ftIn.deserializationCode(builder.get(), curPtr);

        //// debug print first col
        //env->debugPrint(builder, "first column: ", ftIn.get(0));

        // get key column
        assert(0 <= probeKeyIndex && probeKeyIndex < probeType.parameters().size());
        auto probeKeyType = probeType.parameters()[probeKeyIndex];
        auto keyCol = codegen::SerializableValue(ftIn.get(probeKeyIndex), ftIn.getSize(probeKeyIndex),
                                                 ftIn.getIsNull(probeKeyIndex));

        // env->debugPrint(builder, "probe key column: ", keyCol.val);

        // perform probing
        auto key = makeKey(env, builder, probeKeyType, keyCol);

        assert(key->getType() == env->i8ptrType());

        // probe step
        // hmap i8*, i8*, i8**
        // old code
        // FunctionType *hmap_func_type = FunctionType::get(Type::getInt32Ty(context),
        //                                                  {env->i8ptrType(), env->i8ptrType(),
        //                                                   env->i8ptrType()->getPointerTo(0)}, false);
        // #if LLVM_VERSION_MAJOR < 9
        // auto hmap_get_func = env->getModule()->getOrInsertFunction("hashmap_get", hmap_func_type);
        // #else
        // auto hmap_get_func = env->getModule()->getOrInsertFunction("hashmap_get", hmap_func_type).getCallee();
        // #endif
        // auto in_hash_map = builder.CreateCall(hmap_get_func, {hashMap, key, hashedValueVar});
        // auto found_val = builder.CreateICmpEQ(in_hash_map, env->i32Const(0));

        auto found_val = env->callBytesHashmapGet(builder.get(), hashMap, key, nullptr, hashedValueVar);

        // env->debugPrint(builder, "hmap_get result ", in_hash_map);
        // env->debugPrint(builder, "found value in hashmap", found_val);

        BasicBlock *bbMatchFound = BasicBlock::Create(context, "match_found", builder.GetInsertBlock()->getParent());
        BasicBlock *bbNext = BasicBlock::Create(context, "next", builder.GetInsertBlock()->getParent());


        // branch here on hashmap lookup ==> depends on join type
        if (jt == JoinType::INNER)
            builder.CreateCondBr(found_val, bbMatchFound, bbNext);
        else if (jt == JoinType::LEFT) {
            BasicBlock *bbNullMatch = BasicBlock::Create(context, "right_null", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(found_val, bbMatchFound, bbNullMatch);

            // left join. => If match found, then write join result i.e. general join result code from below.
            // else: write NULL result
            builder.SetInsertPoint(bbNullMatch);
            writeBuildNullResult(env, builder, userData, buildType, buildKeyIndex, ftIn, probeKeyIndex);
            builder.CreateBr(bbNext);
        } else throw std::runtime_error("unknown join type seen in generateProbingCode");

        builder.SetInsertPoint(bbMatchFound);

        // call join code
        writeJoinResult(env, builder, userData, builder.CreateLoad(hashedValueVar), buildType, buildKeyIndex, ftIn,
                        probeKeyIndex);

        builder.CreateBr(bbNext);

        builder.SetInsertPoint(bbNext);

        // advance ptr
        auto serializedSize = ftIn.getSize(builder.get()); // should be 341 for the first row!

        //env->debugPrint(builder, "serialized size:", serializedSize);
        builder.CreateStore(builder.CreateGEP(curPtr, serializedSize), ptrVar);
    }

    llvm::Value *HashJoinStage::makeKey(std::shared_ptr<codegen::LLVMEnvironment> &env, codegen::IRBuilder &builder,
                                        const python::Type &type, const tuplex::codegen::SerializableValue &key) {
        using namespace llvm;
        // create key for different types...

        auto &context = env->getContext();

        if (type == python::Type::STRING)
            return key.val;

        if (type == python::Type::makeOptionType(python::Type::STRING)) {
            // a little more complicated because need to account for nullptr...
            assert(key.val && key.is_null);

            // TODO: special case for null should be prefixed row!
            // i.e. separate null bucket!

            // for now: "" is NULL "_" + val is for keys
            auto skey_ptr = builder.CreateSelect(key.is_null, env->malloc(builder.get(), env->i64Const(1)),
                                                 env->malloc(builder.get(), builder.CreateAdd(key.size, env->i64Const(1))));

            BasicBlock *bbNull = BasicBlock::Create(context, "key_is_null", builder.GetInsertBlock()->getParent());
            BasicBlock *bbNotNull = BasicBlock::Create(context, "key_not_null", builder.GetInsertBlock()->getParent());
            BasicBlock *bbNext = BasicBlock::Create(context, "probe", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(key.is_null, bbNull, bbNotNull);

            builder.SetInsertPoint(bbNull);
            builder.CreateStore(env->i8Const(0), skey_ptr);
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNotNull);
            builder.CreateStore(env->i8Const('_'), skey_ptr);
            builder.CreateMemCpy(builder.CreateGEP(skey_ptr, env->i64Const(1)), 0, key.val, 0, key.size);
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNext); // update builder var!

            return skey_ptr;
        }

        throw std::runtime_error("internal error in makeKey " + std::string(__FILE__) + " unsupported type " + type.desc());
        return nullptr;
    }

    void HashJoinStage::writeJoinResult(std::shared_ptr<codegen::LLVMEnvironment> &env,
                                        IRBuilder &builder, llvm::Value *userData, llvm::Value *bucketPtr,
                                        const python::Type &buildType, int buildKeyIndex,
                                        const codegen::FlattenedTuple &ftProbe, int probeKeyIndex) {
        using namespace llvm;
        using namespace std;

        auto &context = env->getContext();
        auto func = builder.GetInsertBlock()->getParent();
        //env->debugPrint(builder, "joining records with all from bucket :P");

        auto numRows = builder.CreateLoad(builder.CreatePointerCast(bucketPtr, env->i64ptrType()));

        // env->debugPrint(builder, "bucket contains #rows: ", numRows);

        // note: in bucket structure can be further optimized! int64_t row_length = *((int64_t*)rightPtr);
        //                        uint8_t* row_data = rightPtr + sizeof(int64_t);
        //                        rightPtr += sizeof(int64_t) + row_length;

        bucketPtr = builder.CreateGEP(bucketPtr, env->i64Const(sizeof(int64_t)));

        // TODO: put bucketPtr Var in constructor
        auto bucketPtrVar = env->CreateFirstBlockAlloca(builder,
                                                        env->i8ptrType()); //builder.CreateAlloca(env->i8ptrType(), 0, nullptr, "bucketPtrVar");
        builder.CreateStore(bucketPtr, bucketPtrVar);

        // loop over numRows
        // TODO: put counter var in constructor block!
        auto loopVar = env->CreateFirstBlockAlloca(builder,
                                                   env->i64Type()); //builder.CreateAlloca(env->i64Type(), 0, nullptr, "iBucketRowVar");
        builder.CreateStore(env->i64Const(0), loopVar);

        BasicBlock *bbLoopCond = BasicBlock::Create(context, "inbucket_loop_cond", func);
        BasicBlock *bbLoopBody = BasicBlock::Create(context, "inbucket_loop_body", func);
        BasicBlock *bbLoopDone = BasicBlock::Create(context, "inbucket_loop_done", func);

        builder.CreateBr(bbLoopCond);

        builder.SetInsertPoint(bbLoopCond);
        auto cond = builder.CreateICmpSLT(builder.CreateLoad(loopVar), numRows);
        builder.CreateCondBr(cond, bbLoopBody, bbLoopDone);

        builder.SetInsertPoint(bbLoopBody);

        bucketPtr = builder.CreateLoad(bucketPtrVar);
        auto rowLength = builder.CreateLoad(builder.CreatePointerCast(bucketPtr, env->i64ptrType()));
        bucketPtr = builder.CreateGEP(bucketPtr, env->i64Const(sizeof(int64_t)));

        // actual data is now in bucketPtr
        // ==> deserialize!
        // env->debugPrint(builder, "row size is", rowLength);

        // Now: deserialize probe row and build row
        codegen::FlattenedTuple ftBuild(env.get());
        ftBuild.init(buildType);
        ftBuild.deserializationCode(builder, bucketPtr);

        // print out probe key
        // env->debugPrint(builder, "build key column: ", ftBuild.get(buildKeyIndex));

        // combine into one row
        codegen::FlattenedTuple ftResult(env.get());
        ftResult.init(combinedType());

        // add all from probe, then build
        // !!! direction !!!
        int pos = 0;
        for (int i = 0; i < ftProbe.numElements(); ++i) {
            if (i != probeKeyIndex)
                ftResult.assign(pos++, ftProbe.get(i), ftProbe.getSize(i), ftProbe.getIsNull(i));
        }

        // add key from probe
        ftResult.assign(pos, ftProbe.get(probeKeyIndex), ftProbe.getSize(probeKeyIndex),
                        ftProbe.getIsNull(probeKeyIndex));
        pos++;

        // add build elements
        for (int i = 0; i < ftBuild.numElements(); ++i) {
            if (i != buildKeyIndex) {
                ftResult.assign(pos++, ftBuild.get(i), ftBuild.getSize(i), ftBuild.getIsNull(i));

                //env->debugPrint(builder, "build column " + std::to_string(i) + ", value is: ", ftBuild.get(i));
                //env->debugPrint(builder, "build column " + std::to_string(i) + ", size is: ", ftBuild.getSize(i));
                //env->debugPrint(builder, "build column " + std::to_string(i) + ", isnull is: ", ftBuild.getIsNull(i));
            }
        }

        auto buf = ftResult.serializeToMemory(builder);

        // call writeRow function
        FunctionType *writeCallback_type = FunctionType::get(codegen::ctypeToLLVM<int64_t>(context),
                                                             {codegen::ctypeToLLVM<void *>(context),
                                                              codegen::ctypeToLLVM<uint8_t *>(context),
                                                              codegen::ctypeToLLVM<int64_t>(context)}, false);
        auto callback_func = env->getModule()->getOrInsertFunction(writeRowFunctionName(), writeCallback_type);

        builder.CreateCall(callback_func, {userData, buf.val, buf.size});

        // create combined row and write it to output!
        //                        // ==> skip the key column
        //                        // first all left cols, then all right cols
        //                        std::vector<Field> fields;
        //                        for(int i = 0; i < leftRow.getNumColumns(); ++i) {
        //                            if(i != leftKeyIndex)
        //                                fields.push_back(leftRow.get(i));
        //                        }
        //                        fields.push_back(leftRow.get(leftKeyIndex));
        //                        for(int i = 0; i < rightRow.getNumColumns(); ++i) {
        //                            if(i != rightKeyIndex)
        //                                fields.push_back(rightRow.get(i));
        //                        }

        // logic here
        // move bucketPtr
        builder.CreateStore(builder.CreateGEP(builder.CreateLoad(bucketPtrVar),
                                              builder.CreateAdd(env->i64Const(sizeof(int64_t)), rowLength)),
                            bucketPtrVar);


        builder.CreateStore(builder.CreateAdd(builder.CreateLoad(loopVar), env->i64Const(1)), loopVar);
        builder.CreateBr(bbLoopCond);

        builder.SetInsertPoint(bbLoopDone);


        // following is the code to join with data in bucket and write all rows to result partition
        // // loop to put out all rows in bucket
        //                    int64_t num_rows = *((int64_t*)value);
        //
        //                    uint8_t* rightPtr = (uint8_t*)value; rightPtr += sizeof(int64_t);
        //
        //                    auto rightSchema = Schema(Schema::MemoryLayout::ROW, hstage->rightType());
        //                    for(int i = 0; i < num_rows; ++i) {
        //
        //                        int64_t row_length = *((int64_t*)rightPtr);
        //                        uint8_t* row_data = rightPtr + sizeof(int64_t);
        //                        rightPtr += sizeof(int64_t) + row_length;
        //
        //                        Row rightRow = Row::fromMemory(rightSchema,
        //                                row_data, row_length); // value of hashmap is row length + actual value
        //
        //                        //std::cout<<"left row: "<<leftRow.toPythonString()<<" right row: "<<rightRow.toPythonString()<<std::endl;
        //
        //                        // create combined row and write it to output!
        //                        // ==> skip the key column
        //                        // first all left cols, then all right cols
        //                        std::vector<Field> fields;
        //                        for(int i = 0; i < leftRow.getNumColumns(); ++i) {
        //                            if(i != leftKeyIndex)
        //                                fields.push_back(leftRow.get(i));
        //                        }
        //                        fields.push_back(leftRow.get(leftKeyIndex));
        //                        for(int i = 0; i < rightRow.getNumColumns(); ++i) {
        //                            if(i != rightKeyIndex)
        //                                fields.push_back(rightRow.get(i));
        //                        }
        //                        // create joined Row
        //                        Row row = Row::from_vector(fields);
        //
        //                        // serialize out to partition writer
        //                        pw.writeRow(row);
        //                    }

    }

    void HashJoinStage::writeBuildNullResult(std::shared_ptr<codegen::LLVMEnvironment> &env, codegen::IRBuilder &builder,
                                             llvm::Value *userData, const python::Type &buildType, int buildKeyIndex,
                                             const tuplex::codegen::FlattenedTuple &ftProbe, int probeKeyIndex) {
        // Write NULL values for the build row

        using namespace llvm;
        using namespace std;

        auto &context = env->getContext();
        auto func = builder.GetInsertBlock()->getParent();

        // combine into one row
        codegen::FlattenedTuple ftResult(env.get());
        ftResult.init(combinedType());

        codegen::FlattenedTuple ftBuild(env.get());
        ftBuild.init(buildType);

        // add all from probe, then build
        // !!! direction !!!
        int pos = 0;
        for (int i = 0; i < ftProbe.numElements(); ++i) {
            if (i != probeKeyIndex)
                ftResult.assign(pos++, ftProbe.get(i), ftProbe.getSize(i), ftProbe.getIsNull(i));
        }

        // add key from probe
        ftResult.assign(pos, ftProbe.get(probeKeyIndex), ftProbe.getSize(probeKeyIndex),
                        ftProbe.getIsNull(probeKeyIndex));
        pos++;

        // add build elements, i.e. simply NULL for all of them
        for (int i = 0; i < ftBuild.numElements(); ++i) {
            if (i != buildKeyIndex) {
                ftResult.assign(pos++, nullptr, nullptr, env->i1Const(true));
            }
        }

        auto buf = ftResult.serializeToMemory(builder);

        // call writeRow function
        FunctionType *writeCallback_type = FunctionType::get(codegen::ctypeToLLVM<int64_t>(context),
                                                             {codegen::ctypeToLLVM<void *>(context),
                                                              codegen::ctypeToLLVM<uint8_t *>(context),
                                                              codegen::ctypeToLLVM<int64_t>(context)}, false);
        auto callback_func = env->getModule()->getOrInsertFunction(writeRowFunctionName(), writeCallback_type);

        builder.CreateCall(callback_func, {userData, buf.val, buf.size});
    }
}