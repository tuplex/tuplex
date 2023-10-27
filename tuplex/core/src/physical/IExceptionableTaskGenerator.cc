//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "physical/IExceptionableTaskGenerator.h"
#include <Logger.h>



namespace tuplex {

    namespace codegen {
        bool IExceptionableTaskGenerator::createTask(const std::string &name, exceptionHandler_f handler) {
            using namespace llvm;
            auto& context = _env->getContext();

            _name = name;
            _handler = handler;


            // create function with arguments
            auto retType = _env->i64Type();
            std::vector<Type*> paramTypes{llvm::Type::getInt8PtrTy(context, 0),
                                          llvm::Type::getInt8PtrTy(context, 0),
                                          _env->i64Type()};
            std::vector<std::string> paramNames{"userData", "input_ptr", "input_size"};
            assert(paramNames.size() == paramTypes.size());

            FunctionType *FT = FunctionType::get(retType, paramTypes, false);
            auto linkage = Function::ExternalLinkage;
            _func = Function::Create(FT, linkage, _name, _env->getModule().get());

            // name function arguments
            int j = 0;
            for (auto &arg : _func->args()) {
                arg.setName(paramNames[j]);
                _parameters[paramNames[j]] = &arg;
                j++;
            }

            // create all basic blocks necessary for a transform stage task computation in general
            _entryBlock = BasicBlock::Create(context, "entry", _func);
            _taskSuccessBlock = BasicBlock::Create(context, "taskSuccess", _func);


            // minimum variables required for exception handling (to call handler)
            IRBuilder builder(_entryBlock);
            addVariable(builder, "currentInputPtr", llvm::Type::getInt8PtrTy(context, 0), i8nullptr());
            addVariable(builder, "currentInputRowLength", _env->i64Type(), _env->i64Const(0));
            addVariable(builder, "row", _env->i64Type(), _env->i64Const(0));
            addVariable(builder, "outputTotalBytesWritten", _env->i64Type(), _env->i64Const(0));


            // need to add other variables here too to avoid problems. (they will be removed in opt pass later)
            // add variables to entry block

            // exception block
            addVariable(builder, "exceptionCode", _env->i64Type(), _env->i64Const(0));
            addVariable(builder, "exceptionOperatorID", _env->i64Type(), _env->i64Const(0));

            // serialization
            addVariable(builder, "outputBasePtr", llvm::Type::getInt64PtrTy(context, 0), i64nullptr());
            addVariable(builder, "outputPtr", llvm::Type::getInt8PtrTy(context, 0), i8nullptr());
            addVariable(builder, "outputCapacityLeft", _env->i64Type(), _env->i64Const(0));
            addVariable(builder, "outputRowsWritten", _env->i64Type(), _env->i64Const(0));


            // Setup SUCCESS Block:
            // success loop block, return number of (total) bytes written to N output partitions
            builder.SetInsertPoint(_taskSuccessBlock);
            builder.CreateRet(getVariable(builder, "outputTotalBytesWritten"));

            _lastBlock = _entryBlock;
            return true;
        }

        void IExceptionableTaskGenerator::createExceptionBlock() {
            using namespace llvm;
            auto& context = _env->getContext();


            _exceptionBlock= BasicBlock::Create(context, "exception", _func);

            // generate actual exception block
            IRBuilder builder(_exceptionBlock);

            // EH handling should be implemented here...
            if(_handler) { // only add call to handler if a valid pointer is given

                // create call to exception handler
                std::vector<Type *> eh_argtypes{Type::getInt8PtrTy(context, 0),
                                                _env->i64Type(),
                                                _env->i64Type(),
                                                _env->i64Type(),
                                                Type::getInt8PtrTy(context, 0),
                                                _env->i64Type()};
                FunctionType *eh_FT = FunctionType::get(Type::getVoidTy(context), eh_argtypes, false);

                auto eh_func_ptr_type = PointerType::get(eh_FT, 0);

#warning "read comments here before trying to create a multi-node system"
                // @Todo: thread safe function required here.
                // the address of the function can be only determined during runtime. Hence, when shipping LLVM IR Code across a cluster,
                // the current process needs to be inspected and the address of the function be inserted.
                auto eh_func_addr = _env->i64Const(reinterpret_cast<int64_t>(_handler));

                auto eh_func = builder.CreateIntToPtr(eh_func_addr, eh_func_ptr_type, "exceptionHandler");


                // parameters for the call
                auto ehcode = getVariable(builder, "exceptionCode");//builder.CreateLoad(_exceptionCodeVar);
                auto ehopid = getVariable(builder, "exceptionOperatorID");//builder.CreateLoad(_exceptionOperatorIDVar);
                auto row = getVariable(builder, "row");//builder.CreateLoad(_rowCounterVar);
                auto inputlength = getVariable(builder, "currentInputRowLength");//builder.CreateLoad(_)


                // adjust inputptr (has been already updated) to previous row uwsing inputlength
                auto inputptr = builder.MovePtrByBytes(getVariable(builder, "currentInputPtr"),
                                                  builder.CreateNeg(inputlength));
                std::vector<llvm::Value *> eh_parameters{_parameters["userData"], ehcode, ehopid, row, inputptr,
                                                         inputlength};
                builder.CreateCall(eh_func, eh_parameters);

            }
        }

        void IExceptionableTaskGenerator::createTaskFailureBlock() {
            _taskFailureBlock = llvm::BasicBlock::Create(_env->getContext(), "taskFailure", _func);

            // Setup FAILURE Block:
            // success loop block, return number of bytes written
            llvm::IRBuilder<> builder(_taskFailureBlock);
            // negative numbers for failure!
            builder.CreateRet(_env->i64Const(-1));
        }

        void IExceptionableTaskGenerator::addVariable(IRBuilder &builder, const std::string name, llvm::Type *type,
                                                      llvm::Value *initialValue) {
            _variables[name] = builder.CreateAlloca(type, 0, nullptr, name);

            if(initialValue)
                builder.CreateStore(initialValue, _variables[name]);
        }

        llvm::Value* IExceptionableTaskGenerator::getVariable(IRBuilder &builder, const std::string name) {
            assert(_variables.find(name) != _variables.end());
            return builder.CreateLoad(_variables[name]);
        }

        llvm::Value* IExceptionableTaskGenerator::getPointerToVariable(IRBuilder &builder, const std::string name) {
            assert(_variables.find(name) != _variables.end());
            return _variables[name];
        }

        void IExceptionableTaskGenerator::assignToVariable(IRBuilder &builder, const std::string name,
                                                           llvm::Value *newValue) {
            assert(_variables.find(name) != _variables.end());
            builder.CreateStore(newValue, _variables[name]);
        }

        void IExceptionableTaskGenerator::linkBlocks() {

            auto builder = getBuilder();

            // check whether taskSuccess has predecessor and current block has successors
            if(!hasPredecessor(taskSuccessBlock()) && !hasSuccessor(builder.GetInsertBlock())) {
                builder.CreateBr(taskSuccessBlock());
            }

            assert(hasPredecessor(taskSuccessBlock()));
            if(_taskFailureBlock)
                assert(hasPredecessor(_taskFailureBlock));
            if(_exceptionBlock)
                assert(hasPredecessor(_exceptionBlock));

        }

        bool IExceptionableTaskGenerator::toMemory(IExceptionableTaskGenerator::reqMemory_f requestOutputMemory,
                                                   const FlattenedTuple& ft) {
            using namespace llvm;
            using namespace std;
            // create code to cast this to a pointer (to call that)
            auto builder = getBuilder();
            auto& context = _env->getContext();

            // add blocks & logic for serialization
            llvm::Value *numRowsPtr = nullptr;
            llvm::Value *numTotalBytesWritten = nullptr;


            // serialization code with check if enough capacity is left
            python::Type outputSchema = ft.getTupleType();


            // check if something needs to be serialized or not. If not, do not bother with serialization code
            assert(outputSchema.isTupleType());
            if(outputSchema.parameters().size() == 0) {
                // nothing todo...

            } else {
                // should never violate this
                assert(ft.numElements() > 0);

                // start block of serialization
                BasicBlock *serializeBlock = BasicBlock::Create(context, "serialize", _func);
                builder.CreateBr(serializeBlock);

                builder.SetInsertPoint(serializeBlock);
#ifndef NDEBUG
                Logger::instance().defaultLogger().info("final output type is: " + outputSchema.desc());
#endif
                auto output = getVariable(builder, "outputPtr");
                auto capacity = getVariable(builder, "outputCapacityLeft");
                BasicBlock *insufficientCapacityBB = BasicBlock::Create(context, "getNewOutputPartition", _func);


                // Following instructions are inserted into serialization block
                auto remainingCapacity = capacity;//builder.CreateSub(capacity, _env->i64Const(8));

                llvm::Value *serializedRowSize = ft.serializationCode(builder, output, remainingCapacity, insufficientCapacityBB);

                // get regular handler for serialization
                BasicBlock *serializeToMemoryBB = builder.GetInsertBlock();

                if(!serializedRowSize) { //nullptr means the code could not be generated (i.e. missing implementation for special types)
                    Logger::instance().defaultLogger().error("could not generate code to serialize data.");
                    return false;
                }

                // store back some variables in then block & make sure to mark as last block!
                // add to variable how much was serialized
                auto newoutput = builder.CreateGEP(output, serializedRowSize);
                assignToVariable(builder, "outputPtr", newoutput);
                auto newcapacity = builder.CreateSub(capacity, serializedRowSize);
                assignToVariable(builder, "outputCapacityLeft", newcapacity);
                auto totalRowsWritten = builder.CreateAdd(getVariable(builder, "outputRowsWritten"), _env->i64Const(1));
                assignToVariable(builder, "outputRowsWritten", totalRowsWritten);

                // inc how many rows are written
                numRowsPtr = getVariable(builder, "outputBasePtr");
                auto curRows = builder.CreateLoad(numRowsPtr);
                builder.CreateStore(builder.CreateAdd(curRows, _env->i64Const(1)), numRowsPtr);

                // inc how many writtes are written
                numTotalBytesWritten = getVariable(builder, "outputTotalBytesWritten");
                assignToVariable(builder, "outputTotalBytesWritten", builder.CreateAdd(numTotalBytesWritten, serializedRowSize));

                // update lastblock with this...
                _lastBlock = serializeToMemoryBB;


                // ------------
                // arrive in this block IF there was not capacity left to store results.
                // request new memory by calling external C function requestOutputMemory

                builder.SetInsertPoint(insufficientCapacityBB);
                vector<Type*> argtypes{Type::getInt8PtrTy(context, 0), _env->i64Type(), _env->i64Type()->getPointerTo(0)};
                FunctionType *FT = FunctionType::get(Type::getInt8PtrTy(context, 0), argtypes, false);

                auto func_ptr_type = PointerType::get(FT, 0);

#warning "read comments here before trying to create a multi-node system"
                // @Todo: thread safe function required here.
                // the address of the function can be only determined during runtime. Hence, when shipping LLVM IR Code across a cluster,
                // the current process needs to be inspected and the address of the function be inserted.
                auto func_addr = _env->i64Const(reinterpret_cast<int64_t>(requestOutputMemory));

                auto func = builder.CreateIntToPtr(func_addr, func_ptr_type, "requestOutputMemory");


                // parameters for the call
                auto output_capacity = getPointerToVariable(builder, "outputCapacityLeft");
                auto minRequired = builder.CreateAdd(serializedRowSize, _env->i64Const(sizeof(int64_t))); // add numrow field
                vector<llvm::Value*> parameters{_parameters["userData"], minRequired, output_capacity};

                auto output_ptr = builder.CreateCall(func, parameters, "output_ptr");
                // first save back to variables the memory request incl. 8 byte offset for number of rows!
                assignToVariable(builder, "outputBasePtr", builder.CreatePointerCast(output_ptr, _env->i64Type()->getPointerTo(0)));
                assignToVariable(builder, "outputPtr", builder.CreateGEP(output_ptr, _env->i32Const(sizeof(int64_t))));

                // check for null. If so (i.e. no memory returned), if so exit task function immediately
                // --> also if capacity returned is less than minRequested.
                auto valid_capacity = builder.CreateICmpSGE(getVariable(builder, "outputCapacityLeft"), minRequired);
                auto valid_memory = builder.CreateICmpNE(output_ptr, llvm::ConstantPointerNull::get(Type::getInt8PtrTy(context, 0)));
                auto validmem_cond = builder.CreateAnd(valid_capacity, valid_memory);

                // need one more helper block which initializes memory region (could be left out when external request does that...)
                // ==> do here to not introduce a bug...

                BasicBlock *initOutputBB = BasicBlock::Create(context, "initOutput", _func);
                builder.CreateCondBr(validmem_cond, initOutputBB, taskFailureBlock());


                builder.SetInsertPoint(initOutputBB);
                // store 0 to outputBasePtr
                numRowsPtr = getVariable(builder, "outputBasePtr");
                builder.CreateStore(_env->i64Const(0), numRowsPtr);
                // go to serialize block
                builder.CreateBr(serializeBlock);
            }

            return true;
        }
    }
}