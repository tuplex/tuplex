//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <BlockGeneratorVisitor.h>
#include <FlattenedTuple.h>
#include <ASTHelpers.h>
#include <StringUtils.h>
#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#include <TypeAnnotatorVisitor.h>
#include <ApplyVisitor.h>

using namespace llvm;

//@Todo
#warning "give the generated function for its first argument the attributes sret and noalias as described in https://media.readthedocs.org/pdf/mapping-high-level-constructs-to-llvm-ir/latest/mapping-high-level-constructs-to-llvm-ir.pdf p.18"


bool blockContainsRet(llvm::BasicBlock *bb) {
    assert(bb);
    return llvm::isa<llvm::ReturnInst>(bb->back());
}

// for both condbr or br
bool blockContainsBr(llvm::BasicBlock *bb) {
    assert(bb);
    return llvm::isa<llvm::BranchInst>(bb->back());
}

// block is open when there is no ret nor br instruction
bool blockOpen(llvm::BasicBlock *bb) {
    if (!bb)
        return false;

    return !blockContainsRet(bb) && !blockContainsBr(bb);
}

// @TODO: refactor this and make the expression check more elegant...

namespace tuplex {
    namespace codegen {

        int BlockGeneratorVisitor::_lambdaCounter = 0;

        void BlockGeneratorVisitor::error(const std::string &message, const std::string &logger) {
            IFailable::error(message, logger);

            // no error tokens etc. yet, fail on the first error.
            throw std::runtime_error(message);
        }

        void BlockGeneratorVisitor::visit(NSuite* suite) {
            if(earlyExit())return;

            if(suite->_isUnrolledLoopSuite) {
                return visitUnrolledLoopSuite(suite);
            }

            for (auto & stmt : suite->_statements) {
                // emit code for each statement. yet, there's one fallacy: If a return occurred, the last block of lfb
                // will become empty. Thus, the generation should be stopped!
                if(_lfb && !_lfb->getLastBlock())
                    break;
                setLastParent(suite);
                stmt->accept(*this);

                // stop if any 'break' or 'continue' encountered
                if(stmt->type() == ASTNodeType::Break || stmt->type() == ASTNodeType::Continue) {
                    break;
                }
            }
        }

        void BlockGeneratorVisitor::visit(NNone *none) {
            if(earlyExit())return;

            // add SerializableValue with null indicator set to true!
            _blockStack.push_back(SerializableValue(nullptr, nullptr, _env->i1Const(true))); // ... is_null=true!
        }

        void BlockGeneratorVisitor::visit(NNumber *num) {
            if(earlyExit())return;

            // depending on type, generate constant literal & push to stack
            if (python::Type::I64 == num->getInferredType()) {
                addInstruction(ConstantInt::get(_env->getContext(), APInt(64, num->getI64())));
            } else if (python::Type::F64 == num->getInferredType()) {
                addInstruction(ConstantFP::get(_env->getContext(), APFloat(num->getF64())));
            } else {
                error("invalid type '"
                      + num->getInferredType().desc()
                      + "' for number literal encountered. ");
            }
        }

        void BlockGeneratorVisitor::visit(NBoolean *boolean) {
            if(earlyExit())return;

            // create value corresponding to type
            assert(boolean);
            addInstruction(_env->boolConst(boolean->_value));
        }

        llvm::Value *BlockGeneratorVisitor::upCast(IRBuilder<> &builder, llvm::Value *val, llvm::Type *type) {
            // check if types are the same, then just return val
            if (val->getType() == type)
                return val;
            else {
                // is boolean?
                if (val->getType() == _env->getBooleanType()) {
                    // goal can be either i64 or double
                    if (type == _env->i64Type())
                        return builder.CreateZExt(val, _env->i64Type());
                    else if (type == _env->doubleType())
                        return builder.CreateSIToFP(val, _env->doubleType());
                    else {
                        error("fatal error: could not upcast type");
                        return nullptr;
                    }
                } else if (val->getType() == _env->i64Type()) {
                    // only upcast to double possible
                    if (type == _env->doubleType())
                        return builder.CreateSIToFP(val, _env->doubleType());
                    else {
                        error("upcast only to double possible!");
                        return nullptr;
                    }
                } else if (val->getType() == _env->doubleType()) {
                    // if already double, all ok
                    if (type == _env->doubleType())
                        return val;
                    else {
                        error("can't upcast double further");
                        return nullptr;
                    }
                } else {
                    std::stringstream ss;
                    ss << "no upcast from " << _env->getLLVMTypeName(val->getType()) << " to "
                       << _env->getLLVMTypeName(type) << " possible. Wrong parameters?";
                    error(ss.str());
                    return nullptr;
                }
            }
        }

        SerializableValue BlockGeneratorVisitor::multiplicationInst(const SerializableValue &L, NBinaryOp *op,
                                                                    const SerializableValue &R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // special case:
            // overloaded string operator

            if ((ltype == python::Type::STRING && (rtype == python::Type::I64 || rtype == python::Type::BOOLEAN))
                || (rtype == python::Type::STRING && (ltype == python::Type::I64 || ltype == python::Type::BOOLEAN))) {
                // Unpack the parameters by type
                SerializableValue str;
                Value *num = nullptr;
                bool num_is_bool = false;
                if (ltype == python::Type::STRING) {
                    str = L;
                    num = R.val;
                    num_is_bool = (rtype == python::Type::BOOLEAN);
                } else {
                    str = R;
                    num = L.val;
                    num_is_bool = (ltype == python::Type::BOOLEAN);
                }

                // Create basic blocks
                auto emptyBlock = BasicBlock::Create(_env->getContext(), "retemptystr",
                                                     builder.GetInsertBlock()->getParent());
                auto origBlock = BasicBlock::Create(_env->getContext(), "retorigstr",
                                                    builder.GetInsertBlock()->getParent());
                auto retBlock = BasicBlock::Create(_env->getContext(), "retstr", builder.GetInsertBlock()->getParent());

                // local variables
                auto retval = builder.CreateAlloca(_env->i8ptrType(), 0, nullptr);
                auto retsize = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
                auto loopvar = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);

                // conditional break whether to return empty string
                auto strisempty = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLE, str.size, _env->i64Const(1));
                if (num_is_bool) {
                    // branch on whether we return an empty string (or the original)
                    auto mulbyfalse = builder.CreateICmpEQ(num, _env->i8Const(0));
                    auto retemptystr = builder.CreateOr(strisempty, mulbyfalse);
                    builder.CreateCondBr(retemptystr, emptyBlock, origBlock);
                } else {
                    // blocks that are only needed for multiplication by a number
                    auto nonemptyBlock = BasicBlock::Create(_env->getContext(), "nonemptystr",
                                                            builder.GetInsertBlock()->getParent());
                    auto loopsetupBlock = BasicBlock::Create(_env->getContext(), "setupstrloop",
                                                             builder.GetInsertBlock()->getParent());
                    auto loopBlock = BasicBlock::Create(_env->getContext(), "duplicatestrloop",
                                                        builder.GetInsertBlock()->getParent());
                    auto loopretBlock = BasicBlock::Create(_env->getContext(), "strloopret",
                                                           builder.GetInsertBlock()->getParent());

                    // branch based on whether you can return an empty string
                    auto mulle0 = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLE, num, _env->i64Const(0));
                    auto retemptystr = builder.CreateOr(strisempty, mulle0);
                    builder.CreateCondBr(retemptystr, emptyBlock, nonemptyBlock);

                    // Nonempty String Block
                    builder.SetInsertPoint(nonemptyBlock);
                    auto retorigstr = builder.CreateICmp(llvm::CmpInst::ICMP_EQ, num,
                                                         _env->i64Const(1)); // branch if we can return original string
                    builder.CreateCondBr(retorigstr, origBlock, loopsetupBlock);

                    // Loop Setup Block for Duplication
                    builder.SetInsertPoint(loopsetupBlock);
                    // set up for loop by allocating memory, setting up loop counter
                    auto origstrlen = builder.CreateSub(str.size, _env->i64Const(1)); // get some relevant lengths
                    auto strlen = builder.CreateMul(origstrlen, num);
                    auto duplen = builder.CreateAdd(strlen, _env->i64Const(1));
                    builder.CreateStore(num, loopvar); // set up loop counter
                    auto allocmem = _env->malloc(builder, duplen); // allocate memory
                    builder.CreateBr(loopBlock);

                    // Loop Block
                    builder.SetInsertPoint(loopBlock);
                    // decrement loop variable
                    auto loopvarval = builder.CreateLoad(loopvar);
                    auto newloopvar = builder.CreateSub(loopvarval, _env->i64Const(1));
                    builder.CreateStore(newloopvar, loopvar);
                    // copy in memory

#if LLVM_VERSION_MAJOR < 9
                    builder.CreateMemCpy(builder.CreateGEP(builder.getInt8Ty(), allocmem,
                                                           builder.CreateMul(newloopvar, origstrlen)), str.val, origstrlen, false);
#else
                    // API update here, old API only allows single alignment.
                    // new API allows src and dest alignment separately
                    auto destPtr = builder.CreateGEP(builder.getInt8Ty(), allocmem,
                                                     builder.CreateMul(newloopvar, origstrlen));
                    auto srcPtr = str.val;
                    auto srcSize = origstrlen;
                    builder.CreateMemCpy(destPtr, 0, srcPtr, 0, srcSize, true);
#endif


                    // jump back to loop if more iterations needed
                    auto loopagain = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SGT, newloopvar,
                                                        _env->i64Const(0));
                    builder.CreateCondBr(loopagain, loopBlock, loopretBlock);

                    // Loop Return Block
                    builder.SetInsertPoint(loopretBlock);
                    builder.CreateStore(_env->i8Const('\0'), builder.CreateGEP(builder.getInt8Ty(),
                                                                               allocmem,
                                                                               strlen)); // null terminate the result
                    builder.CreateStore(allocmem, retval); // save the result into the return variables
                    builder.CreateStore(duplen, retsize);
                    builder.CreateBr(retBlock);
                }

                // Empty String Block
                builder.SetInsertPoint(emptyBlock);
                auto emptystr = _env->malloc(builder, _env->i64Const(1)); // make null terminated empty string
                builder.CreateStore(_env->i8Const('\0'), emptystr);
                builder.CreateStore(emptystr, retval); // save result in ret local vars
                builder.CreateStore(_env->i64Const(1), retsize);
                builder.CreateBr(retBlock); // jump to return block

                // Return Original String Block
                builder.SetInsertPoint(origBlock);
                builder.CreateStore(str.val, retval); // save original string to return
                builder.CreateStore(str.size, retsize);
                builder.CreateBr(retBlock);

                // Overall Return Block (from lambda function)
                builder.SetInsertPoint(retBlock);
                auto ret = SerializableValue(builder.CreateLoad(retval), builder.CreateLoad(retsize));
                _lfb->setLastBlock(retBlock);
                return ret;
            }

            // get supertype
            auto superType = python::Type::superType(ltype, rtype);

            if (superType == python::Type::UNKNOWN)
                return SerializableValue(logErrorV("could not find supertype!"), nullptr);

            // special case: boolean
            // => arithmetic instructions autocast bool then to int!
            if (python::Type::BOOLEAN == superType)
                superType = python::Type::I64;

            auto type = _env->pythonToLLVMType(superType);

            // create casted versions
            auto uL = upCast(builder, L.val, type);
            auto uR = upCast(builder, R.val, type);

            // choose floating point or integer operation
            if (type->isDoubleTy())
                return SerializableValue(builder.CreateFMul(uL, uR), nullptr);
            else
                return SerializableValue(builder.CreateMul(uL, uR), nullptr);
        }

        Value *BlockGeneratorVisitor::integerDivisionInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // special case:
            // overloaded string operator

            if ((ltype == python::Type::STRING && (rtype == python::Type::I64 || rtype == python::Type::BOOLEAN))
                || (rtype == python::Type::STRING && (ltype == python::Type::I64 || ltype == python::Type::BOOLEAN))) {
                return logErrorV("strings do not implement //");
            }

            // get supertype
            auto superType = python::Type::superType(ltype, rtype);

            if (superType == python::Type::UNKNOWN) {
                return logErrorV("could not find supertype!");
            }

            // special case: boolean
            // => arithmetic instructions autocast bool then to int!
            if (python::Type::BOOLEAN == superType)
                superType = python::Type::I64;

            auto type = _env->pythonToLLVMType(superType);

            // create casted versions
            auto uL = upCast(builder, L, type);
            auto uR = upCast(builder, R, type);

            // exception handling if switched on
            if (!_allowUndefinedBehaviour) {
                // check if right side is zero
                auto iszero = type->isDoubleTy() ? builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OEQ, uR,
                                                                      _env->f64Const(0.0)) :
                              builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ, uR, _env->i64Const(0));
                _lfb->addException(builder, ExceptionCode::ZERODIVISIONERROR, iszero);
            } // normal code goes on

            // choose floating point or integer operation
            if (type->isDoubleTy()) {
                // first cast to integer, then perform signed integer div and then cast back to floating point
                auto castL = builder.CreateFPToSI(uL, _env->i64Type());
                auto castR = builder.CreateFPToSI(uR, _env->i64Type());
                auto div_res = _env->floorDivision(builder, castL, castR);
                return builder.CreateSIToFP(div_res, _env->doubleType());
            } else {
                return _env->floorDivision(builder, uL, uR);
            }
        }

        SerializableValue
        BlockGeneratorVisitor::additionInst(const SerializableValue &L, NBinaryOp *op, const SerializableValue &R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // overloaded: string addition = concatenation
            if (ltype == python::Type::STRING
                && rtype == python::Type::STRING) {
                auto concatBlock = BasicBlock::Create(_env->getContext(), "strconcat",
                                                      builder.GetInsertBlock()->getParent());
                auto emptyBlock = BasicBlock::Create(_env->getContext(), "emptystring",
                                                     builder.GetInsertBlock()->getParent());
                auto retBlock = BasicBlock::Create(_env->getContext(), "retstring",
                                                   builder.GetInsertBlock()->getParent());

                auto lnonempty = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SGT, L.size, _env->i64Const(1));
                auto rnonempty = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SGT, R.size, _env->i64Const(1));
                auto bothnonempty = builder.CreateAnd(lnonempty, rnonempty);
                auto retval = builder.CreateAlloca(_env->i8ptrType(), 0, nullptr);
                auto retsize = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);

                builder.CreateCondBr(bothnonempty, concatBlock, emptyBlock);

                builder.SetInsertPoint(concatBlock);
                auto llen = builder.CreateSub(L.size, _env->i64Const(1));
                auto concatsize = builder.CreateAdd(R.size, llen);
                auto concatval = _env->malloc(builder, concatsize);

#if LLVM_VERSION_MAJOR < 9
                builder.CreateMemCpy(builder.CreateGEP(builder.getInt8Ty(), concatval, _env->i64Const(0)), L.val, llen, false);
                builder.CreateMemCpy(builder.CreateGEP(builder.getInt8Ty(), concatval, llen), R.val, R.size, false);
#else
                // API update here, old API only allows single alignment.
                // new API allows src and dest alignment separately
                builder.CreateMemCpy(builder.CreateGEP(builder.getInt8Ty(), concatval, _env->i64Const(0)), 0, L.val,
                                     0,
                                     llen, false);
                builder.CreateMemCpy(builder.CreateGEP(builder.getInt8Ty(), concatval, llen), 0, R.val, 0, R.size,
                                     false);
#endif

                builder.CreateStore(concatval, retval);
                builder.CreateStore(concatsize, retsize);
                builder.CreateBr(retBlock);

                builder.SetInsertPoint(emptyBlock);
                auto paramval = builder.CreateSelect(lnonempty, L.val, R.val);
                auto paramsize = builder.CreateSelect(lnonempty, L.size, R.size);
                builder.CreateStore(paramval, retval);
                builder.CreateStore(paramsize, retsize);
                builder.CreateBr(retBlock);

                builder.SetInsertPoint(retBlock);
                auto ret = SerializableValue(builder.CreateLoad(retval), builder.CreateLoad(retsize));
                _lfb->setLastBlock(retBlock);
                return ret;
            } else {
                // get supertype
                auto superType = python::Type::superType(ltype, rtype);

                // special case: boolean
                // => arithmetic instructions autocast bool then to int!
                if (python::Type::BOOLEAN == superType)
                    superType = python::Type::I64;

                if (superType == python::Type::UNKNOWN) {
                    return SerializableValue(logErrorV("could not find supertype!"), nullptr);
                }

                auto type = _env->pythonToLLVMType(superType);
                // create casted versions
                auto uL = upCast(builder, L.val, type);
                auto uR = upCast(builder, R.val, type);

                // choose floating point or integer operation
                if (type->isDoubleTy())
                    return SerializableValue(builder.CreateFAdd(uL, uR), nullptr);
                else
                    return SerializableValue(builder.CreateAdd(uL, uR), nullptr);
            }
        }

        Value *BlockGeneratorVisitor::subtractionInst(Value *L, NBinaryOp *op, Value *R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // strings not yet supported
            if (ltype == python::Type::STRING
                && rtype == python::Type::STRING)
                return logErrorV("TypeError: unsupported operand type(s) for -: 'str' and 'str'");
            else {
                // get supertype
                auto superType = python::Type::superType(ltype, rtype);

                // special case: boolean
                // => arithmetic instructions autocast bool then to int!
                if (python::Type::BOOLEAN == superType)
                    superType = python::Type::I64;

                if (superType == python::Type::UNKNOWN) {
                    return logErrorV("could not find supertype!");
                }

                auto type = _env->pythonToLLVMType(superType);
                // create casted versions
                auto uL = upCast(builder, L, type);
                auto uR = upCast(builder, R, type);

                // choose floating point or integer operation
                if (type->isDoubleTy())
                    return builder.CreateFSub(uL, uR);
                else
                    return builder.CreateSub(uL, uR);
            }
        }

        Value *BlockGeneratorVisitor::divisionInst(Value *L, NBinaryOp *op, Value *R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // special case:
            // overloaded string operator

            if ((ltype == python::Type::STRING && (rtype == python::Type::I64 || rtype == python::Type::BOOLEAN))
                || (rtype == python::Type::STRING && (ltype == python::Type::I64 || ltype == python::Type::BOOLEAN))) {
                return logErrorV("invalid operators");
            }

            // division in Python3 always results in float.

            auto type = _env->pythonToLLVMType(python::Type::F64);

            // create casted versions
            auto uL = upCast(builder, L, type);
            auto uR = upCast(builder, R, type);

            // exception handling if switched on
            if (!_allowUndefinedBehaviour) {
                // check if right side is zero
                auto iszero = builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OEQ, uR, _env->f64Const(0.0));
                _lfb->addException(builder, ExceptionCode::ZERODIVISIONERROR, iszero);
            } // normal code goes on

            return builder.CreateFDiv(uL, uR);
        }

        Value *BlockGeneratorVisitor::moduloInst(Value *L, NBinaryOp *op, Value *R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // special case:
            // overloaded string operator

            if ((ltype == python::Type::STRING && (rtype == python::Type::I64 || rtype == python::Type::BOOLEAN))
                || (rtype == python::Type::STRING && (ltype == python::Type::I64 || ltype == python::Type::BOOLEAN))) {
                return logErrorV("string formatting not yet supported!");
            }

            // get supertype
            auto superType = python::Type::superType(ltype, rtype);

            if (superType == python::Type::UNKNOWN) {
                return logErrorV("could not find supertype!");
            }

            // special case: boolean
            // => arithmetic instructions autocast bool then to int!
            if (python::Type::BOOLEAN == superType)
                superType = python::Type::I64;

            auto type = _env->pythonToLLVMType(superType);

            // create casted versions
            auto uL = upCast(builder, L, type);
            auto uR = upCast(builder, R, type);

            // exception code (also throw div by zero exception here)
            if (!_allowUndefinedBehaviour) {
                // check if right side is zero
                auto iszero = type->isDoubleTy() ? builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OEQ, uR,
                                                                      _env->f64Const(0.0)) :
                              builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ, uR, _env->i64Const(0));
                _lfb->addException(builder, ExceptionCode::ZERODIVISIONERROR, iszero);
            } // normal code goes on

            // Python3 uses floored modulo
            //  // choose floating point or integer operation
            //  if (type->isDoubleTy())
            //      return builder.CreateFRem(uL, uR);
            //  else
            //      return builder.CreateSRem(uL, uR);

            return _env->floorModulo(builder, uL, uR);
        }

        SerializableValue BlockGeneratorVisitor::logicalAndInst(NBinaryOp *op) {
            using namespace python;

            NBoolean b(false);
            NIfElse ifelse(op->_left, op->_right, &b, true);
            ifelse.setInferredType(python::Type::BOOLEAN);
            generateIfElseExpression(&ifelse, true);
            assert(!_blockStack.empty());
            auto res = _blockStack.back();
            _blockStack.pop_back();
            return res;
        }

        SerializableValue BlockGeneratorVisitor::logicalOrInst(NBinaryOp *op) {
            using namespace python;

            NBoolean b(true);
            NIfElse ifelse(op->_left, &b, op->_right, true);
            ifelse.setInferredType(python::Type::BOOLEAN);
            generateIfElseExpression(&ifelse, true);
            assert(!_blockStack.empty());
            auto res = _blockStack.back();
            _blockStack.pop_back();
            return res;
        }

        Value *BlockGeneratorVisitor::leftShiftInst(Value *L, NBinaryOp *op, Value *R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            if (!(ltype == python::Type::I64 || ltype == python::Type::BOOLEAN) ||
                !(rtype == python::Type::I64 || rtype == python::Type::BOOLEAN)) {
                return logErrorV(
                        "TypeError: unsupported operand type(s) for <<: '" + ltype.desc() + "' and '" + rtype.desc() +
                        "'");
            }

            // TODO: Consider NSW/NUW for overflow

            // upcast to 64 bits
            auto uL = upCast(builder, L, _env->i64Type());
            auto uR = upCast(builder, R, _env->i64Type());

            if (!_allowUndefinedBehaviour) {
                // check if shift count is negative; return ValueError
                auto negcount = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, uR, _env->i64Const(0));
                _lfb->addException(builder, ExceptionCode::VALUEERROR, negcount);
            }
            return builder.CreateShl(uL, uR);
        }

        Value *BlockGeneratorVisitor::rightShiftInst(Value *L, NBinaryOp *op, Value *R) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // only support i64, boolean
            if (!(ltype == python::Type::I64 || ltype == python::Type::BOOLEAN) ||
                !(rtype == python::Type::I64 || rtype == python::Type::BOOLEAN)) {
                return logErrorV(
                        "TypeError: unsupported operand type(s) for <<: '" + ltype.desc() + "' and '" + rtype.desc() +
                        "'");
            }

            // TODO: Consider NSW/NUW for overflow

            // upcast to 64 bits
            auto uL = upCast(builder, L, _env->i64Type());
            auto uR = upCast(builder, R, _env->i64Type());

            if (!_allowUndefinedBehaviour) {
                // check if shift count is negative; return ValueError
                auto negcount = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, uR, _env->i64Const(0));
                _lfb->addException(builder, ExceptionCode::VALUEERROR, negcount);
            }
            return builder.CreateAShr(uL, uR);
        }


        SerializableValue
        BlockGeneratorVisitor::formatStr(const tuplex::codegen::SerializableValue &fmtString, NBinaryOp *op,
                                         const tuplex::codegen::SerializableValue &arg) {

            // first, some basic checks
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            assert(op->_left->getInferredType() == python::Type::STRING);

            std::vector<llvm::Value *> argsList;
            llvm::Value *bufVar, *buf;
            llvm::Value *allocSize = fmtString.size;

            // two possibilities for right side: Either a simple value OR a tuple (for multiple values)
            // calculate allocSize and gather interpolation arguments
            if (op->_right->getInferredType().isTupleType() &&
                op->_right->getInferredType() != python::Type::EMPTYTUPLE) {
                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env, builder, arg.val,
                                                                      op->_right->getInferredType());

                // TODO: None counts as %s
                // calculate size
                for (int i = 0; i < ft.numElements(); i++) {
                    auto ptype = ft.fieldType(std::vector<int>{i});
                    if (ptype.isTupleType()) error("Can't interpolate a tuple into a string");
                    else if (ptype == python::Type::STRING) allocSize = builder.CreateAdd(ft.getSize(i), allocSize);
                    else allocSize = builder.CreateAdd(_env->i64Const(8), allocSize);
                }

                // gather arguments
                for (int i = 0; i < ft.numElements(); i++) {
                    argsList.push_back(ft.get(i));
                }
            } else {
                // simple types
                // --> i.e. perform call to snprintf (make sure to do snprintf!!!!)
                // cf. https://stackoverflow.com/questions/30234027/how-to-call-printf-in-llvm-through-the-module-builder-system
                // on how to call printf/sprintf via llvm...

                // alloc enough memory for args.
                if (arg.val->getType() == _env->i8ptrType()) {
                    // use size
                    allocSize = builder.CreateAdd(arg.size, allocSize);
                } else {
                    // use enough memory for an integer
                    // i.e. 64 bit has a range from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
                    // simply use enough space, i.e. 26 characters
                    // to be sure, use 32 per default per int
                    // if it overflows, try again

                    // rule of thumb is, allocate 8 bytes for a number
                    // will resize for larger numbers...
                    allocSize = builder.CreateAdd(_env->i64Const(8), allocSize);
                }

                // gather arguments for snprintf
                argsList.push_back(arg.val);
            }
            // allocate space
            bufVar = builder.CreateAlloca(_env->i8ptrType());
            builder.CreateStore(_env->malloc(builder, allocSize), bufVar);
            buf = builder.CreateLoad(bufVar);

            // insert standard snprintf arguments
            argsList.insert(argsList.begin(), fmtString.val);
            argsList.insert(argsList.begin(), allocSize);
            argsList.insert(argsList.begin(), buf);

            // call snprintf, if result is too small, realloc
            // cf. http://www.cplusplus.com/reference/cstdio/snprintf/
            auto charsRequired = builder.CreateCall(snprintf_prototype(_env->getContext(), _env->getModule().get()),
                                                    argsList);

            auto sizeWritten = builder.CreateAdd(builder.CreateZExt(charsRequired, _env->i64Type()), _env->i64Const(1));

            // now condition, is this greater than allocSize + 1?
            auto notEnoughSpaceCond = builder.CreateICmpSGT(sizeWritten, allocSize);

            // create two blocks
            BasicBlock *bbNormal = BasicBlock::Create(_env->getContext(), "strformat_normal",
                                                      builder.GetInsertBlock()->getParent());
            BasicBlock *bbRealloc = BasicBlock::Create(_env->getContext(), "strnormal_realloc",
                                                       builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(notEnoughSpaceCond, bbRealloc, bbNormal);

            builder.SetInsertPoint(bbRealloc);

            // realloc with sizeWritten
            // store new malloc in bufVar
            builder.CreateStore(_env->malloc(builder, sizeWritten), bufVar);
            buf = builder.CreateLoad(bufVar);
            builder.CreateCall(snprintf_prototype(_env->getContext(), _env->getModule().get()), argsList);

            builder.CreateBr(bbNormal);

            _lfb->setLastBlock(bbNormal);
            builder.SetInsertPoint(bbNormal);
            return SerializableValue(builder.CreateLoad(bufVar), sizeWritten);
        }

        llvm::Value *BlockGeneratorVisitor::numericCompareInst(llvm::IRBuilder<>& builder, llvm::Value *L, const python::Type &leftType,
                                                               const TokenType &tt, llvm::Value *R,
                                                               const python::Type &rightType) {
            assert(L);
            assert(R);
            assert(_lfb);
            auto superType = python::Type::superType(leftType, rightType);
            auto llvmSuperType = _env->pythonToLLVMType(superType);

            // upcasted left/right side
            auto uL = upCast(builder, L, llvmSuperType);
            auto uR = upCast(builder, R, llvmSuperType);

            // first do any upcasting if necessary
            auto lType = L->getType();
            auto rType = R->getType();

            bool floatType = superType == python::Type::F64;
            assert(uL->getType() == uR->getType());

            llvm::CmpInst::Predicate predicate;

            // ready to go for compare operations:
            // note that integers are always signed in python and that for the float we use the ordered compare (there is
            // also an unordered compare)
            switch (tt) {

                case TokenType::GREATER:
                    predicate = floatType ? llvm::CmpInst::Predicate::FCMP_OGT : llvm::CmpInst::Predicate::ICMP_SGT;
                    break;
                case TokenType::LESS:
                    predicate = floatType ? llvm::CmpInst::Predicate::FCMP_OLT : llvm::CmpInst::Predicate::ICMP_SLT;
                    break;

                case TokenType::GREATEREQUAL:
                    predicate = floatType ? llvm::CmpInst::Predicate::FCMP_OGE : llvm::CmpInst::Predicate::ICMP_SGE;
                    break;
                case TokenType::LESSEQUAL:
                    predicate = floatType ? llvm::CmpInst::Predicate::FCMP_OLE : llvm::CmpInst::Predicate::ICMP_SLE;
                    break;

                case TokenType::EQEQUAL:
                    predicate = floatType ? llvm::CmpInst::Predicate::FCMP_OEQ : llvm::CmpInst::Predicate::ICMP_EQ;
                    break;
                case TokenType::NOTEQUAL:
                    predicate = floatType ? llvm::CmpInst::Predicate::FCMP_ONE : llvm::CmpInst::Predicate::ICMP_NE;
                    break;

                default:
                    error(
                            "could not generate valid LLVM instruction for operator " + opToString(tt));
                    return nullptr;
            }

            // create cmp instruction
            if (floatType)
                return _env->upcastToBoolean(builder, builder.CreateFCmp(predicate, uL, uR));
            else
                return _env->upcastToBoolean(builder, builder.CreateICmp(predicate, uL, uR));
        }


        llvm::Value *BlockGeneratorVisitor::stringCompareInst(llvm::IRBuilder<>& builder, llvm::Value *L, const python::Type &leftType,
                                                              const TokenType &tt, llvm::Value *R,
                                                              const python::Type &rightType) {
            assert(L);
            assert(R);
            assert(_lfb);
            auto superType = python::Type::superType(leftType, rightType);

            if (superType != python::Type::STRING) {
                error("comp instruction for " + superType.desc() + " not yet implemented.");
                return nullptr;
            }

            switch (tt) {
                case TokenType::EQEQUAL: {
                    // need to compare using strcmp
                    FunctionType *ft = FunctionType::get(_env->i32Type(), {_env->i8ptrType(), _env->i8ptrType()},
                                                         false);
                    auto strcmp_f = _env->getModule()->getOrInsertFunction("strcmp", ft);
                    auto cmp_res = builder.CreateICmpEQ(builder.CreateCall(strcmp_f, {L, R}), _env->i32Const(0));
                    return _env->upcastToBoolean(builder, cmp_res);
                    break;
                }
                case TokenType::IN:
                case TokenType::NOTIN: {

                    // do substr search using strstr

                    // note: special case '' in 'hello' is always true!
                    auto func = ststr_prototype(_env->getContext(), _env->getModule().get());

                    // call with needle
                    assert(L->getType() == _env->i8ptrType() && R->getType() == _env->i8ptrType());
                    auto strstr_res = builder.CreateCall(func, {R, L}); // note the switch here!

                    // check if nullptr or not
                    // if nullptr then not in string!
                    auto i8nullptr = llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(_env->i8ptrType()));
                    if (tt == TokenType::IN) {
                        auto inStrCond = builder.CreateICmpNE(strstr_res, i8nullptr);
                        return _env->upcastToBoolean(builder, inStrCond);
                    } else {
                        auto notinStrCond = builder.CreateICmpEQ(strstr_res, i8nullptr);
                        return _env->upcastToBoolean(builder, notinStrCond);
                    }
                    break;
                }
                default: {
                    error("operator " + opToString(tt) + " for type string not yet implemented");
                    return nullptr;
                }
            }
        }

        llvm::Value* BlockGeneratorVisitor::listInclusionCheck(llvm::IRBuilder<>& builder, llvm::Value *L, const python::Type &leftType,
                                                       llvm::Value *R, const python::Type &rightType) {
            assert(R); assert(_lfb);
            assert(!leftType.isOptionType());
            assert(!rightType.isOptionType());
            assert(rightType.isListType());

            if(rightType == python::Type::EMPTYLIST) {
                return _env->boolConst(false);
            }

            auto elementType = rightType.elementType();

            if(elementType.withoutOptions() != leftType.withoutOptions()) {
                return _env->boolConst(false);
            }

            if(elementType.isSingleValued()) {
                return _env->upcastToBoolean(builder, builder.CreateICmpSGT(R, _env->i64Const(0)));
            } else if (elementType == python::Type::I64 || elementType == python::Type::F64 ||
                       elementType == python::Type::BOOLEAN || elementType == python::Type::STRING) {
                assert(L);
                // extract relevant pieces of list
                auto num_elements = builder.CreateExtractValue(R, 1);
                auto els_array = builder.CreateExtractValue(R, 2);

                // create blocks for loop
                auto bodyBlock = BasicBlock::Create(_env->getContext(), "listInclusion_body", builder.GetInsertBlock()->getParent());
                auto retBlock = BasicBlock::Create(_env->getContext(), "listInclusion_ret", builder.GetInsertBlock()->getParent());
                auto startBlock = builder.GetInsertBlock();
                // allocate result
                auto res = builder.CreateAlloca(_env->getBooleanType());
                builder.CreateStore(_env->boolConst(false), res);

                builder.CreateCondBr(builder.CreateICmpSGT(num_elements, _env->i64Const(0)), bodyBlock, retBlock);
                builder.SetInsertPoint(bodyBlock);
                auto loopVar = builder.CreatePHI(_env->i64Type(), 2);
                loopVar->addIncoming(_env->i64Const(0), startBlock); // start loopvar at 0

                auto el = builder.CreateLoad(builder.CreateGEP(els_array, loopVar));
                auto found = compareInst(builder, L, leftType, TokenType::EQEQUAL, el, elementType); // check for the element
                builder.CreateStore(found, res);

                auto nextLoopVar = builder.CreateAdd(loopVar, _env->i64Const(1));
                loopVar->addIncoming(nextLoopVar, builder.GetInsertBlock()); // add nextloopvar as phinode input

                auto list_bounds_check = builder.CreateICmpSLT(nextLoopVar, num_elements); // keep looping to end of list
                auto keep_looping = builder.CreateAnd(list_bounds_check, builder.CreateNot(_env->booleanToCondition(builder, found)));
                builder.CreateCondBr(keep_looping, bodyBlock, retBlock);

                builder.SetInsertPoint(retBlock);
                _lfb->setLastBlock(retBlock);
                return builder.CreateLoad(res);
            }

            assert(false);
            return nullptr;
        }

        llvm::Value *
        BlockGeneratorVisitor::compareInst(llvm::IRBuilder<>& builder, llvm::Value *L, const python::Type &leftType, const TokenType &tt,
                                           llvm::Value *R, const python::Type &rightType) {
            assert(!leftType.isOptional());
            assert(!rightType.isOptional());

            assert(L);
            assert(R);
            // comparison of values without null
            auto superType = python::Type::superType(leftType.withoutOptions(), rightType.withoutOptions());
            if (superType == python::Type::UNKNOWN) {
                std::stringstream ss;
                ss << "Could not generate comparison for types "
                   << leftType.desc()
                   << " " << opToString(tt) << " "
                   << rightType.desc();
                error(ss.str());
                // return TRUE as dummy constant to continue tracking process
                return _env->boolConst(true);
            }

            if (superType.isNumericType()) {
                return numericCompareInst(builder, L, leftType, tt, R, rightType);
            } else {
                return stringCompareInst(builder, L, leftType, tt, R, rightType);
            }
        }


        llvm::Value* BlockGeneratorVisitor::oneSidedNullComparison(llvm::IRBuilder<>& builder, const python::Type& type, const TokenType& tt, llvm::Value* isnull) {
            assert(tt == TokenType::EQEQUAL || tt == TokenType::NOTEQUAL); // only for == or !=!

            if(type == python::Type::NULLVALUE)
                return _env->boolConst(tt == TokenType::EQEQUAL); // if == then true, if != then false

            // option type? check isnull
            // else, super simple. Decide on tokentype

            if(type.isOptionType()) {
                assert(isnull);
                assert(isnull->getType() == _env->i1Type());

                // the other side is null
                // if isnull is true && equal => true
                // if isnull is false && notequal => false (case 12 != None)
                if(tt == TokenType::NOTEQUAL)
                    return _env->upcastToBoolean(builder, _env->i1neg(builder, isnull));
                else
                    return _env->upcastToBoolean(builder, isnull);
            } else {
                // the other side is null
                // => 12 != null => true
                // => 12 == null => false
                return _env->boolConst(tt == TokenType::NOTEQUAL);
            }
        }

        llvm::Value *
        BlockGeneratorVisitor::compareInst(llvm::IRBuilder<>& builder, llvm::Value *L, llvm::Value *L_isnull, const python::Type &leftType,
                                           const TokenType &tt, llvm::Value *R, llvm::Value *R_isnull,
                                           const python::Type &rightType) {

            // None comparisons only work for == or !=, i.e. for all other ops throw exception
            if (tt == TokenType::EQEQUAL || tt == TokenType::NOTEQUAL) {
                // special case: one side is None
                if(leftType == python::Type::NULLVALUE || rightType == python::Type::NULLVALUE) {

                    // left side NULL VALUE?
                    if(leftType == python::Type::NULLVALUE)
                        return oneSidedNullComparison(builder, rightType, tt, R_isnull);

                    // right side NULL VALUE?
                    if(rightType == python::Type::NULLVALUE)
                        return oneSidedNullComparison(builder, leftType, tt, L_isnull);

                    // error
                    assert(false);
                } else {
                    // non-None types involved

                    // NOTE: @TODO: I think actual if-statements are required here...

                    // special case: one is optional, the other not
                    if (leftType.isOptionType() && !rightType.isOptionType()) {
                        // ==
                        // if left is null, then for == always false, for != always true
                        // else compareInst with base type

                        assert(L_isnull);
                        assert(L);
                        assert(R);

                        auto resVal = _env->CreateTernaryLogic(builder, L_isnull, [&] (llvm::IRBuilder<>& builder) { return _env->boolConst(tt == TokenType::NOTEQUAL); },
                                                               [&] (llvm::IRBuilder<>& builder) { return compareInst(builder, L, leftType.withoutOptions(), tt, R, rightType); });
                        _lfb->setLastBlock(builder.GetInsertBlock());
                        return resVal;
                    }

                    // other way round
                    if (rightType.isOptionType() && !leftType.isOptionType()) {
                        // ==
                        // if right is null, then for == always false, for != always true
                        // else compareInst with base type

                        assert(R_isnull);
                        assert(L);
                        assert(R);

                        auto resVal = _env->CreateTernaryLogic(builder, R_isnull, [&] (llvm::IRBuilder<>& builder) { return _env->boolConst(tt == TokenType::NOTEQUAL); },
                                                               [&] (llvm::IRBuilder<>& builder) { return compareInst(builder, L, leftType, tt, R, rightType.withoutOptions()); });
                        _lfb->setLastBlock(builder.GetInsertBlock());
                        return resVal;
                    }

                    // special case: both are optional
                    if (leftType.isOptionType() && rightType.isOptionType()) {
                        assert(L);
                        assert(R);
                        assert(L_isnull);
                        assert(R_isnull);

                        // most complicated case
                        // compareInst if both are NOT none
                        auto bothValid = builder.CreateAnd(L_isnull, R_isnull);
                        auto xorResult = builder.CreateXor(L_isnull, R_isnull);
                        if (TokenType::EQEQUAL == tt)
                            xorResult = builder.CreateNot(xorResult);

                        auto resVal = _env->CreateTernaryLogic(builder, bothValid, [&] (llvm::IRBuilder<>& builder) { return compareInst(builder, L, leftType.withoutOptions(), tt, R,
                                                                                                                                         rightType.withoutOptions()); }, [&] (llvm::IRBuilder<>& builder) { return xorResult; });
                        _lfb->setLastBlock(builder.GetInsertBlock());
                        return resVal;
                    }

                    // special case none are optional
                    if (!leftType.isOptionType() && !rightType.isOptionType()) {
                        auto resVal = compareInst(builder, L, leftType, tt, R, rightType);
                        _lfb->setLastBlock(builder.GetInsertBlock());
                        return resVal;
                    }
                }

                // ERROR
                error("invalid case attained in compareInst");
                return _env->boolConst(true);
            } else if(tt == TokenType::IN && rightType.withoutOptions().isListType()) {
                // the in [list] operator has specific semantics for None on the left side
                if(R_isnull) { // R can't be null
                    _lfb->addException(builder, ExceptionCode::TYPEERROR, R_isnull);
                }

                // kind of hacky for right now, because lists don't support optional types
                if(leftType.isOptionType()) {
                    assert(L_isnull);
                    auto res = _env->CreateTernaryLogic(builder, L_isnull,
                                                           [&](llvm::IRBuilder<> &builder) {
                                                               return listInclusionCheck(builder, L,
                                                                                         python::Type::NULLVALUE, R,
                                                                                         rightType.withoutOptions());
                                                           },
                                                           [&](llvm::IRBuilder<> &builder) {
                                                               return listInclusionCheck(builder, L,
                                                                                         leftType.withoutOptions(),
                                                                                         R,
                                                                                         rightType.withoutOptions());
                                                           });
                    _lfb->setLastBlock(builder.GetInsertBlock()); // ternary logic creates blocks but can't set lfb last block
                    return res;
                }

                assert(!leftType.isOptionType());
                return listInclusionCheck(builder, L, leftType, R, rightType.withoutOptions());
            } else {
                // exception check left
                if (L_isnull) {
                    _lfb->addException(builder, ExceptionCode::TYPEERROR, L_isnull);
                }

                // exception check right
                if (R_isnull) {
                    _lfb->addException(builder, ExceptionCode::TYPEERROR, R_isnull);
                }

                auto resVal = compareInst(builder, L, leftType.withoutOptions(), tt, R, rightType.withoutOptions());
                _lfb->setLastBlock(builder.GetInsertBlock());
                return resVal;
            }
        }


        void BlockGeneratorVisitor::visit(NUnaryOp *op) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            ApatheticVisitor::visit(op);
            // restore stack
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }

            assert(_blockStack.size() >= 1);

            auto val = _blockStack.back();
            Value *V = val.val;
            _blockStack.pop_back();

            assert(V);

            Value *res = nullptr;
            switch (op->_op) {

                case TokenType::PLUS: {
                    using namespace python;

                    assert(_lfb);
                    auto builder = _lfb->getLLVMBuilder();
                    python::Type type = op->_operand->getInferredType();

                    // for boolean with unary plus, we convert it to int (true for 1 and false for 0)
                    if (python::Type::BOOLEAN == type)
                        type = python::Type::I64;

                    // create llvm value
                    auto llvmType = _env->pythonToLLVMType(type);
                    res = upCast(builder, V, llvmType);

                    break;
                }

                case TokenType::MINUS: {
                    using namespace python;

                    assert(_lfb);
                    auto builder = _lfb->getLLVMBuilder();
                    python::Type type = op->_operand->getInferredType();

                    // for boolean, we convert it to int (true for 1 and false for 0)
                    if (python::Type::BOOLEAN == type)
                        type = python::Type::I64;

                    // create negation in llvm
                    auto llvmType = _env->pythonToLLVMType(type);
                    auto uV = upCast(builder, V, llvmType);
                    if(uV->getType()->isDoubleTy()) {
                        res = builder.CreateFNeg(uV);
                    } else {
                        res = builder.CreateNeg(uV);
                    }
                    break;
                }

                case TokenType::TILDE: {
                    using namespace python;

                    // @TODO: test this here...

                    assert(_lfb);
                    auto builder = _lfb->getLLVMBuilder();
                    python::Type type = op->_operand->getInferredType();

                    if (python::Type::BOOLEAN == type) {
                        // for boolean, we convert it to int (true for 1 and false for 0)
                        type = python::Type::I64;
                        auto llvmType = _env->pythonToLLVMType(type);
                        V = upCast(builder, V, llvmType);
                    } else if (python::Type::I64 == type) {
                        // bitwise negation
                        // ok...
                    } else {
                       error("bitwise negation operator ~ only allowed for boolean or int values");
                    }

                    // perform ~, i.e. xor with -1 (two - complement)
                    res = builder.CreateXor(V, _env->i64Const(-1));
                    break;
                }

                case TokenType::NOT: {
                    // not

                    // negate truth value test of value
                    assert(_lfb);
                    auto builder = _lfb->getLLVMBuilder();
                    python::Type type = op->_operand->getInferredType();
                    auto truthResult = _env->truthValueTest(builder, val, type);
                    _lfb->setLastBlock(builder.GetInsertBlock()); // need to update b.c. truth value test produces new blocks...

                    // result is a boolean, upcast!
                    res = _env->upcastToBoolean(builder, builder.CreateNot(truthResult));
                    break;
                }

                default:
                    res = logErrorV("unknown unary operand '" + opToString(op->_op) + "' encountered");
                    break;
            }

            addInstruction(res);
        }

        llvm::Value *BlockGeneratorVisitor::powerInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R) {

            // make sure correct llvm types are used
            assert(L && (L->getType()->isIntegerTy() || L->getType()->isDoubleTy()));
            assert(R && (R->getType()->isIntegerTy() || R->getType()->isDoubleTy()));

            // check whether in dynamic mode or not.
            bool speculate = op->hasAnnotation() || op->_left->hasAnnotation() || op->_right->hasAnnotation();
            bool likelyPositiveExponent = false; // should be only accessed if speculate is true!
            if(speculate) {
                likelyPositiveExponent = op->_right->annotation().positiveValueCount >= op->_right->annotation().negativeValueCount;
            }

            // TODO:
            // boolean, integer and float case
            auto leftType = op->_left->getInferredType().withoutOptions();
            auto rightType = op->_right->getInferredType().withoutOptions();

            // assume no options.
            assert(!op->_left->getInferredType().isOptionType());
            assert(!op->_right->getInferredType().isOptionType());

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            // for speculation only interger ** integer is interesting.
            // for bool, can solve directly.
            // left ** right
            // only bool ** bool case is trivial. Other cases require speculation...
            if(leftType == python::Type::BOOLEAN) {
                // bool ** bool -> implicit cast to i64
                // bool ** int -> when speculation is active, result depends on int
                // bool ** float -> should be float??
                // --> perform null check!
                if(rightType == python::Type::BOOLEAN) {
                    // only False ** True is 0, all others are one.
                    // => simple truth test works...
                    // this is converse implication https://en.wikipedia.org/wiki/Converse_(logic)
                    // which is equivalent to
                    // a or not b
                    // not b can be easily expressed as 1 - b
                    auto bitL = builder.CreateZExtOrTrunc(L, _env->i1Type());
                    auto bitR = builder.CreateZExtOrTrunc(R, _env->i1Type());
                    auto notR = builder.CreateSub(_env->i1Const(true), bitR);
                    auto cond = builder.CreateOr(bitL, notR);
                    return builder.CreateZExtOrTrunc(cond, _env->i64Type());
                }
                // other cases are handled below, i.e. implicit cast to integer!
            }

            // general case.
            // if any of L/R is boolean, time to cast it up to full integer
            if(L->getType() == _env->getBooleanType())
                L = _env->upCast(builder, L, _env->i64Type());
            if(R->getType() == _env->getBooleanType())
                R = _env->upCast(builder, R, _env->i64Type());

            // TODO:
            // REDO after https://github.com/python/cpython/blob/442ad74fc2928b095760eb89aba93c28eab17f9b/Objects/floatobject.c#L707
            // AND https://github.com/python/cpython/blob/442ad74fc2928b095760eb89aba93c28eab17f9b/Objects/longobject.c#L4097


            // 0 raised to negative number -> zerodivisionerror
            // 0 ** 0 = 1
            //

            // explanation table for all the cases in python
            // Note: when base is not integer, but a float the result will always be a float!
            // - negative
            // + positive
            // 0 zero
            // frac -> float which is not an int
            // base | exponent | exponent has frac  | result
            // --------------------------------------
            //  -   |     -    | false              | +/- (float)
            //  -   |     +    | false              | +/-  (int)
            //  -   |     0    | false              |  1
            //  +   |     -    | false              | + (float)
            //  +   |     +    | false              | + (int)
            //  +   |     0    | false              | 1
            //  0   |     -    | false              | ZeroDivisionError (also for 0.0)
            //  0   |     +    | false              | 0
            //  0   |     0    | false              | 1
            // --------------------------------------
            //  -   |     -    | true               | complex number
            //  -   |     +    | true               | complex number
            //  +   |     -    | true               | + (float)
            //  +   |     +    | true               | + (float)

            // reordered into similar cases:
            //  -   |     -    | false              | +/- (float)
            //  +   |     -    | false              | + (float)
            //  -   |     +    | false              | +/-  (int)
            //  +   |     +    | false              | + (int)
            //  -   |     0    | false              |  1
            //  +   |     0    | false              | 1
            //  0   |     0    | false              | 1
            //  0   |     -    | false              | ZeroDivisionError (also for 0.0)
            //  0   |     +    | false              | 0 or 0.0
            // --------------------------------------
            //  -   |     -    | true               | complex number
            //  -   |     +    | true               | complex number
            //  +   |     -    | true               | + (float)
            //  +   |     +    | true               | + (float)

            // both integers?
            // if not, auto cast to float...
            if(L->getType()->isIntegerTy() && R->getType()->isIntegerTy()) {
                // integer & integer case
                // need speculation!

                // special case to optimize for:
                // -> constant integer! I.e., something like x**2
                // => generate constant int code
                if(llvm::isa<llvm::ConstantInt>(R)) {
                    // get number & perform fast multiply
                    ConstantInt *CI = llvm::dyn_cast<ConstantInt>(R);
                    auto exponent = CI->getSExtValue();
                    auto ret = generateConstantIntegerPower(builder, L, exponent);
                    return ret;
                } else {
                    // if base == 0 && exponent < 0 => ZeroDivisionError
                    auto base_is_zero_and_negative_exponent = builder.CreateAnd(builder.CreateICmpEQ(L, _env->i64Const(0)),
                                                                                     builder.CreateICmpSLT(R, _env->i64Const(0)));
                    _lfb->addException(builder, ExceptionCode::ZERODIVISIONERROR, base_is_zero_and_negative_exponent);

                    // if exponent is 0, doesn't matter -> always 1 as result.
                    // => this gets handled by the runtime function.

                    // call in speculate parts below positive integer version

                    // int64_t pow_i64(int64_t base, int64_t exp)
                    auto& ctx = _env->getContext();
                    auto func = builder.GetInsertBlock()->getParent();
                    auto mod = func->getParent();
                    llvm::FunctionType* powi64_type = llvm::FunctionType::get(_env->i64Type(), {_env->i64Type(), _env->i64Type()}, false);
#if LLVM_VERSION_MAJOR < 9
                    Function* powi64_func = cast<Function>(mod->getOrInsertFunction("pow_i64", powi64_type));
#else
                    Function* powi64_func = cast<Function>(mod->getOrInsertFunction("pow_i64", powi64_type).getCallee());
#endif

                    // speculate: I.e. is exponent more often negative or more often positive?
                    assert(speculate);
                    if(likelyPositiveExponent) {
                        // if exponent is negative -> normal case violation
                        auto exp_is_negative = builder.CreateICmpSLT(R, _env->i64Const(0));
                        _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, exp_is_negative);
                        // continue...
                        // normal case: positive or negative integer result
                        auto ret = builder.CreateCall(powi64_func, {L, R});
                        return ret;
                    } else {
                        // if exponent is non-negative -> normal case violation
                        auto exp_is_non_negative = builder.CreateICmpSGE(R, _env->i64Const(0));
                        _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, exp_is_non_negative);
                        // continue...
                        // normal case: positive or negative float result
                        auto exponent = builder.CreateNeg(R); // assumed to be negative, so use -R to get positive version.
                        llvm::Value* ret = builder.CreateCall(powi64_func, {L, exponent});
                        ret = builder.CreateSIToFP(ret, _env->doubleType());
                        return builder.CreateFDiv(_env->f64Const(1.0), ret);
                    }
                }
            } else {

                // special case: R is const integer
                if(llvm::isa<llvm::ConstantInt>(R)) {
                    // get number & perform fast multiply
                    ConstantInt *CI = llvm::dyn_cast<ConstantInt>(R);
                    auto exponent = CI->getSExtValue();
                    auto ret = generateConstantIntegerPower(builder, L, exponent);
                    return ret;
                }

                // else, use general float power function
                // simply call custom function & do err checking there...
                // cast to float!
                if(L->getType()->isIntegerTy())
                    L = builder.CreateSIToFP(L, _env->doubleType());
                if(R->getType()->isFloatTy())
                    R = builder.CreateSIToFP(R, _env->doubleType());

                auto& ctx = _env->getContext();
                auto func = builder.GetInsertBlock()->getParent();
                auto mod = func->getParent();
                llvm::FunctionType* pow_type = llvm::FunctionType::get(_env->doubleType(), {_env->doubleType(), _env->doubleType(), _env->i64ptrType()}, false);
#if LLVM_VERSION_MAJOR < 9
                        Function* pow_func = cast<Function>(mod->getOrInsertFunction("rt_py_pow", pow_type));
#else
                        Function* pow_func = cast<Function>(mod->getOrInsertFunction("rt_py_pow", pow_type).getCallee());
#endif

                // create ecCode var
                llvm::Value* pow_ec = _env->CreateFirstBlockAlloca(builder, _env->i64Type(), "pow_ec");
                builder.CreateStore(_env->i64Const(ecToI64(ExceptionCode::SUCCESS)), pow_ec);

                // call func
                auto res = builder.CreateCall(pow_func, {L, R, pow_ec});
                auto pow_ec_val = builder.CreateLoad(pow_ec);
                _lfb->addException(builder, pow_ec_val, builder.CreateICmpNE(pow_ec_val, _env->i64Const(ecToI64(ExceptionCode::SUCCESS))));
                return res;
            }

            // for mod version: confer
            // https://stackoverflow.com/questions/5246856/how-did-python-implement-the-built-in-function-pow

            fatal_error("invalid controlfow in power operator taken");
            return nullptr;
        }

        void BlockGeneratorVisitor::visit(NBinaryOp *op) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            SerializableValue res;
            // have to handle logical instructions separately for short-circuiting
            if(op->_op == TokenType::AND) {
                res = logicalAndInst(op);
            } else if(op->_op == TokenType::OR) {
                res = logicalOrInst(op);
            } else {
                ApatheticVisitor::visit(op);

                // error happened? stop, and restore stack
                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return; // expression is done (and so is the statement where it has been used).
                }

                // pop two vals from the stack incl. nullcheck
                // ==> binary operations are not defined over None! (==/!= are in compare)
                assert(_lfb);
                auto builder = _lfb->getLLVMBuilder();
                auto SerialR = popWithNullCheck(builder, ExceptionCode::TYPEERROR,
                                                "unsupported right operand type NoneType");
                auto SerialL = popWithNullCheck(builder, ExceptionCode::TYPEERROR,
                                                "unsupported left operand type NoneType");

                Value *R = SerialR.val;
                Value *L = SerialL.val;
                assert(R);
                assert(L);

                switch (op->_op) {
                    // plus
                    case TokenType::PLUS: {

                        res = additionInst(SerialL, op, SerialR);
                        break;
                    }

                    case TokenType::MINUS: {

                        res.val = subtractionInst(L, op, R);
                        break;
                    }

                        // multiplication
                    case TokenType::STAR: {

                        res = multiplicationInst(SerialL, op, SerialR);
                        break;
                    }

                    case TokenType::SLASH: {
                        // this may raise a divide by zero exception, i.e. add with exeception handler...
                        res.val = divisionInst(L, op, R);
                        break;
                    }

                    case TokenType::DOUBLESLASH: {
                        // this may raise a divide by zero exception, i.e. add with exeception handler...
                        res.val = integerDivisionInst(L, op, R);
                        break;
                    }

                    case TokenType::PERCENT: {
                        // operator has depending on type two meanings:
                        // either modulo for numbers OR format for strings
                        if (op->_left->getInferredType() == python::Type::STRING)
                            res = formatStr(SerialL, op, SerialR);
                        else
                            // modulo operation (handles errors too)
                            res.val = moduloInst(L, op, R);
                        break;
                    }

                    case TokenType::LEFTSHIFT: {

                        res.val = leftShiftInst(L, op, R);
                        break;
                    }

                    case TokenType::RIGHTSHIFT: {

                        res.val = rightShiftInst(L, op, R);
                        break;
                    }

                    // binary instructions &, |, ^
                    case TokenType::AMPER:
                    case TokenType::VBAR:
                    case TokenType::CIRCUMFLEX: {
                        res.val = binaryInst(L, op, R);
                        break;
                    }

                    case TokenType::DOUBLESTAR: {
                        res.val = powerInst(L, op, R);
                        break;
                    }

                    default:
                        res.val = logErrorV("unknown binary operand '" + opToString(op->_op) + "' encountered");
                        break;
                }
            }

            addInstruction(res.val, res.size);
        }

        BlockGeneratorVisitor::Variable::Variable(LLVMEnvironment &env, llvm::IRBuilder<> &builder,
                                                  const python::Type &t, const std::string &name) {
            // map type to LLVM
            // allocate variable in first block! (important because of loops!)
            // get rid off option!

            // only string, bool, int, f64 so far supported!
            ptr = env.CreateFirstBlockAlloca(builder, env.pythonToLLVMType(t.isOptionType() ? t.getReturnType() : t), name);
            // alloc size
            sizePtr = env.CreateFirstBlockAlloca(builder, env.i64Type(), name + "_size");

            // option type? then alloc isnull!
            nullPtr = t.isOptionType() ? env.CreateFirstBlockAlloca(builder, env.i1Type()) : nullptr;

            this->name = name;
        }

        void BlockGeneratorVisitor::declareVariables(ASTNode* func) {
            using namespace std;

            auto var_info = getDeclaredVariables(func);
            _variableSlots.clear();

            auto builder = _lfb->getLLVMBuilder();

            // retrieve parameters and types
            vector<tuple<string, python::Type>> paramInfo;
            auto paramNames = getFunctionParameters(func);
            for(unsigned i = 0; i < paramNames.size(); ++i) {
                auto name = paramNames[i];
                python::Type type;
                if(func->type() == ASTNodeType::Function) {
                    assert(((NFunction*)func)->_parameters->_args.size() == paramNames.size());
                    // BUG:
                    // // for some reason the args get somewhere overwritten. This is a bug. no idea where
                    // // fix when there's time.
                    // auto type = func->_parameters->_args[i]->getInferredType(); // holds wrong type??

                    // func stores the correct type!
                    type = func->getInferredType().getParamsType().parameters()[i];
                } else {
                    assert(func->type() == ASTNodeType::Lambda);
                    assert(((NLambda*)func)->_arguments->_args.size() == paramNames.size());
                    type = func->getInferredType().getParamsType().parameters()[i];
                }

                paramInfo.emplace_back(make_tuple(name, type));
            }

            // add parameters
            for(unsigned i = 0; i < paramInfo.size(); ++i) {
                auto name = std::get<0>(paramInfo[i]);
                auto type = std::get<1>(paramInfo[i]);
                auto param = _lfb->getParameter(name);

                VariableSlot slot;
                slot.type = type;
                slot.definedPtr = _env->CreateFirstBlockAlloca(builder, _env->i1Type(), name + "_defined");
                assert(slot.definedPtr);
                builder.CreateStore(_env->i1Const(true), slot.definedPtr); // params are always defined!!!
                slot.var = Variable(*_env, builder, type, name);

                // store param into var
                slot.var.store(builder, param);
                _variableSlots[name] = slot;
            }

            // for each other var found, declare a slot now.
            // if this fails, then print detailed error message!
            std::stringstream unify_error_stream;
            for(auto& keyval : var_info) {

                // exists already as parameter? => skip!
                if(_variableSlots.find(keyval.first) != _variableSlots.end())
                    continue;

                if(keyval.second.size() != 1) {
                    assert(keyval.second.size() > 1); // can't be empty!
                    keyval.second = std::vector<python::Type>{python::Type::UNKNOWN}; // save result
                }

                // add var to lookup but mark as undefined yet.
                if(keyval.second.front() != python::Type::UNKNOWN) {
                    VariableSlot slot;
                    slot.type = keyval.second.front();
                    slot.definedPtr = _env->CreateFirstBlockAlloca(builder, _env->i1Type(), keyval.first + "_defined");
                    builder.CreateStore(_env->i1Const(false), slot.definedPtr);
                    slot.var = Variable(*_env, builder, keyval.second.front(), keyval.first);
                    _variableSlots[keyval.first] = slot;
                } else {
                    // this is a variable which has multiple types assigned to names.
                    VariableSlot slot;
                    slot.type = python::Type::UNKNOWN;
                    slot.definedPtr = _env->CreateFirstBlockAlloca(builder, _env->i1Type(), keyval.first + "_defined");
                    builder.CreateStore(_env->i1Const(false), slot.definedPtr);
                    slot.var = Variable();
                    _variableSlots[keyval.first] = slot;
                }
            }
        }

        void BlockGeneratorVisitor::visit(NFunction *func) {
            // clear slots
            // @TODO: nested functions?
            _variableSlots.clear();

            assert(_env);
            assert(func);
            assert(func->_name);
            assert(func->_suite);
            auto &context = _env->getContext();

            std::string twine = "rettuple"; // name for the structtypes used as return values
            std::string func_name = func->_name->_name;

            if (_lfb)
                delete _lfb;
            _lfb = nullptr;

            _lfb = new LambdaFunctionBuilder(_logger, _env);
            _lfb->create(func);
            _funcNames.push(_lfb->funcName());

            // i.e. for each variable, we need to
            // define a slot, incl. a helper variable isDefined which is an i1 so we
            // can throw UnboundLocalErrors
            declareVariables(func);

            // after variables are declared upfront, start code generation.
            // go into suite
            func->_suite->accept(*this);

            // make sure that lastBlock is nullptr
            if (_lfb->getLastBlock()) {
                delete _lfb;
                _lfb = nullptr;
                error("missing return statement");
            }

            delete _lfb;
            _lfb = nullptr;
        }

//#error "maybe use setjmp/lngjmp to get out of generating an expression, resetting everything? => might be the right decision..."
//"Need to check that stuff.... Make a dummy example to check that behavior in BlockGeneratorVisitor.cc"

        void BlockGeneratorVisitor::assignToSingleVariable(NIdentifier *target, const python::Type& valueType) {
            auto builder = _lfb->getLLVMBuilder();
            // pop from stack & store in var
            auto val = _blockStack.back();
            _blockStack.pop_back();

            // check var stack on whether there's a slot
            auto slot = getSlot(target->_name);
            // exists? => reassign!
            if(slot) {

                // mark as defined
                builder.CreateStore(_env->i1Const(true), slot->definedPtr);

                // now check whether type compatible, if so assign.
                // if not, simply alloc a new variable which takes the place.

                auto targetType = target->getInferredType();

                if(targetType.isIteratorType()) {
                    updateIteratorVariableSlot(builder, slot, val, targetType, target->annotation().iteratorInfo);
                    return;
                }

                if(targetType != slot->type || !slot->var.ptr) {
                    // LLVM endlifetime for this variable here, this hint helps the optimizer.
                    //slot->var.endLife(builder);

                    // allocate new pointer for this var, overwrite type
                    slot->type = targetType;
                    slot->var = Variable(*_env, builder, targetType, target->_name);
                } else {
                    // compatible, so simply assign.
                    // nothing todo here, done below.
                }

                // note: assigning bool, float, i64 to the same name
                auto casted_val = upCastReturnType(builder, val, valueType, slot->type);

                // load to variable
                slot->var.store(builder, casted_val);
            } else {
                error("internal compiler error, i.e. variables not initialized properly!");
            }
        }

        void BlockGeneratorVisitor::assignToMultipleVariables(NTuple *lhs, ASTNode *rhs) {
            using namespace std;

            auto builder = _lfb->getLLVMBuilder();

            // type check the rhs
            // cannot assign tuple to something other than id, string, or tuple
            switch (rhs->type()) {
                case ASTNodeType::Identifier:
                case ASTNodeType::String:
                case ASTNodeType::Tuple:
                    break;
                default:
                    error("invalid tuple assignment, can only assign from identifiers, strings or a tuple");
            }

#ifndef NDEBUG
            // validate tuple is of identifiers
            for(auto el : lhs->_elements)
                if(el->type() != ASTNodeType::Identifier)
                    error("invalid AST tree, left hand side of assign expected to be tuple of identifiers!");
#endif
            // for Analysis following info about names can be extracted
            // // first tricky part is, because python allows things like
            // // a, b = b, a
            // // to snapshot variables from the left side before reassigning to the right side
            // set<string> lhs_names;
            // for(const auto& el : lhs->_elements) {
            //     assert(el->type() == ASTNodeType::Identifier);
            //     auto id = (NIdentifier*)el;
            //     lhs_names.insert(id->_name);
            // }
            // // using visitor, find out which names are used on the right side
            // auto rhs_varinfo = getDeclaredVariables(rhs);
            // set<string> rhs_names;
            // for(auto info: rhs_varinfo)
            //     rhs_names.insert(info.first);
            // vector<string> names_to_snapshot;
            // for(auto name: lhs_names) {
            //     if(rhs_names.find(name) != rhs_names.end())
            //         names_to_snapshot.push_back(name);
            // }
            // // snapshot these variables
            // unordered_map<string, VariableRealization> snapshots;
            // for(auto name : names_to_snapshot) {
            //     auto slot = getSlot(name); assert(slot);
            //     snapshots[name] = VariableRealization::fromSlot(builder, name, *slot);
            // }

            // access RHS block from the block stack
            assert(!_blockStack.empty());
            auto rhs_block = _blockStack.back();

            FlattenedTuple ft(_env);

            // do another type check
            // check if it's an LLVM string or tuple
            auto inferredType = rhs->getInferredType();
            if (inferredType.isTupleType()) {
                ft = FlattenedTuple::fromLLVMStructVal(_env, builder, rhs_block.val, rhs->getInferredType());
            } else if (inferredType == python::Type::STRING) {
                // check length
                auto rhs_len = builder.CreateSub(rhs_block.size, _env->i64Const(1));
                auto size_not_equal = builder.CreateICmpNE(_env->i64Const(lhs->_elements.size()), rhs_len);

                _lfb->addException(builder, ExceptionCode::VALUEERROR, size_not_equal);
            } else {
                error("assigning tuple to invalid value");
            }

            // assign now to multiple targets...

            // note: very important to make a copy?? i.e. swap problem?
            for (auto i = 0; i < lhs->_elements.size(); i++) {

                // TODO: add ASTNodeType::Subscription?
                // i.e. for lists and dicts this is valid!
                if (lhs->_elements[i]->type() != ASTNodeType::Identifier) {
                    error("LHS is not a tuple of identifiers");
                }

                // fetch value & its type
                // assign LHS[i] = copy(rhs[i])
                SerializableValue val;
                python::Type valueType;
                if (inferredType.isTupleType()) {
                    val = ft.getLoad(builder, {i});
                    valueType = inferredType.parameters()[i];
                } else if (inferredType == python::Type::STRING) {
                    // index into string
                    auto rhs_char = _env->malloc(builder, _env->i64Const(2));
                    builder.CreateStore(builder.CreateLoad(builder.CreateGEP(rhs_block.val, _env->i64Const(i))), rhs_char);
                    builder.CreateStore(_env->i8Const(0), builder.CreateGEP(rhs_char, _env->i64Const(1)));
                    val = SerializableValue(rhs_char, _env->i64Const(2));
                    valueType = python::Type::STRING;
                } else {
                    throw std::runtime_error("unknown lhs type " + inferredType.desc() + " in multi assign!");
                }


                // find variable to assign to
                auto target = (NIdentifier *) lhs->_elements[i];

                auto name = target->_name;
                auto slot = getSlot(name); assert(slot);

                // exists? => reassign!
                if(slot) {

                    // mark as defined
                    builder.CreateStore(_env->i1Const(true), slot->definedPtr);

                    // now check whether type compatible, if so assign.
                    // if not, simply alloc a new variable which takes the place.

                    auto targetType = target->getInferredType();

                    if(targetType != slot->type || !slot->var.ptr) {
                        // LLVM endlifetime for this variable here, this hint helps the optimizer.
                        //slot->var.endLife(builder);

                        // allocate new pointer for this var, overwrite type
                        slot->type = targetType;
                        slot->var = Variable(*_env, builder, targetType, target->_name);
                    } else {
                        // compatible, so simply assign.
                        // nothing todo here, done below.
                    }

                    // note: assigning bool, float, i64 to the same name
                    auto casted_val = upCastReturnType(builder, val, valueType, slot->type);

                    // load to variable
                    slot->var.store(builder, casted_val);
                } else {
                    error("internal compiler error, i.e. variables not initialized properly!");
                }
            }
        }

        void BlockGeneratorVisitor::visit(NAssign *assign) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            if(assign->_target->type() != ASTNodeType::Identifier &&
                   assign->_target->type() != ASTNodeType::Tuple)
                error("only tuples/identifiers are acceptable assignment targets");

            // case I: assigning to a single identifier, i.e. x = ...
            if (assign->_target->type() == ASTNodeType::Identifier) {
                auto id = (NIdentifier *) assign->_target;
                // simple:
                // --> visit target to push onto stack
                // (do not visit _target!)
                assign->_value->accept(*this);

                // restore stack
                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return;
                }

                assert(_blockStack.size() >= 1); // at least one element needs to be there

                assignToSingleVariable(id, assign->_value->getInferredType());
            } else if(assign->_target->type() == ASTNodeType::Tuple) {
                // case II: assigning to a tuple of identifiers
                auto lhs = (NTuple*)assign->_target;
                auto rhs = assign->_value;
                // visit the RHS to push everything on the stack
                assign->_value->accept(*this);

                // restore stack
                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return;
                }

                assert(_blockStack.size() >= 1); // at least one element needs to be there

                // assign procedure.
                assignToMultipleVariables(lhs, rhs);

                // pop RHS off the stack
                _blockStack.pop_back();
            } else {
                error("internal compile error, assignment allow for target only Identifier or Tuple of identifier nodes!");
            }
        }

        // speculative ifelse_expression
        void BlockGeneratorVisitor::generatePartialIfElseExpression(NIfElse *ifelse, bool visit_if, bool short_circuit) {
            // which branch to visit?
            assert(ifelse->_then && ifelse->_else); // it's an expression, so both are valid!

            // this here is for codegen, when speculation on branch is true
            auto num_stack_before = _blockStack.size();
            // -----
            // step 1: generate code for condition (can be in the current basic block)
            // check if it is only a single IF block or whether there is also an else statement
            ifelse->_expression->accept(*this);

            // error happened within expression?
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // statement using this expression done.
            }

            // block stack needs to have condition so far
            assert(_blockStack.size() >= 1);

            // get condition
            auto cond = _blockStack.back();
            _blockStack.pop_back();
            auto builder = _lfb->getLLVMBuilder();
            auto parentFunc = builder.GetInsertBlock()->getParent();

            // convert condition value to i1 value according to python3 truth testing rules!
            auto ifcond = _env->truthValueTest(builder, cond, ifelse->_expression->getInferredType());
            llvm::BasicBlock *entryBB = builder.GetInsertBlock();

            // now evaluate expression in branches
            if(visit_if) {
                // visit only if
                auto iftype_py = ifelse->_then->getInferredType();
                if(iftype_py.isTupleType())
                    error("tuple type as result of if-else expression not yet supported.");

                // create exception when condition does not hold
                _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, _env->i1neg(builder, ifcond));

                // emit code for if block only
                // in case of expression that's super simple
                ifelse->_then->accept(*this);
                assert(_blockStack.size() > 0);
                auto res = _blockStack.back();
                _blockStack.pop_back();

                if(short_circuit) {
                    // need to convert to Boolean
                    res.val = _env->upcastToBoolean(builder, _env->truthValueTest(builder, res, iftype_py));
                    res.size = nullptr;
                    res.is_null = nullptr;
                    _lfb->setLastBlock(
                            builder.GetInsertBlock()); // need to update b.c. truth value test produces new blocks...
                }

                // push onto stack
                _blockStack.push_back(res);
            } else {
                // visit only else
                auto elsetype_py = ifelse->_else->getInferredType();
                if(elsetype_py.isTupleType())
                    error("tuple type as result of if-else expression not yet supported.");

                // create exception when condition does hold
                _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, ifcond);

                // now emit code for else block only
                // in case of expression that's super simple
                ifelse->_else->accept(*this);
                assert(_blockStack.size() > 0);
                auto res = _blockStack.back();
                _blockStack.pop_back();

                if(short_circuit) {
                    // need to convert to Boolean
                    res.val = _env->upcastToBoolean(builder, _env->truthValueTest(builder, res, elsetype_py));
                    res.size = nullptr;
                    res.is_null = nullptr;
                    _lfb->setLastBlock(
                            builder.GetInsertBlock()); // need to update b.c. truth value test produces new blocks...
                }

                // push onto stack
                _blockStack.push_back(res);
            }
        }

        void BlockGeneratorVisitor::generateIfElseExpression(NIfElse *ifelse, bool short_circuit) {
            assert(ifelse->isExpression());

            // which to visit when speculation is used?
            auto visit_t = whichBranchToVisit(ifelse);
            auto visit_ifelse = std::get<0>(visit_t);
            auto visit_if = std::get<1>(visit_t);
            auto visit_else = std::get<2>(visit_t);

            // speculative processing on this ifelse?
            bool speculate = ifelse->annotation().numTimesVisited > 0;
            // only one should be true, logical xor
            if(speculate && (!visit_if != !visit_else)) {
                generatePartialIfElseExpression(ifelse, visit_if, short_circuit);
                return;
            }

            // Note: unfortunately, python uses short circuit evaluation for if-else expressions. hence, select statement
            // can't be used here.

            // there need to be two expressions here
            assert(ifelse->_then && ifelse->_else);

            auto num_stack_before = _blockStack.size();

            // step 1: generate code for condition (can be in the current basic block)
            // check if it is only a single IF block or whether there is also an else statement
            ifelse->_expression->accept(*this);

            // error happened within expression?
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // statement using this expression done.
            }

            // block stack needs to have condition so far
            assert(_blockStack.size() >= 1);

            // get condition
            auto cond = _blockStack.back();
            _blockStack.pop_back();

            auto builder = _lfb->getLLVMBuilder();
            auto parentFunc = builder.GetInsertBlock()->getParent();

            // convert condition value to i1 value according to python3 truth testing rules!
            auto ifcond = _env->truthValueTest(builder, cond, ifelse->_expression->getInferredType());

            // debug
            // _env->debugPrint(builder, "ifcond value: ", ifcond);

            llvm::BasicBlock *entryBB = builder.GetInsertBlock();
            // Now, create two blocks: one for if part, one for else part. Each gets a variable to store the result!
            // ==> why? because of short circuit evaluation.

            // Combine variables for e.g. option types...

            // Note: combining also necessary for if/else for def'ed functions, return paths! oO ==> what about nested? more difficult...

            // need to add two variables, for each if and else block.


            auto iftype_py = ifelse->_then->getInferredType();
            auto elsetype_py = ifelse->_else->getInferredType();

            // create combined type & alloc var!
            auto restype_py = ifelse->getInferredType();
            auto restype_llvm = _env->pythonToLLVMType(
                    restype_py.isOptionType() ? restype_py.getReturnType() : restype_py);

            if (restype_py.isTupleType())
                error("tuple type as result of if-else expression not yet supported.");

            // Note: variable alloc should go into constructor block!
            // create alloca for result variable
            auto result_var = builder.CreateAlloca(restype_llvm, 0, nullptr);
            auto result_size = builder.CreateAlloca(_env->i64Type(), 0, nullptr);
            auto result_isnull = builder.CreateAlloca(_env->i1Type(), 0, nullptr);
            builder.CreateStore(_env->i1Const(false), result_isnull); // per default set it as valid!
            builder.CreateStore(_env->i64Const(0), result_size); // store dummy val of 0 in it.

            // @TODO: nested if-else statements...
#warning "nested if else expressions? they might break this..."

            // create if block
            llvm::BasicBlock *ifBB = BasicBlock::Create(_env->getContext(), "if", parentFunc);
            llvm::BasicBlock *elseBB = BasicBlock::Create(_env->getContext(), "else", parentFunc);
            llvm::BasicBlock *exitBB = BasicBlock::Create(_env->getContext(), "ifelse_exit", parentFunc);

            // condition & branch
            builder.CreateCondBr(ifcond, ifBB, elseBB);

            // Note: we could optimize all of this by cleaning the AST first and getting rid off the unneeded basic blocks...

            bool skip_if_result = false;
            bool skip_else_result = false;

            // visit if
            _lfb->setLastBlock(ifBB);
            ifelse->_then->accept(*this);

             // error happened in if-expression part?
             if(earlyExit())
                 skip_if_result = true;
                //error("exit path in ifelse expression (if-branch) not yet implemented.");

            if(!skip_if_result)
                assert(!_blockStack.empty());
            // pop from stack & process value result.
            SerializableValue ifval, elseval;
            if(!skip_if_result) {
                // store result to var
                builder.SetInsertPoint(_lfb->getLastBlock());

                ifval = _blockStack.back();
                _blockStack.pop_back();

                // if-else result type might be different from if-type
                // --> upcast if necessary
                if(short_circuit) {
                    // need to convert to Boolean
                    ifval.val = _env->upcastToBoolean(builder, _env->truthValueTest(builder, ifval, iftype_py));
                    ifval.size = nullptr;
                    ifval.is_null = nullptr;
                    _lfb->setLastBlock(builder.GetInsertBlock()); // need to update b.c. truth value test produces new blocks...
                } else {
                    ifval = upCastReturnType(builder, ifval, iftype_py, restype_py);
                }

                if (ifval.val)
                    builder.CreateStore(ifval.val, result_var);
                if (ifval.size)
                    builder.CreateStore(ifval.size, result_size);
                builder.CreateStore(ifval.is_null ? ifval.is_null : _env->i1Const(false), result_isnull);
                builder.CreateBr(exitBB);
            }

            // else block
            // visit else
            _lfb->setLastBlock(elseBB);
            ifelse->_else->accept(*this);

             if(earlyExit())
                 skip_else_result = true;

             if(!skip_else_result) {
                 assert(!_blockStack.empty());

                 // store result to var
                 builder.SetInsertPoint(_lfb->getLastBlock());
                 // pop from stack
                 elseval = _blockStack.back();
                 _blockStack.pop_back();

                 // if-else result type might be different from if-type
                 // --> upcast if necessary
                 if(short_circuit) {
                     // need to convert to Boolean
                     elseval.val = _env->upcastToBoolean(builder, _env->truthValueTest(builder, elseval, elsetype_py));
                     elseval.size = nullptr;
                     elseval.is_null = nullptr;
                     _lfb->setLastBlock(builder.GetInsertBlock()); // need to update b.c. truth value test produces new blocks...
                 } else {
                     elseval = upCastReturnType(builder, elseval, elsetype_py, restype_py);
                 }

                 if (elseval.val)
                     builder.CreateStore(elseval.val, result_var);
                 if (elseval.size)
                     builder.CreateStore(elseval.size, result_size);
                 builder.CreateStore(elseval.is_null ? elseval.is_null : _env->i1Const(false), result_isnull);
                 builder.CreateBr(exitBB);
             }

             // make sure skip_if and skip_else are not both true
             if(skip_if_result && skip_else_result) {
                 fatal_error("internak error: something went wrong, both values of if expression skipped. can't be");
             }

            _lfb->setLastBlock(exitBB);
            builder.SetInsertPoint(exitBB);
            // push result to stack
            codegen::SerializableValue result(builder.CreateLoad(result_var),
                                              builder.CreateLoad(result_size),
                                              builder.CreateLoad(result_isnull));

            _blockStack.push_back(result);
        }

        void BlockGeneratorVisitor::generateIfElseStatement(NIfElse *ifelse,
                bool exceptOnThen,
                bool exceptOnElse) {
            assert(!ifelse->isExpression());

            // save stack info
            auto num_stack_before = _blockStack.size();

            // check if it is only a single IF block or whether there is also an else statement
            ifelse->_expression->accept(*this);

            // was an early exit (i.e. an always excepting code path) generated?
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }

            assert(ifelse->_then);
            // block stack needs to have condition so far
            assert(_blockStack.size() >= 1);

            // get condition
            auto cond = _blockStack.back();
            _blockStack.pop_back();

            auto builder = _lfb->getLLVMBuilder();
            auto parentFunc = builder.GetInsertBlock()->getParent();

            // because this is a statement, need to capture all sorts of variable redefinitions!
            // snapshot all variables, accessed within if-block
            // -> for now, due to simplicity reasons, simply load everything.
            auto var_realizations = snapshotVariableValues(builder);
            auto slots_backup = _variableSlots;

            // realizations of variables after EACH block is processed from the statement.
            std::unordered_map<std::string, VariableRealization> if_var_realizations;
            std::unordered_map<std::string, VariableRealization> else_var_realizations;

            // convert condition value to i1 value according to python3 truth testing rules!
            auto ifcond = _env->truthValueTest(builder, cond, ifelse->_expression->getInferredType());

            // special case: exceptions present
            if(exceptOnThen) {

                // else exists?
                if(ifelse->_else) {
                    // leave function when condition is true, else exec else branch!
                    _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, ifcond);
                    builder.SetInsertPoint(_lfb->getLastBlock());

                    // generate if block code (regular)
                    ifelse->_else->accept(*this);
                } else {
                    // condition is generated
                    // i.e. logic is
                    // if (cond)
                    //      throw non-normal-case
                    // ... # continue with other code
                    _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, ifcond);
                }

            } else if(exceptOnElse) {
                // check with negated ifcond for exception, then continue generation
                auto neg_ifcond = _env->i1neg(builder, ifcond);
                _lfb->addException(builder, ExceptionCode::NORMALCASEVIOLATION, neg_ifcond);
                builder.SetInsertPoint(_lfb->getLastBlock());

                // generate if block code (regular)
                ifelse->_then->accept(*this);
            } else {
                assert(!exceptOnElse && !exceptOnThen); // should be both false, regular case
                // this also means, we can unify numeric types!
                // -> use unify_tupes(a, b, true).

                // !!! entry Block is the one AFTER the condition, else it will be screwed up
                llvm::BasicBlock *entryBB = builder.GetInsertBlock();
                llvm::BasicBlock *ifBB = BasicBlock::Create(_env->getContext(), "If", parentFunc);
                llvm::BasicBlock *elseBB = ifelse->_else ? BasicBlock::Create(_env->getContext(), "Else", parentFunc)
                                                         : nullptr;
                llvm::BasicBlock *exitBB = nullptr;

                // for connection use lastBlocks
                auto lastIfBB = ifBB;
                auto lastElseBB = elseBB;

                // TODO: in order to optimize, simply check which slots can be reused, and which need to be
                // overwritten when unifying types at the end.

                // create BasicBlock for if
                // via block generator visitor
                _lfb->setLastBlock(ifBB);

                ifelse->_then->accept(*this);

                // important --> get last block (recursive if!!!)
                // lastblock may be nullptr if return was used!
                // -> this could be also the case if an early-exit was emitted.
                lastIfBB = _lfb->getLastBlock();

                // lastIfBB escape? => return!
                if(lastIfBB) {
                    auto if_builder = llvm::IRBuilder<>(lastIfBB);
                    // variables are overwritten with whatever has been generated in if block.
                    // => get realizations, then reset vars to state before entering if-stmt!
                    if(blockOpen(lastIfBB)) // do not snapshot when exit path
                        if_var_realizations = snapshotVariableValues(if_builder);
                }

                // create BasicBlock for else
                if (elseBB) {
                    _lfb->setLastBlock(elseBB);
                    auto else_builder = _lfb->getLLVMBuilder();
                    // restore all variables, based on previous realizations.
                    restoreVariableSlots(else_builder, var_realizations);
                    ifelse->_else->accept(*this);
                    // check if early return...
                    lastElseBB = _lfb->getLastBlock(); // lastblock may be nullptr if return was used!
                    if(blockOpen(lastElseBB)) {
                        else_builder.SetInsertPoint(lastElseBB);
                        else_var_realizations = snapshotVariableValues(else_builder);
                    }
                }

                // in the created if/else blocks attempt variable unification!
                // @TODO: single if branch with conflicts?
                // ---------------------------------------------------------------------------------
                // unification of variables... slow and bad...

                bool allowNumericUpcasting = true;

                // restore slots to how the slots were initially, i.e. not from realizations but rather just the pointers!
                _variableSlots = slots_backup;

                // note, if BOTH branches redefine a variable under a different type,
                // then that slot should get that redefined type!

                // if some slot gets overriden by both, update to their shared type!
                // this allows for code like this
                // x = 20
                // if x > 20:
                //    x = 'hello'
                // else:
                //    x = 'test'
                // important to call this before the individual update functions!
                updateSlotsWithSharedTypes(builder, if_var_realizations, else_var_realizations);

                // update with variables occuring in each branch
                updateSlotsBasedOnRealizations(builder, if_var_realizations, "if-branch", allowNumericUpcasting);
                updateSlotsBasedOnRealizations(builder, else_var_realizations, "else-branch", allowNumericUpcasting);

                // go through realizations and update change slots with values if necessary!
                // else, the pointer was reused during generation, so the entry in the slot was not updated to a new variable.
                // also, update with define ptr here!
                if (blockOpen(lastIfBB)) {
                    for(const auto& if_var : if_var_realizations) {

                        llvm::IRBuilder<> bIf(lastIfBB);
                        auto name = if_var.first;

                        // updated slot? then store!
                        auto slot = getSlot(name); assert(slot);

                        // other pointer than before?
                        if(slot->var.sizePtr != if_var_realizations[name].original_ptr.size ||
                        slot->var.ptr != if_var_realizations[name].original_ptr.val ||
                        slot->var.nullPtr != if_var_realizations[name].original_ptr.is_null) {
                            // perform upcasting operations
                            auto upcast_if = upCastReturnType(bIf, if_var.second.val,
                                                              if_var.second.type, slot->type);

                            slot->var.store(bIf, upcast_if);
                        }

                        // store definedness in variable for unbound-local error!
                        // @TODO: do this only for accessed variables within that if...
                        // else it's a waste of instructions!
                        bIf.CreateStore(_env->i1Const(true), slot->definedPtr);

                        lastIfBB = bIf.GetInsertBlock();
                    }
                }

                if (ifelse->_else && blockOpen(lastElseBB)) {
                    for(const auto& else_var : else_var_realizations) {

                        llvm::IRBuilder<> bElse(lastElseBB);
                        auto name = else_var.first;

                        // updated slot? then store!
                        auto slot = getSlot(name); assert(slot);

                        // other pointer than before?
                        if(slot->var.sizePtr != else_var_realizations[name].original_ptr.size ||
                           slot->var.ptr != else_var_realizations[name].original_ptr.val ||
                           slot->var.nullPtr != else_var_realizations[name].original_ptr.is_null) {
                            // perform upcasting operations
                            auto upcast_else = upCastReturnType(bElse, else_var.second.val,
                                                              else_var.second.type, slot->type);

                            slot->var.store(bElse, upcast_else);
                        }

                        // store definedness in variable for unbound-local error!
                        // @TODO: Do this only for variables accessed within that else branch!!!
                        bElse.CreateStore(_env->i1Const(true), slot->definedPtr);

                        lastElseBB = bElse.GetInsertBlock();
                    }
                }

                // special case: single if, it could happen that there's a conflict from the types defined in
                // the if statement with the previous variables.
                // Hence, simply add store instructions to the new slot BEFORE the if statement is entered.
                if(!ifelse->_else) {
                    // go through the previous var realizations...
                    for(const auto& prev_var : var_realizations) {

                        llvm::IRBuilder<> bBeforeIf(entryBB);
                        auto name = prev_var.first;

                        // updated slot? then store!
                        auto slot = getSlot(name); assert(slot);

                        // other pointer than before?
                        if(slot->var.sizePtr != var_realizations[name].original_ptr.size ||
                           slot->var.ptr != var_realizations[name].original_ptr.val ||
                           slot->var.nullPtr != var_realizations[name].original_ptr.is_null) {
                            // perform upcasting operations
                            auto upcast_var = upCastReturnType(bBeforeIf, prev_var.second.val,
                                                              prev_var.second.type, slot->type);

                            slot->var.store(bBeforeIf, upcast_var);
                        }

                        // store definedness in variable for unbound-local error!
                        // @TODO: This could be optimized away when using static bools instead of runtime booleans
                        // for the easier cases...
                        // else it's a waste of instructions!
                        assert(slot->definedPtr);
                        bBeforeIf.CreateStore(_env->i1Const(true), slot->definedPtr);

                        entryBB = bBeforeIf.GetInsertBlock();
                    }
                }

                // ---------------------------------------------------------------------------------

                // connect blocks via condition
                builder.SetInsertPoint(entryBB);
                if (elseBB) {
                    builder.CreateCondBr(ifcond, ifBB, elseBB);
                    // check if there was a return OR not
                    if (blockOpen(lastIfBB) || blockOpen(lastElseBB)) {
                        // need to create special block
                        exitBB = BasicBlock::Create(_env->getContext(), "exitIf", parentFunc);
                        // connect to this block
                        if (blockOpen(lastIfBB)) {
                            builder.SetInsertPoint(lastIfBB);
                            builder.CreateBr(exitBB);
                        }

                        if (blockOpen(lastElseBB)) {
                            builder.SetInsertPoint(lastElseBB);
                            builder.CreateBr(exitBB);
                        }

                        _lfb->setLastBlock(exitBB);
                    }

                    // connect
                } else {
                    // only if, so simpler logic.
                    // always need to create exitBB
                    exitBB = BasicBlock::Create(_env->getContext(), "exitIf", parentFunc);

                    // no if-branch variable realizations? I.e., this means all blocks returned.
                    // Thus, simply restore old ones...
                    if(if_var_realizations.empty()) {
                        llvm::IRBuilder<> exitBuilder(exitBB);
                        restoreVariableSlots(exitBuilder, var_realizations, true);
                    }

                    builder.CreateCondBr(ifcond, ifBB, exitBB);

                    // check if if contained ret, if not then connect to exitBB
                    if (blockOpen(lastIfBB)) {
                        builder.SetInsertPoint(lastIfBB);
                        builder.CreateBr(exitBB);
                    }

                    _lfb->setLastBlock(exitBB);
                }

                // statement done.
                // @TODO: optimize to only address variables where things get assigned to in order to generate
                // less LLVM IR. => Ease burden on compiler.
                builder.SetInsertPoint(_lfb->getLastBlock());

                // @TODO: also the exitBlock analysis!
            }
        }

        void BlockGeneratorVisitor::generateIfElse(NIfElse* ifelse,
                                                   bool exceptOnThen,
                                                   bool exceptOnElse) {
            // regular ifelse compilation
            // ...
            if(ifelse->isExpression())
                generateIfElseExpression(ifelse);
            else
                generateIfElseStatement(ifelse, exceptOnThen, exceptOnElse);
        }

        void BlockGeneratorVisitor::visit(NIfElse *ifelse) {
            if(earlyExit())return;

            // @TODO: use annotations here to decide which block to use/follow
            // => there is also the option that ifelse might be ignored!
            auto visit_t = whichBranchToVisit(ifelse);
            auto visit_ifelse = std::get<0>(visit_t);
            auto visit_if = std::get<1>(visit_t);
            auto visit_else = std::get<2>(visit_t);

            // speculative processing on this ifelse?
            bool speculate = ifelse->annotation().numTimesVisited > 0;

            if(!visit_ifelse) {
                _logger.debug("ifelse skipped.");
                return;
            }

            // annotations available?
            // ==> check what to compile
            // no? then regular compilation
            assert(ifelse->_then);
            assert(ifelse->_expression);
            if(ifelse->_else) {
                // else branch exists
                // do both have annotations or none?
                //assert((ifelse->_then->hasAnnotation() && ifelse->_else->hasAnnotation()) ||
                //               (!ifelse->_then->hasAnnotation() && !ifelse->_else->hasAnnotation()));
                // decide which branch should be generated, the other one results in a not normal case exception
                if(speculate) {
                    bool branchToExcept = ifelse->_then->annotation().numTimesVisited >= ifelse->_else->annotation().numTimesVisited;
                    generateIfElse(ifelse, !branchToExcept, branchToExcept);
                } else {
                    // regular compilation
                    generateIfElse(ifelse);
                }
            } else {
                // only if branch exists, no else.

                // annotation existing on if branch?
                if(speculate) {
                    // is the branch rarely visited, i.e. less than 50% of the time?
                    // => this would indicate a rare condition on whose removal is being speculated.
                    // => other reason could be a type conflict of declared variables.
                    // Note: There might be a better way to estimate the frequency + variable conflict.
                    //       => should use that.
                    // the speculation rule.
                    // decide which branch to visit based on majority
                    auto numTimesIfVisited = ifelse->_then->annotation().numTimesVisited;
                    // for else, if else branch exists, take that annotation number. Else, simply check how often if is not visited.
                    auto numTimesElseVisited = ifelse->annotation().numTimesVisited - numTimesIfVisited;

                    if(numTimesIfVisited < numTimesElseVisited) {
                        // always throw exception to force interpreter path!
                        generateIfElse(ifelse, true);
                        std::stringstream ss;
                        ss<<"if branch optimized away, as attained in trace only "<<std::setprecision(2)
                         <<100.0 * numTimesIfVisited / (1.0 * numTimesIfVisited + numTimesElseVisited)
                         <<"% of all cases";
                        _logger.debug(ss.str());
                    } else {
                        // generate sole if branch
                        generateIfElse(ifelse);
                    }
                } else {
                    // regular ifelse compilation, i.e. both branches active.
                    generateIfElse(ifelse);
                }
            }
        }

        void BlockGeneratorVisitor::visit(NLambda *lambda) {
            if(earlyExit())return;

            assert(_env);
            assert(lambda);
            auto &context = _env->getContext();

            std::string twine = "rettuple"; // name for the structtypes used as return values
            std::string func_name = getNextLambdaName();

            // inc lambda func counter
            _numLambdaFunctionsEncountered++;

            // up to one lambda is allowed
            if (_numLambdaFunctionsEncountered > 1) {
                error("nested lambda functions are not supported");
                return;
            }

            if(_lfb)
                delete _lfb;
            _lfb = nullptr;

            _lfb = new LambdaFunctionBuilder(_logger, _env);
            _lfb->create(lambda, func_name);

            // fetch LLVM IR func name
            _funcNames.push(_lfb->funcName());

            // insert into map
            auto builder = _lfb->getLLVMBuilder();

            declareVariables(lambda);

            // @TODO: this part should be refactored. I.e. BlockGeneratorVisitor to generate one block.
            // then other visitor to stitch functions together...
            // build inner function logic!
            lambda->_expression->accept(*this);

            // is lambda an early ending one?
            // => warn, this is really bad because it basically means the lambda will ALWAYS throw
            if(earlyExit()) {
                assert(_blockStack.empty());

                _logger.warn("Lambda function produces always exceptions. This means, pipeline will"
                             " get processed in slow or python-only mode.");
            } else {
                // crete return value from lat expression on stack
                // fetch last value from stack & use as return value!
                assert(_blockStack.size() > 0);
                // get one from blockstack as retval...

                auto retVal = _blockStack.back();
                _blockStack.pop_back();

                // upcast
                assert(lambda->getInferredType().isFunctionType());
                auto lamReturnType = lambda->getInferredType().getReturnType();
                retVal = upCastReturnType(builder, retVal, lambda->_expression->getInferredType(), lamReturnType);

                // fetch type of child node!
                _lfb->addReturn(retVal);
            }

            delete _lfb;
            _lfb = nullptr;
        }

        void BlockGeneratorVisitor::visit(NIdentifier *id) {
            if(earlyExit())return;

            assert(id);
            assert(_lfb);

            auto builder = _lfb->getLLVMBuilder();

            // first check if a variable with this type exists
            // and is defined!
            auto slot = getSlot(id->_name);
            if(slot && slot->isDefined(builder)) {
                // unbound local check, if not defined throw UnboundLocal error!
                slot->generateUnboundLocalCheck(*_lfb, builder);

                assert(slot->type != python::Type::UNKNOWN &&
                id->getInferredType() != python::Type::UNKNOWN);

                // check whether upcast is necessary
                if(slot->type != id->getInferredType()) {

                    // can id get upcasted to slot type?
                    // downcast!
                    if(python::canUpcastType(id->getInferredType(), slot->type)) {

                    } else if(python::canUpcastType(slot->type, id->getInferredType())) {
                        // upcast
                        auto var = slot->var.load(builder);
                        auto casted_car = upCastReturnType(builder, var, slot->type, id->getInferredType());
                        addInstruction(var.val, var.size, var.is_null);
                    } else {
                        std::stringstream ss;
                        ss<<"slot type is: "<<slot->type.desc()<<" id type is: "<<id->getInferredType().desc();
                        error("perform upcasting or downcasting, typing issue. Details: " + ss.str());
                    }
                }

                // load from var
                auto var = slot->var.load(builder);

#ifndef NDEBUG
                //_env->debugPrint(builder, "accessing var " + slot->var.name + " =", var.val);
#endif

                addInstruction(var.val, var.size, var.is_null);
                return;
            }

//            auto it = _variables.find(std::make_tuple(id->_name, id->getInferredType()));
//            if (it != _variables.end()) {
//                auto var = it->second.load(builder);
//                addInstruction(var.val, var.size, var.is_null);
//                return;
//            }

            auto jt = _globals.find(id->_name);
            if(jt != _globals.end()) {
                auto global_type = std::get<0>(jt->second);
                auto global_var = std::get<1>(jt->second);
                if(id->getInferredType() != global_type)
                    error("Global type " + global_type.desc() + " not compatible with required type " + id->getInferredType().desc());
                auto var = global_var.load(builder);
                addInstruction(var.val, var.size, var.is_null);
                return;
            }

            // Correct way to do things is here
            // ==> need to use better symbol table/scope traversal
            // i.e. bring symbols in scope when visiting a subscope!
            // ==> then check for identifiers if they exist in current scope and add then dummy if necessary!
            // // then, perform parameter lookup
            // auto param = _lfb->getParameter(id->_name);
            // if(param.val || param.is_null)
            //     addInstruction(param.val, param.size, param.is_null);
            // else
            //     error("could not find identifier with name " + id->_name);

            // deprecated... should be removed, because params are within varslots now!!!
            // then, perform parameter lookup
            auto param = _lfb->getParameter(id->_name);

            // this here is bad...
            if (param.val || param.is_null)
                addInstruction(param.val, param.size, param.is_null);
            else
                addInstruction(nullptr, nullptr,
                               nullptr); // dummy! functions are not checked yet. That's bad!!! ==> i.e. better scope thing in the future.
        }

        void BlockGeneratorVisitor::visit(NTuple *tuple) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            // handle special case empty tuple
            if (tuple->getInferredType() == python::Type::EMPTYTUPLE) {
                // create alloc instruction for tuple and fill it with stack elements
                assert(_lfb);
                auto builder = _lfb->getLLVMBuilder();

                auto &context = _env->getContext();

                // empty tuple is represented by special type emptytuple.
                // simply allocate this (dummy) type and return load of it
                auto alloc = builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr);
                auto load = builder.CreateLoad(alloc);

                // size of empty tuple is also 8 bytes (serialized size!)
                addInstruction(load, _env->i64Const(sizeof(int64_t)));
            } else {
                // tuple with at least one element
                // visit children, this should push as many nodes to the stack as this tuple has elements
                ApatheticVisitor::visit(tuple);

                // if tuple expression was early terminated...
                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return;
                }

                assert(_blockStack.size() >= tuple->_elements.size());

                // create alloc instruction for tuple and fill it with stack elements
                assert(_lfb);
                auto builder = _lfb->getLLVMBuilder();

                auto &context = _env->getContext();


                // new version: for this tuple, create flattened representation
                auto tt = tuple->getInferredType();

                FlattenedTuple ft(_env);
                ft.init(tt);

                // fetch values from blockstack & fill in to flattened tuple
                assert(_blockStack.size() >= tuple->_elements.size());
                std::vector<SerializableValue> vals;
                for (int i = 0; i < tuple->_elements.size(); ++i) {
                    auto val = _blockStack.back();
                    _blockStack.pop_back();
                    vals.push_back(val);
                }

                // reverse due to stack calling...
                std::reverse(vals.begin(), vals.end());

                // put to flattenedtuple (incl. assigning tuples!)
                for (int i = 0; i < tuple->_elements.size(); ++i)
                    ft.setElement(builder, i, vals[i].val, vals[i].size, vals[i].is_null);

                // get loadable struct type
                auto ret = ft.getLoad(builder);
                assert(ret->getType()->isStructTy());
                auto size = ft.getSize(builder);
                addInstruction(ret, size);
            }
        }

        SerializableValue
        BlockGeneratorVisitor::createCJSONFromDict(NDictionary *dict, const std::vector<SerializableValue> &keys,
                                                   const std::vector<SerializableValue> &vals) {
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            auto ret = builder.CreateCall(cJSONCreateObject_prototype(_env->getContext(), _env->getModule().get()), {});
            for (unsigned i = 0; i < dict->_pairs.size(); ++i) {
                CallInst *value;
                Value *key = dictionaryKey(_env->getContext(), _env->getModule().get(), builder, keys[i].val,
                                           dict->_pairs[i].first->getInferredType(),
                                           dict->_pairs[i].second->getInferredType());
                if (key == nullptr) {
                    error("Invalid key for dictionary");
                    return {};
                }
                // get cJSON object with value
                auto valtype = dict->_pairs[i].second->getInferredType();
                if (vals[i].val->getType()->isDoubleTy() && valtype == python::Type::F64) {
                    value = builder.CreateCall(
                            cJSONCreateNumber_prototype(_env->getContext(), _env->getModule().get()),
                            {vals[i].val});
                } else if (vals[i].val->getType()->isIntegerTy() && valtype == python::Type::I64) {
                    value = builder.CreateCall(
                            cJSONCreateNumber_prototype(_env->getContext(), _env->getModule().get()),
                            {upCast(builder, vals[i].val, _env->doubleType())});
                } else if (vals[i].val->getType()->isPointerTy() && valtype == python::Type::STRING) {
                    value = builder.CreateCall(
                            cJSONCreateString_prototype(_env->getContext(), _env->getModule().get()),
                            {vals[i].val});
                } else if (vals[i].val->getType()->isIntegerTy(8) && valtype == python::Type::BOOLEAN) {
                    value = builder.CreateCall(
                            cJSONCreateBool_prototype(_env->getContext(), _env->getModule().get()),
                            {upCast(builder, vals[i].val, _env->i64Type())});
                } else {
                    std::string type_str;
                    llvm::raw_string_ostream rso(type_str);
                    vals[i].val->getType()->print(rso);
                    error("Unsupported type in dictionary: " + valtype.desc() + "; llvm:" + rso.str());
                    return {};
                }

                builder.CreateCall(cJSONAddItemToObject_prototype(_env->getContext(), _env->getModule().get()),
                                   {ret, key, value});
            }
            assert(ret->getType()->isPointerTy());
            auto cjsonstr = builder.CreateCall(
                    cJSONPrintUnformatted_prototype(_env->getContext(), _env->getModule().get()),
                    {ret});
            auto size = builder.CreateAdd(
                    builder.CreateCall(strlen_prototype(_env->getContext(), _env->getModule().get()), {cjsonstr}),
                    _env->i64Const(1));
            return {ret, size};
        }

        void BlockGeneratorVisitor::visit(NDictionary *dict) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            // correctly typed??
            assert(dict->getInferredType() == python::Type::GENERICDICT || dict->getInferredType().isDictionaryType());

            ApatheticVisitor::visit(dict);

            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }

            assert(_blockStack.size() >= 2 * dict->_pairs.size());

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();
            std::vector<SerializableValue> keys, vals;
            for (int i = 0; i < (int) dict->_pairs.size(); ++i) {
                auto val = _blockStack.back();
                _blockStack.pop_back();
                assert(val.size);
                assert(val.val);
                vals.push_back(val);

                auto key = _blockStack.back();
                _blockStack.pop_back();
                assert(key.size);
                assert(key.val);
                keys.push_back(key);
            }
            // reverse so that it matches NDictionary ASTNode
            std::reverse(vals.begin(), vals.end());
            std::reverse(keys.begin(), keys.end());
            // build cJSON object from dict
            auto dict_rep = createCJSONFromDict(dict, keys, vals);
            addInstruction(dict_rep.val, dict_rep.size);
        }

        void BlockGeneratorVisitor::visit(NList *list) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            if (list->getInferredType() == python::Type::EMPTYLIST) {
                // if they ever call an append on an empty list, it should fix the element type, so this is a weird case
                ApatheticVisitor::visit(list);
                // restore stack
                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return;
                }

                addInstruction(nullptr, nullptr);
            } else {
                auto llvmType = _env->pythonToLLVMType(list->getInferredType());

                // visit children, this should push as many nodes to the stack as this list has elements
                ApatheticVisitor::visit(list);
                // if error happened abandon compilation
                // restore stack
                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return;
                }
                assert(_blockStack.size() >= list->_elements.size());

                // create alloc instruction for list and fill it with stack elements
                assert(_lfb);
                auto builder = _lfb->getLLVMBuilder();
                auto &context = _env->getContext();

                // fetch values from _blockStack
                assert(_blockStack.size() >= list->_elements.size());
                std::vector<SerializableValue> vals;
                for (int i = 0; i < list->_elements.size(); ++i) {
                    auto val = _blockStack.back();
                    _blockStack.pop_back();
                    vals.push_back(val);
                }
                std::reverse(vals.begin(), vals.end());

                // Allocate space for the list (TODO: check if this is the correct way to do this)
                // check if the block of builder is the entry block, if not add alloc to entry block
                // entry block has no predecessor
                llvm::Value *listAlloc = _env->CreateFirstBlockAlloca(builder, llvmType, "BGV_listAlloc");
                llvm::Value* listSize = _env->i64Const(8);
                auto elementType = list->getInferredType().elementType();
                if(elementType.isSingleValued()) {
                    builder.CreateStore(_env->i64Const(list->_elements.size()), listAlloc);
                } else if(elementType == python::Type::I64 || elementType == python::Type::F64 || elementType == python::Type::BOOLEAN
                || elementType == python::Type::STRING || elementType.isTupleType() || elementType.isDictionaryType()) {
                    // load the list with its initial size
                    auto list_capacity_ptr = _env->CreateStructGEP(builder, listAlloc, 0);
                    builder.CreateStore(_env->i64Const(list->_elements.size()), list_capacity_ptr);
                    auto list_len_ptr = _env->CreateStructGEP(builder, listAlloc,  1);
                    builder.CreateStore(_env->i64Const(list->_elements.size()), list_len_ptr);

                    // load the initial values ------
                    // get the byte-size of the elements TODO: is there a better way to do this?
                    size_t element_byte_size = 8; // f64, i64
                    if (elementType == python::Type::BOOLEAN)
                        element_byte_size = 1; // single character elements
                    // allocate the array
                    llvm::Value *malloc_size = nullptr;
                    if(elementType.isTupleType() && elementType.isFixedSizeType()) {
                        // if tuple is fixed size, store the actual tuple struct
                        // if tuple has varlen field, store a pointer to the tuple
                        auto ft = FlattenedTuple::fromLLVMStructVal(_env, builder, vals[0].val, elementType);
                        malloc_size = builder.CreateMul(ft.getSize(builder), _env->i64Const(list->_elements.size()));
                    } else {
                        malloc_size = _env->i64Const(element_byte_size * list->_elements.size());
                    }
                    auto list_arr_malloc = builder.CreatePointerCast(_env->malloc(builder, malloc_size), llvmType->getStructElementType(2));
                    // store the values
                    for(size_t i = 0; i < vals.size(); i++) {
                        auto list_el = builder.CreateGEP(list_arr_malloc, _env->i32Const(i));
                        if(elementType.isTupleType() && !elementType.isFixedSizeType()) {
                            // list_el has type struct.tuple**
                            auto el_tuple = _env->CreateFirstBlockAlloca(builder, _env->pythonToLLVMType(elementType), "tuple_alloc");
                            builder.CreateStore(vals[i].val, el_tuple);
                            builder.CreateStore(el_tuple, list_el);
                        } else {
                            builder.CreateStore(vals[i].val, list_el);
                        }
                    }
                    // store the new array back into the array pointer
                    auto list_arr = _env->CreateStructGEP(builder, listAlloc, 2);
                    builder.CreateStore(list_arr_malloc, list_arr);

                    // set the serialized size (i64/f64/bool are fixed sized!)
                    listSize = _env->i64Const(8 * list->_elements.size() + 8);  // TODO: are booleans serialized as 1 or 8 bytes?

                    // if string values, store the lengths as well
                    if(elementType == python::Type::STRING || elementType.isDictionaryType()) {
                        listSize = _env->i64Const(8 * list->_elements.size() + 8); // length field, size array
                        // allocate the size array
                        auto list_sizearr_malloc = builder.CreatePointerCast(_env->malloc(builder, _env->i64Const(8 * list->_elements.size())), llvmType->getStructElementType(3));
                        // store the lengths
                        for(size_t i = 0; i < vals.size(); i++) {
                            auto list_el = builder.CreateGEP(list_sizearr_malloc, _env->i32Const(i));
                            builder.CreateStore(vals[i].size, list_el);
                            listSize = builder.CreateAdd(listSize, vals[i].size);
                        }
                        // store the new array back into the array pointer
                        auto list_sizearr = _env->CreateStructGEP(builder, listAlloc, 3);
                        builder.CreateStore(list_sizearr_malloc, list_sizearr);
                    }
                }

                // TODO:
                // --> change to passing around the pointer to the list, not the semi-loaded struct
                // ---> THIS WILL HAVE IMPLICATIONS WHEREVER LISTS ARE USED.
                // also listSize here is wrong. The listSize should be stored as part of the pointer. You can either pass 8 as listsize or null.

                addInstruction(builder.CreateLoad(listAlloc), listSize);
            }
        }

        void BlockGeneratorVisitor::visit(NRange* range) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            ApatheticVisitor::visit(range);
            // restore stack
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }

            // _func should have yields all the parameters
            assert(_blockStack.size() >= range->_positionalArguments.size());

            // the values
            SerializableValue funcVal;
            std::vector<SerializableValue> args;

            // pop as many args as required from stack
            for (int i = 0; i < range->_positionalArguments.size(); ++i) {
                args.emplace_back(_blockStack.back());
                _blockStack.pop_back();
            }

            // reverse args vector (it's a stack!!!)
            std::reverse(args.begin(), args.end());

            // allocate the range object
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();
            auto &context = _env->getContext();
            auto rangeStructPtr = _env->CreateFirstBlockAlloca(builder, _env->getRangeObjectType(), "range");

            // store the data in
            if(args.size() == 1) {
                auto elPtr = _env->CreateStructGEP(builder, rangeStructPtr, 0);
                builder.CreateStore(_env->i64Const(0), elPtr);
                elPtr = _env->CreateStructGEP(builder, rangeStructPtr, 1);
                builder.CreateStore(args[0].val, elPtr); // stop is the argument
                elPtr = _env->CreateStructGEP(builder, rangeStructPtr, 2);
                builder.CreateStore(_env->i64Const(1), elPtr);
            } else if(args.size() == 2) {
                for(int i = 0; i < 2; ++i) {
                    auto elPtr = _env->CreateStructGEP(builder, rangeStructPtr, i);
                    builder.CreateStore(args[i].val, elPtr);
                }
                auto elPtr = _env->CreateStructGEP(builder, rangeStructPtr, 2);
                builder.CreateStore(_env->i64Const(1), elPtr);
            } else {
                assert(args.size() == 3);
                for(int i = 0; i < 3; ++i) {
                    auto elPtr = _env->CreateStructGEP(builder, rangeStructPtr, i);
                    builder.CreateStore(args[i].val, elPtr);
                }
            }

            // return the object
            addInstruction(rangeStructPtr, _env->i64Const(sizeof(uint8_t*))); // pointer
        }

        void BlockGeneratorVisitor::visit(NComprehension* comprehension) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            // add variables
            auto id = comprehension->target;

            // Note: no support for multiple targets yet??
            // => TODO listed here: https://github.com/LeonhardFS/Tuplex/issues/212
            // add id as variable + add instruction
            auto builder = _lfb->getLLVMBuilder();
            VariableSlot slot;
            slot.type = id->getInferredType();
            slot.definedPtr = _env->CreateFirstBlockAlloca(builder, _env->i1Type(), id->_name + "_defined");
            assert(slot.definedPtr);
            builder.CreateStore(_env->i1Const(true), slot.definedPtr);
            slot.var = Variable(*_env, builder, id->getInferredType(), id->_name);
            _variableSlots[id->_name] = slot;

            // visit recursively
            ApatheticVisitor::visit(comprehension);
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // early end expression
            }

            _blockStack.pop_back(); // get rid of the target load

            // add the pointer so caller retrieves it.
            addInstruction(slot.var.ptr, slot.var.sizePtr, slot.var.nullPtr); // add target pointer
        }

        void BlockGeneratorVisitor::visit(NListComprehension* listComprehension) {
            if(earlyExit())return;

            // comprehensions define their own variables.
            // I.e., back all variables up here and then restore them after list is done.
            // => no variable leakage!
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();
            auto variables_snapshot = snapshotVariableValues(builder);

            auto num_stack_before = _blockStack.size();

            // avoid visiting expression
            for(auto gen : listComprehension->generators) {
                assert(gen);
                gen->accept(*this);

                if(earlyExit()) {
                    while(_blockStack.size() > num_stack_before)
                        _blockStack.pop_back();
                    return; // early end expression
                }
            }

            builder.SetInsertPoint(_lfb->getLastBlock());

            if(listComprehension->generators.size() > 1)
                error("Codegen for list comprehension with multiple generators not implemented");

            if (listComprehension->getInferredType() == python::Type::EMPTYLIST || listComprehension->getInferredType() == python::Type::EMPTYTUPLE) {
                addInstruction(nullptr, nullptr);
                return;
            }

            auto iterType = listComprehension->generators[0]->iter->getInferredType();
            if(iterType == python::Type::RANGE || iterType == python::Type::STRING || (iterType.isListType() && iterType != python::Type::EMPTYLIST) || (iterType.isTupleType() && tupleElementsHaveSameType(iterType))) {
                auto elementType = listComprehension->getInferredType().elementType();
                auto listLLVMType = _env->getListType(listComprehension->getInferredType());

                auto target = _blockStack.back(); // from comprehension
                _blockStack.pop_back();
                auto iter = _blockStack.back(); // from comprehension
                _blockStack.pop_back();

                llvm::Value *start, *stop, *step;
                if(iterType == python::Type::RANGE) {
                    // get range parameters
                    start = builder.CreateLoad(_env->CreateStructGEP(builder, iter.val, 0));
                    stop = builder.CreateLoad(_env->CreateStructGEP(builder, iter.val, 1));
                    step = builder.CreateLoad(_env->CreateStructGEP(builder, iter.val, 2));
                } else if(iterType == python::Type::STRING) {
                    start = _env->i64Const(0);
                    stop = builder.CreateSub(iter.size, _env->i64Const(1));
                    step = _env->i64Const(1);
                } else if(iterType.isListType()) {
                    start = _env->i64Const(0);
                    step = _env->i64Const(1);
                    if(iterType.elementType().isSingleValued()) {
                        stop = iter.val;
                    } else {
                        stop = builder.CreateExtractValue(iter.val, {1});
                    }
                } else if(iterType.isTupleType() && tupleElementsHaveSameType(iterType)) {
                    start = _env->i64Const(0);
                    step = _env->i64Const(1);
                    stop = _env->i64Const(iterType.parameters().size());
                } else {
                    error("unsupported type for list comprehension generator" + iterType.desc());
                }

                // calculate number of iterations
                auto diff = builder.CreateSub(stop, start);
                auto numiters = _env->floorDivision(builder, diff, step);
                auto hasnorem = builder.CreateICmpEQ(builder.CreateSRem(diff, step), _env->i64Const(0));
                numiters = builder.CreateSelect(hasnorem, numiters, builder.CreateAdd(numiters, _env->i64Const(1)));

                llvm::Value *listAlloc = _env->CreateFirstBlockAlloca(builder, listLLVMType,
                                                                      "BGV_listComprehensionAlloc");
                llvm::Value *listSize =  _env->CreateFirstBlockAlloca(builder, _env->i64Type(), "BGV_listComprehensionSize");
                if(elementType == python::Type::NULLVALUE || elementType == python::Type::EMPTYDICT || elementType == python::Type::EMPTYTUPLE) {
                    builder.CreateStore(numiters, listAlloc);
                    builder.CreateStore(_env->i64Const(8), listSize);
                } else {
                    builder.CreateStore(builder.CreateAdd(builder.CreateMul(numiters, _env->i64Const(8)), _env->i64Const(8)), listSize);

                    // load the list with its initial size
                    auto list_capacity_ptr = _env->CreateStructGEP(builder, listAlloc, 0);
                    builder.CreateStore(numiters, list_capacity_ptr);
                    auto list_len_ptr = _env->CreateStructGEP(builder, listAlloc, 1);
                    builder.CreateStore(numiters, list_len_ptr);

                    // allocate the array
                    size_t element_byte_size = 8; // f64, i64
                    if (listComprehension->getInferredType().elementType() == python::Type::BOOLEAN)
                        element_byte_size = 1; // single character elements
                    auto list_arr_malloc = builder.CreatePointerCast(
                            _env->malloc(builder, builder.CreateMul(numiters, _env->i64Const(element_byte_size))),
                            listLLVMType->getStructElementType(2));

                    // store the new array back into the array pointer
                    auto list_arr = _env->CreateStructGEP(builder, listAlloc, 2);
                    builder.CreateStore(list_arr_malloc, list_arr);

                    llvm::Value* list_sizearr_malloc;
                    if(elementType == python::Type::STRING) {
                        // allocate string len array
                        list_sizearr_malloc = builder.CreatePointerCast(
                                _env->malloc(builder, builder.CreateMul(numiters, _env->i64Const(8))),
                                listLLVMType->getStructElementType(3));

                        // store the new array back into the array pointer
                        auto list_sizearr = _env->CreateStructGEP(builder, listAlloc, 3);
                        builder.CreateStore(list_sizearr_malloc, list_sizearr);
                    }

                    // initialize target
                    llvm::Value *tuple_array, *tuple_sizes;
                    if(iterType == python::Type::RANGE) {
                        builder.CreateStore(start, target.val);
                    } else if(iterType == python::Type::STRING) {
                        // create a 1 character string for the target
                        auto newtargetstr = builder.CreatePointerCast(_env->malloc(builder, _env->i64Const(2)),
                                                                      _env->i8ptrType());
                        // do via load & store, no need for memcpy here yet
                        auto startChar = builder.CreateLoad(builder.CreateGEP(iter.val, start));
                        builder.CreateStore(startChar, newtargetstr); // store charAtIndex at ptr
                        builder.CreateStore(_env->i8Const(0),
                                            builder.CreateGEP(newtargetstr, _env->i32Const(1))); // null terminate
                        builder.CreateStore(newtargetstr, target.val);
                        builder.CreateStore(_env->i64Const(2), target.size);
                    } else if(iterType.isListType()) {
                        if(iterType.elementType().isSingleValued()) {
                            // don't need to do anything
                        } else {
                            auto init_val = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(iter.val, {2}), start));
                            builder.CreateStore(init_val, target.val);
                            if(iterType.elementType() == python::Type::STRING) {
                                auto init_size = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(iter.val, {3}), start));
                                builder.CreateStore(init_size, target.size);
                            }
                        }
                    } else if(iterType.isTupleType() && tupleElementsHaveSameType(iterType)) {
                        // store loaded vals into array & then index via gep
                        auto tupleElementType = iterType.parameters().front();
                        auto numElements = iterType.parameters().size();

                        // create array & index
                        tuple_array = builder.CreateAlloca(_env->pythonToLLVMType(tupleElementType), _env->i64Const(numElements));
                        tuple_sizes = builder.CreateAlloca(_env->i64Type(), _env->i64Const(numElements));

                        // store the elements into the array
                        FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env, builder, iter.val, iterType);

                        std::vector<SerializableValue> elements;
                        for (int i = 0; i < numElements; ++i) {
                            auto load = ft.getLoad(builder, {i});
                            elements.push_back(load);
                        }

                        // fill in array elements
                        for (int i = 0; i < numElements; ++i) {
                            builder.CreateStore(elements[i].val, builder.CreateGEP(tuple_array, i32Const(i)));
                            builder.CreateStore(elements[i].size, builder.CreateGEP(tuple_sizes, i32Const(i)));
                        }

                        // load from array
                        auto init_val = builder.CreateLoad(builder.CreateGEP(tuple_array, builder.CreateTrunc(start, _env->i32Type())));
                        builder.CreateStore(init_val, target.val);
                        auto init_size = builder.CreateLoad(builder.CreateGEP(tuple_sizes, builder.CreateTrunc(start, _env->i32Type())));
                        builder.CreateStore(init_size, target.size);
                    }

                    // generate + store the values
                    auto bodyBlock1 = BasicBlock::Create(_env->getContext(), "listComp_bodyBlock1",
                                                        builder.GetInsertBlock()->getParent());
                    auto retBlock = BasicBlock::Create(_env->getContext(), "listComp_retBlock",
                                                       builder.GetInsertBlock()->getParent());

                    auto startBB = builder.GetInsertBlock(); // current basic block (used for phinode)

                    // loop body node
                    builder.CreateBr(bodyBlock1);
                    builder.SetInsertPoint(bodyBlock1);
                    auto loopVar = builder.CreatePHI(_env->i64Type(), 2);
                    loopVar->addIncoming(_env->i64Const(0), startBB); // start the loop variable at 0

                    auto list_el = builder.CreateGEP(list_arr_malloc, loopVar);
                    _lfb->setLastBlock(bodyBlock1);

                    // -------
                    // generate expression, vars & Co should be in nametable for this!

                    listComprehension->expression->accept(*this); // TODO: assumes that the expression will only push a single element onto the stack
                    if(earlyExit())return;
                    // end expression generation
                    // ------

                    builder.SetInsertPoint(_lfb->getLastBlock());
                    auto expression = _blockStack.back();

                    assert(expression.is_null || expression.val);

                    _blockStack.pop_back();
                    builder.CreateStore(expression.val, list_el);

                    // if string values, store the lengths as well
                    if (elementType == python::Type::STRING) {
                        auto list_len_el = builder.CreateGEP(list_sizearr_malloc, loopVar);
                        builder.CreateStore(expression.size, list_len_el);
                        builder.CreateStore(builder.CreateAdd(builder.CreateLoad(listSize), expression.size), listSize);
                    }

                    auto nextLoopVar = builder.CreateAdd(loopVar, _env->i64Const(1));
                    loopVar->addIncoming(nextLoopVar, builder.GetInsertBlock()); // add nextloopvar as a phi node input to the loopvar

                    if(iterType == python::Type::RANGE) {
                        builder.CreateStore(builder.CreateAdd(builder.CreateLoad(target.val), step),
                                            target.val); // target += step
                    } else if(iterType == python::Type::STRING) {
                        // TODO: can I just keep modifying the same string here, instead of allocating new ones?
                        // create a 1 character string for the target
                        auto newtargetstr = builder.CreatePointerCast(_env->malloc(builder, _env->i64Const(2)),
                                                                      _env->i8ptrType());
                        // do via load & store, no need for memcpy here yet
                        auto startChar = builder.CreateLoad(builder.CreateGEP(iter.val, nextLoopVar));
                        builder.CreateStore(startChar, newtargetstr); // store charAtIndex at ptr
                        builder.CreateStore(_env->i8Const(0),
                                            builder.CreateGEP(newtargetstr, _env->i32Const(1))); // null terminate
                        builder.CreateStore(newtargetstr, target.val);
                        builder.CreateStore(_env->i64Const(2), target.size);
                    } else if(iterType.isListType()) {
                        if(iterType.elementType().isSingleValued()) {
                            // don't need to do anything
                        } else {
                            auto init_val = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(iter.val, {2}), nextLoopVar));
                            builder.CreateStore(init_val, target.val);
                            if(iterType.elementType() == python::Type::STRING) {
                                auto init_size = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(iter.val, {3}), nextLoopVar));
                                builder.CreateStore(init_size, target.size);
                            }
                        }
                    } else if(iterType.isTupleType() && tupleElementsHaveSameType(iterType)) {
                        // load from array
                        auto init_val = builder.CreateLoad(builder.CreateGEP(tuple_array, builder.CreateTrunc(nextLoopVar, _env->i32Type())));
                        builder.CreateStore(init_val, target.val);
                        auto init_size = builder.CreateLoad(builder.CreateGEP(tuple_sizes, builder.CreateTrunc(nextLoopVar, _env->i32Type())));
                        builder.CreateStore(init_size, target.size);
                    }

                    auto keep_looping = builder.CreateICmpSLT(nextLoopVar, numiters);
                    builder.CreateCondBr(keep_looping, bodyBlock1, retBlock);

                    builder.SetInsertPoint(retBlock);
                    _lfb->setLastBlock(retBlock);
                }
                addInstruction(builder.CreateLoad(listAlloc), builder.CreateLoad(listSize));
            } else {
                throw std::runtime_error("Unsupported iterable in list comprehension codegen: " + iterType.desc());
            }

            // restore variables (delete first all which do not exist in variables_snapshot
            restoreVariableSlots(builder, variables_snapshot, true);
        }

        void BlockGeneratorVisitor::visit(NCompare *cmp) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();
            // python compare operations are a bit odd.
            // i.e. 3 < 4 != 5 is perfectly legal. This translates to 3 < 4 && 4 != 5
            // => break into binary instructions and add them for final result

            // visit children & make sure there are enough elements on the stack
            ApatheticVisitor::visit(cmp);
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }
            assert(_blockStack.size() >= cmp->_comps.size() + 1); // +1 for the left

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            // two cases:
            // (1) [basically not reached b.c. CleanAstVisitor would have eleminated it]
            // only left
            if (cmp->_comps.size() == 0) {
                // just return the value from the stack
                // i.e. nothing todo.
                //auto val = _blockStack.back();
                //_blockStack.pop_back();
                // addInstruction(val)
            }
                // (2) left + ops
            else {

                std::vector<python::Type> types;
                types.push_back(cmp->_left->getInferredType());
                for (const auto &el : cmp->_comps)
                    types.push_back(el->getInferredType());

                // pop all the compares
                // get element vals from stack
                // note the reversion of the index due to the stack!
                std::vector<llvm::Value *> vals;
                std::vector<llvm::Value *> nulls;
                for (int i = 0; i < cmp->_comps.size() + 1; ++i) {
                    auto val = _blockStack.back().val;
                    auto isnull = _blockStack.back().is_null;
                    _blockStack.pop_back();
                    vals.push_back(val);
                    nulls.push_back(isnull);
                }
                std::reverse(vals.begin(), vals.end());
                std::reverse(nulls.begin(), nulls.end());
                // now divide in pairs & and and them
                llvm::Value *lastPair = nullptr;
                assert(cmp->_comps.size() == cmp->_ops.size());
                for (int i = 1; i < vals.size(); ++i) {
                    auto a = vals[i - 1];
                    auto b = vals[i];
                    auto a_isnull = nulls[i - 1];
                    auto b_isnull = nulls[i];
                    auto atype = types[i - 1];
                    auto btype = types[i];

                    // first pair?
                    if (!lastPair)
                        lastPair = compareInst(builder, a, a_isnull, atype, cmp->_ops[i - 1], b, b_isnull, btype);
                    else {
                        // create bit and
                        auto newPair = compareInst(builder, a, a_isnull, atype, cmp->_ops[i - 1], b, b_isnull, btype);
                        lastPair = builder.CreateAnd(lastPair, newPair);
                    }
                }
                addInstruction(lastPair); // always not an option
            }
        }

        void BlockGeneratorVisitor::visit(NString *str) {
            if(earlyExit())return;

            assert(str);
            // generate global str value for this
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            // process string value, i.e. removing quotes and so on.
            auto val = str->value();

            auto sconst = builder.CreateGlobalStringPtr(val);
            auto sptr = builder.CreatePointerCast(sconst,
                                                  llvm::Type::getInt8PtrTy(_env->getContext(), 0)); // need gep to cast
            // from [n x i8]* to i8* type

            // size is determined via strlength + 1
            auto ssize = _env->i64Const(val.length() + 1);

            addInstruction(sptr, ssize);
        }

        SerializableValue BlockGeneratorVisitor::indexTupleWithStaticExpression(ASTNode *index_node,
                                                                                ASTNode *value_node,
                                                                                SerializableValue index,
                                                                                SerializableValue value) {

            auto builder = _lfb->getLLVMBuilder();

            if (index_node->type() == ASTNodeType::Number || index_node->type() == ASTNodeType::Boolean) {
                // just take directly the value and return the load...
                int idx = 0;
                if (index_node->type() == ASTNodeType::Number)
                    idx = static_cast<NNumber *>(index_node)->getI64();
                if (index_node->type() == ASTNodeType::Boolean)
                    idx = static_cast<NBoolean *>(index_node)->_value;

                // correct for negative indices (reverse lookup)
                if (idx < 0)
                    idx += value_node->getInferredType().parameters().size();

                // check whether index fits
                if (idx < 0 || idx >= value_node->getInferredType().parameters().size())
                    return SerializableValue(logErrorV("tried to index tuple with invalid indexing expression."),
                                             nullptr);

                // directly take values out of tuple
                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env,
                                                                      builder,
                                                                      value.val,
                                                                      value_node->getInferredType());

                return ft.getLoad(builder, {idx});
            } else {
                // THIS HERE IS BACKUP CODE, usable if the AST tree isn't reduced completely.
                _logger.warn(
                        "backup code used for [] operator: Make sure the AST tree is properly reduced in its literal expressions.");

                // ast tree is not completely reduced here, so generate expressions
                assert(isStaticValue(index_node, true));

                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env,
                                                                      builder,
                                                                      value.val,
                                                                      value_node->getInferredType());
                auto tupleNumElements = value_node->getInferredType().parameters().size();

                // create temp struct type & use GEP method to retrieve the element.
                // go over all the first level elements that are contained
                std::vector<SerializableValue> elements;
                std::vector<llvm::Type *> elementTypes;
                for (int i = 0; i < tupleNumElements; ++i) {
                    auto load = ft.getLoad(builder, {i});
                    elements.push_back(load);
                    elementTypes.push_back(load.val->getType());
                }

                // create new struct type to get the i-th element via getelementptr
                auto structType = llvm::StructType::create(_env->getContext(), elementTypes, "indextuple");
                // load the values into this struct type
                auto alloc = builder.CreateAlloca(structType, 0, nullptr);
                for (int i = 0; i < tupleNumElements; ++i)
                    builder.CreateStore(elements[i].val, builder.CreateGEP(alloc, {i32Const(0), i32Const(i)}));


                // fetch element
                auto lookupPtr = builder.CreateGEP(alloc, {i32Const(0), builder.CreateTrunc(index.val,
                                                                                            Type::getInt32Ty(
                                                                                                    _env->getContext()))});

                // also need to lookup size...
                auto salloc = builder.CreateAlloca(llvm::ArrayType::get(_env->i64Type(), tupleNumElements), 0,
                                                   nullptr);
                // insert elements
                for (int i = 0; i < tupleNumElements; ++i)
                    builder.CreateStore(elements[i].size,
                                        builder.CreateGEP(salloc, {i32Const(0), i32Const(i)}));

                auto retSize = builder.CreateLoad(builder.CreateGEP(salloc, {i32Const(0),
                                                                             builder.CreateTrunc(index.val,
                                                                                                 Type::getInt32Ty(
                                                                                                         _env->getContext()))}));
                auto retVal = builder.CreateLoad(lookupPtr);
                return SerializableValue(retVal, retSize);
            }
        }

        // Use offsets from the layout of the cJSON struct to fetch the correct values
        SerializableValue BlockGeneratorVisitor::subscriptCJSONDictionary(NSubscription *sub, SerializableValue index,
                                                                          const python::Type &index_type,
                                                                          SerializableValue value) {
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();
            auto subType = sub->getInferredType();

            auto key = dictionaryKey(_env->getContext(), _env->getModule().get(), builder, index.val,
                                     index_type, subType);
            if (key == nullptr) {
                error("Invalid dictionary key");
                return {};
            }
            auto cjson_val = builder.CreateCall(
                    cJSONGetObjectItem_prototype(_env->getContext(), _env->getModule().get()),
                    {value.val, key});
            // throw a keyerror if it is an invalid key
            _lfb->addException(builder, ExceptionCode::KEYERROR, builder.CreateIsNull(cjson_val));

            if (subType == python::Type::BOOLEAN) {
                // BOOL: in type
                auto isTrue = builder.CreateCall(
                        cJSONIsTrue_prototype(_env->getContext(), _env->getModule().get()),
                        {cjson_val});
                auto val = _env->upcastToBoolean(builder, builder.CreateICmpEQ(isTrue, _env->i64Const(1)));
                return {val, nullptr};
            } else if (subType == python::Type::STRING) {
                // STRING: 32 bytes offset
                auto valaddr = builder.CreateGEP(cjson_val, _env->i64Const(32));
                auto valptr = builder.CreatePointerCast(valaddr, llvm::Type::getInt64PtrTy(_env->getContext()));
                auto valload = builder.CreateLoad(valptr);
                auto val = builder.CreateCast(Instruction::CastOps::IntToPtr, valload, _env->i8ptrType());
                auto len = builder.CreateCall(strlen_prototype(_env->getContext(), _env->getModule().get()), {val});
                return {val, builder.CreateAdd(len, _env->i64Const(1))};
            } else if (subType == python::Type::I64) {
                // Integer: 40 bytes offset
                auto valaddr = builder.CreateGEP(cjson_val, _env->i64Const(40));
                auto valptr = builder.CreatePointerCast(valaddr, llvm::Type::getInt64PtrTy(_env->getContext()));
                return {builder.CreateLoad(llvm::Type::getInt64Ty(_env->getContext()), valptr),
                        _env->i64Const(sizeof(int64_t))};
            } else if (subType == python::Type::F64) {
                // Double: 48 bytes offset
                auto valaddr = builder.CreateGEP(cjson_val, _env->i64Const(48));
                auto valptr = builder.CreatePointerCast(valaddr, llvm::Type::getDoublePtrTy(_env->getContext()));
                return {builder.CreateLoad(llvm::Type::getDoubleTy(_env->getContext()), valptr),
                        _env->i64Const(sizeof(double))};
            } else {
                // throw error for non primitive value type
                addInstruction(logErrorV("Unsupported dictionary value type: " + subType.desc()));
                return {};
            }
        }

        void BlockGeneratorVisitor::visit(NSubscription *sub) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            // visit children first
            ApatheticVisitor::visit(sub);
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }

            auto &context = _env->getContext();

            // there should be now two instructions on the instruction stack
            assert(_blockStack.size() >= 2);

            auto index = _blockStack.back();
            _blockStack.pop_back();
            auto value = _blockStack.back();
            _blockStack.pop_back();

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            // handle option types here
            // ==> in python indexing lists, tuples, strings, sets with None gives a TypeError!
            // ==> for dictionaries, it's a KeyError!
            auto value_type = sub->_value->getInferredType();
            auto index_type = sub->_expression->getInferredType();

            if (index_type.isOptionType()) {
                assert(index.is_null);

                // add check
                if (value_type.withoutOptions().isDictionaryType()) {
                    // KeyError
                    _lfb->addException(builder, ExceptionCode::KEYERROR, index.is_null);
                } else {
                    // TypeError
                    _lfb->addException(builder, ExceptionCode::TYPEERROR, index.is_null);
                }
            }
            // remove option from index_type
            index_type = index_type.withoutOptions();

            if (value_type.isOptionType()) {
                // None can't be indexed, i.e. null check here!
                assert(value.is_null);

                _lfb->addException(builder, ExceptionCode::TYPEERROR, value.is_null);
            }
            value_type = value_type.withoutOptions();

            // there are two options:
            // either the value is a string or a struct type (aka tuple)
            if (value_type.isTupleType()) {

                // for empty tuple, there will be always an error here!

                int tupleNumElements = value_type.parameters().size();

                // correct for negative indices (once)
                auto cmp = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, index.val, _env->i64Const(0));
                index = SerializableValue(builder.CreateSelect(cmp,
                                                               builder.CreateAdd(index.val,
                                                                                 _env->i64Const(tupleNumElements)),
                                                               index.val),
                                          index.size);

                // there are 3 cases
                // case 1: expression is a constant/scalar value => compilable
                // case 2: value is a tuple of elements with identical type => compilable
                // case 3: expression is a variable (runtime) and tuple has elements with different types => not compilable, needs
                // to be run via interpreter.

                auto expression = sub->_expression;

                // case 1: to be implemented
                if (isStaticValue(expression, true)) {
                    auto ret = indexTupleWithStaticExpression(expression, sub->_value, index, value);
                    addInstruction(ret.val, ret.size, ret.is_null);
                }
                    // case 2: load to array & then select via gep
                else if (tupleElementsHaveSameType(value_type)) {

                    // store loaded vals into array & then index via gep
                    auto elementType = value_type.parameters().front();
                    auto numElements = value_type.parameters().size();

                    // create array & index
                    auto array = builder.CreateAlloca(_env->pythonToLLVMType(elementType), _env->i64Const(numElements));
                    auto sizes = builder.CreateAlloca(_env->i64Type(), _env->i64Const(numElements));

                    // @ Todo: index protection (out of bounds?)
                    // store the elements into the array
                    FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env,
                                                                          builder,
                                                                          value.val,
                                                                          sub->_value->getInferredType());

                    std::vector<SerializableValue> elements;
                    std::vector<llvm::Type *> elementTypes;
                    for (int i = 0; i < numElements; ++i) {
                        auto load = ft.getLoad(builder, {i});
                        elements.push_back(load);
                        elementTypes.push_back(load.val->getType());
                    }

                    // fill in array elements
                    for (int i = 0; i < numElements; ++i) {
                        builder.CreateStore(elements[i].val, builder.CreateGEP(array, {i32Const(i)}));
                        builder.CreateStore(elements[i].size, builder.CreateGEP(sizes, {i32Const(i)}));
                    }

                    // load from array
                    auto retVal = builder.CreateLoad(builder.CreateGEP(array, {builder.CreateTrunc(index.val,
                                                                                                   llvm::Type::getInt32Ty(
                                                                                                           context))}));
                    auto retSize = builder.CreateLoad(builder.CreateGEP(sizes, {builder.CreateTrunc(index.val,
                                                                                                    llvm::Type::getInt32Ty(
                                                                                                            context))}));

                    // @TODO: null value for this case here!

                    addInstruction(retVal, retSize);
                    return;
                } else {
                    // case 3: give error
                    addInstruction(logErrorV(
                            "Can't compile index operator, need to run code via interpreter. Index expressions must be "
                            "either compile time static or the tuple to be indexed comprised of elements with identic type."));

                    return;
                }

            } else if (value.val->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0)
                       && value_type == python::Type::STRING
                       && sub->getInferredType() == python::Type::STRING) {

                auto strlength = builder.CreateSub(value.size, _env->i64Const(1));

                // correct for negative indices (once)
                auto cmp = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, index.val, _env->i64Const(0));
                index = SerializableValue(builder.CreateSelect(cmp,
                                                               builder.CreateAdd(index.val,
                                                                                 strlength),
                                                               index.val),
                                          index.size);


                // @TODO: this here throws an exception... --> correct! but then weird things happen?

                // first perform index check, if fails --> exception!
                auto indexcmp = _env->indexCheck(builder, index.val, strlength);
                _lfb->addException(builder, ExceptionCode::INDEXERROR, _env->i1neg(builder, indexcmp));

                // normal code goes on (builder variable has been updated)
                // copy out one char string here
                auto newstr = builder.CreatePointerCast(_env->malloc(builder, _env->i64Const(2)),
                                                        llvm::Type::getInt8PtrTy(context,
                                                                                 0)); // indexing string will return one char string!
                // do via load & store, no need for memcpy here yet
                auto charAtIndex = builder.CreateLoad(builder.CreateGEP(value.val, index.val));
                assert(charAtIndex->getType() == llvm::Type::getInt8Ty(context));

                // store charAtIndex at ptr
                builder.CreateStore(charAtIndex, newstr);
                builder.CreateStore(_env->i8Const(0), builder.CreateGEP(newstr, _env->i32Const(1)));

                // add serializedValue
                addInstruction(newstr, _env->i64Const(2));

            } else if (value.val->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0) &&
                       value_type == python::Type::GENERICDICT) {
                // throw error for genericdict
                std::stringstream ss;
                ss << "Can't subscript generic dictionaries (can't type).";
                ss << "\nindex type: " << sub->_expression->getInferredType().desc();
                ss << "\nvalue type: " << sub->_value->getInferredType().desc();
                ss << "\nindex llvm type: " << _env->getLLVMTypeName(index.val->getType());
                ss << "\nvalue llvm type: " << _env->getLLVMTypeName(value.val->getType());
                error(ss.str());
            } else if (value.val->getType() == llvm::Type::getInt8PtrTy(_env->getContext(), 0) &&
                       value_type.isDictionaryType()) {
                auto subval = subscriptCJSONDictionary(sub, index, index_type, value);
                addInstruction(subval.val, subval.size);
            } else if(value_type.isListType()) {
                if(value_type == python::Type::EMPTYLIST) {
                    _lfb->addException(builder, ExceptionCode::INDEXERROR, _env->i1Const(true));
                    addInstruction(nullptr, nullptr);
                } else {
                    auto elementType = value_type.elementType();
                    if(elementType.isSingleValued()) {
                        auto indexcmp = _env->indexCheck(builder, index.val, value.val);
                        _lfb->addException(builder, ExceptionCode::INDEXERROR, _env->i1neg(builder, indexcmp)); // error if index out of bounds
                        if(elementType == python::Type::NULLVALUE) {
                            addInstruction(nullptr, nullptr, _env->i1Const(true));
                        } else if(elementType == python::Type::EMPTYTUPLE) {
                            auto alloc = builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr);
                            auto load = builder.CreateLoad(alloc);
                            addInstruction(load, _env->i64Const(sizeof(int64_t)));
                        } else if(elementType == python::Type::EMPTYDICT || elementType == python::Type::EMPTYLIST) {
                            addInstruction(nullptr, nullptr); // TODO: may want to actually construct an empty dictionary, look at LambdaFunction.cc::addReturn, in the !res case
                        }
                    } else {
                        auto num_elements = builder.CreateExtractValue(value.val, {1});

                        // correct for negative indices (once)
                        auto cmp = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, index.val, _env->i64Const(0));
                        index = SerializableValue(builder.CreateSelect(cmp, builder.CreateAdd(index.val, num_elements), index.val), index.size);

                        // first perform index check, if it fails --> exception!
                        auto indexcmp = _env->indexCheck(builder, index.val, num_elements);
                        _lfb->addException(builder, ExceptionCode::INDEXERROR, _env->i1neg(builder, indexcmp));

                        // get the element
                        auto subval = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(value.val, 2), index.val));
                        llvm::Value* subsize = _env->i64Const(sizeof(int64_t)); // TODO: is this 8 for boolean as well?
                        if(elementType == python::Type::STRING) {
                            subsize = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(value.val, 3), index.val));
                        }

                        addInstruction(subval, subsize);
                    }
                }
            } else if (value.val->getType() == _env->getMatchObjectPtrType() &&
                       value_type == python::Type::MATCHOBJECT) {
                auto ovector = builder.CreateLoad(builder.CreateGEP(value.val, {_env->i32Const(0), _env->i32Const(0)}));
                auto subject = builder.CreateLoad(builder.CreateGEP(value.val, {_env->i32Const(0), _env->i32Const(1)}));
                auto subject_len = builder.CreateLoad(builder.CreateGEP(value.val, {_env->i32Const(0), _env->i32Const(2)}));

                // TODO: add some boundary checking here, probably with _env->indexCheck (remember that 0 is a valid choice)
                auto ind = builder.CreateMul(_env->i64Const(2), index.val);
                auto start = builder.CreateLoad(llvm::Type::getInt64Ty(_env->getContext()), builder.CreateGEP(ovector, ind));
                auto end = builder.CreateLoad(llvm::Type::getInt64Ty(_env->getContext()), builder.CreateGEP(ovector, builder.CreateAdd(ind, _env->i64Const(1))));

                auto ret = stringSliceInst({subject, subject_len}, start, end, _env->i64Const(1));
                addInstruction(ret.val, ret.size);
            }
            else {
                // undefined
                std::stringstream ss;
                ss << "unsupported type encountered with [] operator.";
                ss << "\nindex type: " << sub->_expression->getInferredType().desc();
                ss << "\nvalue type: " << sub->_value->getInferredType().desc();
                ss << "\nindex llvm type: " << _env->getLLVMTypeName(index.val->getType());
                ss << "\nvalue llvm type: " << _env->getLLVMTypeName(value.val->getType());
                error(ss.str());
            }
        }


        SerializableValue
        BlockGeneratorVisitor::CreateDummyValue(llvm::IRBuilder<> &builder, const python::Type &type) {
            // dummy value needs to be created for llvm to combine stuff.
            SerializableValue retVal;
            if (python::Type::BOOLEAN == type || python::Type::I64 == type) {
                retVal.val = _env->i64Const(0);
                retVal.size = _env->i64Const(sizeof(int64_t));
            } else if (python::Type::F64 == type) {
                retVal.val = _env->f64Const(0.0);
                retVal.size = _env->i64Const(sizeof(double));
            } else if (python::Type::STRING == type || type.isDictionaryType()) {
                retVal.val = _env->i8ptrConst(nullptr);
                retVal.size = _env->i64Const(0);
            } else if (type.isListType()) {
                auto llvmType = _env->getListType(type);
                auto val = _env->CreateFirstBlockAlloca(builder, llvmType);
                if (type == python::Type::EMPTYLIST) {
                    builder.CreateStore(_env->i8nullptr(), val);
                } else {
                    auto elementType = type.elementType();
                    if (elementType.isSingleValued()) {
                        builder.CreateStore(_env->i64Const(0), val);
                    } else {
                        builder.CreateStore(_env->i64Const(0), _env->CreateStructGEP(builder, val, 0));
                        builder.CreateStore(_env->i64Const(0), _env->CreateStructGEP(builder, val, 1));
                        builder.CreateStore(llvm::ConstantPointerNull::get(
                                llvm::dyn_cast<PointerType>(llvmType->getStructElementType(2))),
                                            _env->CreateStructGEP(builder, val, 2));
                        if (elementType == python::Type::STRING) {
                            builder.CreateStore(llvm::ConstantPointerNull::get(
                                    llvm::dyn_cast<PointerType>(llvmType->getStructElementType(3))),
                                                _env->CreateStructGEP(builder, val, 3));
                        }
                    }
                }
                retVal.val = builder.CreateLoad(val);
                retVal.size = _env->i64Const(3 * sizeof(int64_t));
            }
            return retVal;
        }

        SerializableValue BlockGeneratorVisitor::upCastReturnType(llvm::IRBuilder<>& builder, const SerializableValue &val,
                                                                  const python::Type &type,
                                                                  const python::Type &targetType) {
            if(!canUpcastType(type, targetType))
                throw std::runtime_error("types " + type.desc() + " and " + targetType.desc() + " not compatible for upcasting");

            if(type == targetType)
                return val;

            // the types are different

            // primitives
            // bool -> int -> f64
            if(type == python::Type::BOOLEAN) {
                if(targetType == python::Type::I64) {
                    // widen to i64
                    return SerializableValue(upCast(builder, val.val, _env->i64Type()), _env->i64Const(sizeof(int64_t)));
                }
                if(targetType == python::Type::F64)
                    // widen to f64
                    return SerializableValue(upCast(builder, val.val, _env->doubleType()), _env->i64Const(sizeof(int64_t)));
            }
            // int -> f64
            if(type == python::Type::I64 && targetType == python::Type::F64)
                return SerializableValue(upCast(builder, val.val, _env->doubleType()), _env->i64Const(sizeof(int64_t)));

            // null to Option[Any]
            if(type == python::Type::NULLVALUE && targetType.isOptionType()) {

                // need to create dummy value so LLVM works...
                auto baseType = targetType.getReturnType();
                auto tmp = CreateDummyValue(builder, baseType);
                return SerializableValue(tmp.val, tmp.size, _env->i1Const(true));
            }

            // primitive to option?
            if(!type.isOptionType() && targetType.isOptionType()) {
                auto tmp = upCastReturnType(builder, val, type, targetType.withoutOptions());
                return SerializableValue(tmp.val, tmp.size, _env->i1Const(false));
            }

            // option[a] to option[b]?
            if(type.isOptionType() && targetType.isOptionType()) {
                auto tmp = upCastReturnType(builder, val, type.withoutOptions(), targetType.withoutOptions());
                return SerializableValue(tmp.val, tmp.size, val.is_null);
            }

            if(type.isTupleType() && targetType.isTupleType()) {
                if(type.parameters().size() != targetType.parameters().size()) {
                    error("upcasting tuple type " + type.desc() + " to tuple type " + targetType.desc()
                    + " not possible, because tuples contain different amount of parameters!");
                }
                // upcast tuples
                // Load as FlattenedTuple
                FlattenedTuple val_tuple = FlattenedTuple::fromLLVMStructVal(_env, builder, val.val, type);
                FlattenedTuple target_tuple(_env);
                target_tuple.init(targetType);

                auto num_elements = type.parameters().size();

                // put to flattenedtuple (incl. assigning tuples!)
                for (int i = 0; i < num_elements; ++i) {
                    // retrieve from tuple itself and then upcast!
                    auto el_type = val_tuple.fieldType(i);
                    auto el_target_type = target_tuple.fieldType(i);
                    SerializableValue el(val_tuple.get(i), val_tuple.getSize(i), val_tuple.getIsNull(i));
                    auto el_target = upCastReturnType(builder, el, el_type, el_target_type);
                    target_tuple.setElement(builder, i, el_target.val, el_target.size, el_target.is_null);
                }

                // get loadable struct type
                auto ret = target_tuple.getLoad(builder);
                assert(ret->getType()->isStructTy());
                auto size = target_tuple.getSize(builder);
                addInstruction(ret, size);

                return SerializableValue(ret, size);
            }

            // @TODO: Issue https://github.com/LeonhardFS/Tuplex/issues/214
            // @TODO: List[a] to List[b] if a,b are compatible?
            // @TODO: Dict[a, b] to Dict[c, d] if upcast a to c works, and upcast b to d works?
            if(type.isListType() && targetType.isListType()) {
                // @TODO:
                error("upcasting list type " + type.desc() + " to list type " + targetType.desc() + " not yet implemented");
            }

            if(type.isDictionaryType() && targetType.isDictionaryType()) {
                error("upcasting dict type " + type.desc() + " to dict type " + targetType.desc() + " not yet implemented");
            }

            error("can not generate code to upcast " + type.desc() + " to " + targetType.desc());
            return val;
        }

        void BlockGeneratorVisitor::visit(NReturn *ret) {
            if(earlyExit())return;
            auto num_stack_entries = _blockStack.size();

            // visit expression
            ApatheticVisitor::visit(ret);

            // did expression leave early? then restore stack to before and skip statement!
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_entries)
                    _blockStack.pop_back();
                return;
            }

            assert(_blockStack.size() > 0);
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            SerializableValue retVal;
            if(ret->_expression) {
                retVal = _blockStack.back();
                _blockStack.pop_back();
            } else {
                retVal = SerializableValue(nullptr, nullptr, _env->i1Const(true)); // NULL/None
            }


            // speculate if return types do not match.
            // => function should have one globally accepted return type assigned!
            auto funcReturnType = _lfb->pythonRetType();

            // check whether type can be upcasted
            auto expression_type = ret->_expression ? ret->_expression->getInferredType() : python::Type::NULLVALUE; // return is None!
            auto target_type = ret->getInferredType();

            if(target_type == funcReturnType) {
                // ok, fits the globally agreed function return type!

                // the retval popped could need extension to an option type!
                retVal = upCastReturnType(builder, retVal, expression_type, target_type);

                // this adds a retValue
                _lfb->addReturn(retVal);
            } else {

                _logger.debug("added normalcase speculation on return type " + target_type.desc() + ".");

                // normal case violation.
                _lfb->exitNormalCase(); // NOTE: this only works in a statement, not on an expression.
            }
        }

        void BlockGeneratorVisitor::visit(NCall *call) {
            if(earlyExit())return;

            // note: also expression like (lambda x: x * x)(8) should work eventually.
            // Hence, it is a good idea to return function objects...

            //TODO: Change this -> basically do the following:
            // not call on the identifier, rather use the full information
            // to create a call based on the FunctionRegistry
            // this is more efficient often.

            // visit only arguments.
            // only need type
            auto num_stack_before = _blockStack.size();

            ApatheticVisitor::visit(call);

            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // early end expression
            }


            // _func should have yields all the parameters
            assert(_blockStack.size() >= 1 + call->_positionalArguments.size());

            // the values
            SerializableValue funcVal;
            SerializableValue caller;
            std::vector<SerializableValue> args;

            funcVal = _blockStack.back();
            _blockStack.pop_back();

            // only get caller if it is an attribute call
            if (!funcVal.val && call->_func->type() == ASTNodeType::Attribute) {
                caller = _blockStack.back();
                _blockStack.pop_back();
            }

            // pop from stack
            // pop as many args as required
            for (int i = 0; i < call->_positionalArguments.size(); ++i) {
                args.emplace_back(_blockStack.back());
                _blockStack.pop_back();
            }
            // reverse args vector (it's a stack!!!)
            std::reverse(args.begin(), args.end());

            // perform call
            // check what result function yielded
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            SerializableValue ret;
            assert(call->_func->getInferredType().isFunctionType());
            auto argsType = call->_func->getInferredType().getParamsType();
            auto retType = call->_func->getInferredType().getReturnType();

            // special case: if generic tuple, fill out argsType manually
            assert(argsType.isTupleType() || argsType == python::Type::GENERICTUPLE);
            if (python::Type::GENERICTUPLE == argsType) {
                std::vector<python::Type> atv;
                for (auto arg : call->_positionalArguments)
                    atv.emplace_back(arg->getInferredType());
                argsType = python::Type::makeTupleType(atv);
            }

            // add NULL exception if function does expect non-null argument!
            auto numArgs = call->_positionalArguments.size();
            assert(argsType.isTupleType());
            assert(argsType.parameters().size() == numArgs);

            for (int i = 0; i < numArgs; ++i) {
                auto argType = argsType.parameters()[i];
                auto type = call->_positionalArguments[i]->getInferredType();
                // null check? or propagation?
                if (!type.isOptionType() && argType ==
                    python::Type::makeOptionType(type)) {
                    // required is option => propagate
                    // note: not necessary for NULL VALUE...
                    if(!args[i].is_null)
                        args[i].is_null = _env->i1Const(false); // is not null
                } else if (!argType.isOptionType() && python::Type::makeOptionType(argType) ==
                           type) {
                    // null check, exception required
                    if(args[i].is_null) {
                        _lfb->addException(builder, ExceptionCode::TYPEERROR, args[i].is_null);
                    }
                } else if(!python::canUpcastType(type, argType)) { // if option select doesn't work and neither upcasting, error.
                    // @TODO: what about downcasting? I.e. when function needs bool but i64 is given? try downcast option!
                    _lfb->exitWithException(ExceptionCode::TYPEERROR);
//                    _lfb->addException(builder, ExceptionCode::TYPEERROR, _env->i1Const(true));
//
//                    // => could quit generation now basically. For this, need mechanism to stop generation in expressions
//                    //    when fatal error occured. @TODO
//
//                    ret = SerializableValue(); // default, to continue codegen.
//                    addInstruction(ret.val, ret.size, ret.is_null);
                    return;
                }
            }

            // there are now multiple options:
            // either, directly a function object was given (i.e. generated function OR a nullptr, meaning a direct call can be performed)
            if (funcVal.val) {
                error("no support for call to def'ed functions yet");
                return;
            } else {
                // make indirect call (faster, saves inlining for simple functions)

                // check first what type there is:
                // i.e. could be call on func or attribute func
                if (call->_func->type() == ASTNodeType::Identifier) {
                    // direct call
                    std::string funcName = ((NIdentifier *) call->_func)->_name;
                    auto& ann = ((NIdentifier *) call->_func)->annotation();
                    if(ann.symbol)
                        funcName = ann.symbol->fullyQualifiedName(); // need this to lookup module entries!

                    // this creates a general call. Might create an exception block within the current function!
                    // handle iterator-related calls separately since an additional argument iteratorInfo is needed
                    if(call->hasAnnotation() && call->annotation().iteratorInfo) {
                        ret = _functionRegistry->createIteratorRelatedSymbolCall(*_lfb, builder, funcName, argsType, retType, args, call->annotation().iteratorInfo);
                    } else {
                        ret = _functionRegistry->createGlobalSymbolCall(*_lfb, builder, funcName, argsType, retType, args);
                    }

                    // e.g., len(...) won't cause an exception, hence it is very fast...
                    // however, calling int(...), float(...) causes an exception potentially (i.e. parse error)

                } else if (call->_func->type() == ASTNodeType::Attribute) {

                    NAttribute *attr = (NAttribute *) call->_func;
                    std::string attrName = attr->_attribute->_name;

                    auto callerType = attr->_value->getInferredType();

                    // null checks
                    // for None in python
                    // only internal functions work:
                    // None.__bool__(           None.__format__(         None.__init_subclass__(  None.__reduce_ex__(
                    //None.__class__(          None.__ge__(             None.__le__(             None.__repr__(
                    //None.__delattr__(        None.__getattribute__(   None.__lt__(             None.__setattr__(
                    //None.__dir__(            None.__gt__(             None.__ne__(             None.__sizeof__(
                    //None.__doc__             None.__hash__(           None.__new__(            None.__str__(
                    //None.__eq__(             None.__init__(           None.__reduce__(         None.__subclasshook__(
                    // i.e. operators...
                    // ==> if NULLVALUE, always exception!
                    // ==> if option type, perform null check!
                    // why? because there is no attribute on None defined anyways
                    // ==> will throw attribute error

                    if(callerType.isOptionType()) {
                        // null check
                        _lfb->addException(builder, ExceptionCode::ATTRIBUTEERROR, caller.is_null);
                        ret = _functionRegistry->createAttributeCall(*_lfb, builder, attrName, callerType, argsType,
                                                                     retType, SerializableValue(caller.val, caller.size), args);
                    } else if(callerType == python::Type::NULLVALUE) {
                        _lfb->addException(builder, ExceptionCode::ATTRIBUTEERROR, _env->i1Const(true));
                        ret = SerializableValue(); // default, to continue codegen.
                    } else if(callerType == python::Type::MODULE) {
                        auto& ann = attr->_attribute->annotation();
                        assert(ann.symbol);
                        auto name = ann.symbol->fullyQualifiedName(); // what to call
                        ret = _functionRegistry->createGlobalSymbolCall(*_lfb, builder, name, argsType, retType, args);
                    } else {
                        // regular call to an instance of an object, i.e. builtin objects like int, bool, float, ...
                        assert(!caller.is_null);
                        ret = _functionRegistry->createAttributeCall(*_lfb, builder, attrName, callerType, argsType,
                                                                     retType, caller, args);
                    }
                } else {
                    // error
                    error("unknown call on AST Node of type " + std::to_string((int) call->_func->type()) +
                          "encountered");
                }
            }

            if (ret.val == nullptr && retType != python::Type::EMPTYLIST) {
                std::string funcName = "<anon-fun>";
                if (call->_func->type() == ASTNodeType::Function)
                    funcName = ((NFunction *) call->_func)->_name->_name;
                if (call->_func->type() == ASTNodeType::Identifier)
                    funcName = ((NIdentifier *) call->_func)->_name;
                error("code generation for call to " + funcName + " failed");
            }

            // make sure ret is valid!
            assert(ret.val || ret.is_null || retType == python::Type::EMPTYLIST);

            addInstruction(ret.val, ret.size, ret.is_null);
        }

        void BlockGeneratorVisitor::visit(NSlice *slice) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();

            // Note: in python subscripting with None is totally fine. Why...
            // visit children
            ApatheticVisitor::visit(slice);

            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // early end expression
            }

            auto &context = _env->getContext();
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            assert(slice->_slices.front()->type() == ASTNodeType::SliceItem);
            auto sliceItem = (NSliceItem *) slice->_slices.front();

            SerializableValue value, start, end, stride, res;

            // read slice items off the stack, validate + clean values
            if (sliceItem->_stride) {
                auto itype = sliceItem->_stride->getInferredType();
                if (itype == python::Type::I64 || itype == python::Type::BOOLEAN) {
                    assert(_blockStack.size() >= 1);

                    stride = _blockStack.back();
                    _blockStack.pop_back();
                } else {
                    error("stride for slice must be integer or bool");
                }
            } else stride = SerializableValue(_env->i64Const(1), nullptr);

            if (sliceItem->_end) {
                auto itype = sliceItem->_end->getInferredType();
                if (itype == python::Type::I64 || itype == python::Type::BOOLEAN) {
                    assert(_blockStack.size() >= 1);
                    end = _blockStack.back();
                    _blockStack.pop_back();
                } else {
                    error("end index for slice must be integer or bool");
                }
            } else end = SerializableValue(nullptr, nullptr);

            if (sliceItem->_start) {
                auto itype = sliceItem->_start->getInferredType();
                if (itype == python::Type::I64 || itype == python::Type::BOOLEAN) {
                    assert(_blockStack.size() >= 1);
                    start = _blockStack.back();
                    _blockStack.pop_back();
                } else {
                    error("start index for slice must be integer or bool");
                }
            } else start = SerializableValue(nullptr, nullptr);
            // read in value
            // value for subscript needs to be valid, i.e. perform null check!
            value = popWithNullCheck(builder, ExceptionCode::TYPEERROR, "'NoneType' object is not subscriptable");

            // get rid off option here
            if (slice->_value->getInferredType().withoutOptions() == python::Type::STRING) {
                assert(value.val->getType() == _env->i8ptrType());

                stride.val = upCast(builder, stride.val, _env->i64Type());
                if (sliceItem->_end) end.val = upCast(builder, end.val, _env->i64Type());
                if (sliceItem->_start) start.val = upCast(builder, start.val, _env->i64Type());
                // do the slice
                res = stringSliceInst(value, start.val, end.val, stride.val);
            } else if (slice->_value->getInferredType().isTupleType()) {
                // only support static slices
                if ((!sliceItem->_start || isStaticValue(sliceItem->_start, true))
                    && (!sliceItem->_end || isStaticValue(sliceItem->_end, true))
                    && (!sliceItem->_stride || isStaticValue(sliceItem->_stride, true))) {
                    res = tupleStaticSliceInst(slice->_value, sliceItem->_start, sliceItem->_end, sliceItem->_stride,
                                               value, start.val, end.val, stride.val);
                } else {
                    error("We do not currently support slicing tuples with non-static expressions");
                }
            } else {
                error("TypeError: unsupported operand type for slicing: '" + slice->_value->getInferredType().desc() +
                      "'");
            }
            addInstruction(res.val, res.size); // always valid result
        }

        SerializableValue BlockGeneratorVisitor::stringSliceInst(const SerializableValue &value, llvm::Value *start,
                                                                 llvm::Value *end, llvm::Value *stride) {
            // assume all Values are i64Const: UpCast in caller
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            auto positiveStrideBlk = BasicBlock::Create(_env->getContext(), "positivestride",
                                                        builder.GetInsertBlock()->getParent());
            auto positiveStrideBlk1 = BasicBlock::Create(_env->getContext(), "positivestride1",
                                                         builder.GetInsertBlock()->getParent());
            auto positiveStrideSpecial = BasicBlock::Create(_env->getContext(), "positivestridespecial",
                                                            builder.GetInsertBlock()->getParent());
            auto negativeStrideBlk = BasicBlock::Create(_env->getContext(), "negativestride",
                                                        builder.GetInsertBlock()->getParent());
            auto validRangeBlk = BasicBlock::Create(_env->getContext(), "validrange",
                                                    builder.GetInsertBlock()->getParent());
            auto loopBlock = BasicBlock::Create(_env->getContext(), "loopblock", builder.GetInsertBlock()->getParent());
            auto loopEntryBlock = BasicBlock::Create(_env->getContext(), "loopentryblock",
                                                     builder.GetInsertBlock()->getParent());
            auto emptyBlock = BasicBlock::Create(_env->getContext(), "emptystr", builder.GetInsertBlock()->getParent());
            auto retBlock = BasicBlock::Create(_env->getContext(), "retblock", builder.GetInsertBlock()->getParent());

            auto stringLen = builder.CreateSub(value.size, _env->i64Const(1));
            // local variables
            auto retval = builder.CreateAlloca(_env->i8ptrType(), 0, nullptr);
            auto retsize = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
            auto startpos = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
            auto endpos = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
            auto looppos = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);
            auto newstrpos = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);

            if (!_allowUndefinedBehaviour) { // zero stride isn't allowed
                auto strideIsZero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ, stride, _env->i64Const(0));
                _lfb->addException(builder, ExceptionCode::VALUEERROR, strideIsZero);
            }

            // branch based on whether stride is positive or negative
            auto strideIsPositive = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SGT, stride, _env->i64Const(0));
            builder.CreateCondBr(strideIsPositive, positiveStrideBlk, negativeStrideBlk);

            // positive stride
            builder.SetInsertPoint(positiveStrideBlk);
            // rescale first
            if (!start) builder.CreateStore(_env->i64Const(0), startpos);
            else builder.CreateStore(processSliceIndex(builder, start, stringLen, stride), startpos);
            if (!end) builder.CreateStore(stringLen, endpos);
            else builder.CreateStore(processSliceIndex(builder, end, stringLen, stride), endpos);

            // check if start < end; else, return empty
            auto nonemptyResPos = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, builder.CreateLoad(startpos),
                                                     builder.CreateLoad(endpos));
            builder.CreateCondBr(nonemptyResPos, positiveStrideBlk1, emptyBlock);

            // fall through block for previous branch
            builder.SetInsertPoint(positiveStrideBlk1);
            // special case: [x::1]
            auto strideIsOne = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ, stride, _env->i64Const(1));
            auto endIsStringLenPos = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ, builder.CreateLoad(endpos),
                                                        stringLen);
            auto positiveSpecialCase = builder.CreateAnd(strideIsOne, endIsStringLenPos);
            builder.CreateCondBr(positiveSpecialCase, positiveStrideSpecial, validRangeBlk);

            // positive stride, special case
            builder.SetInsertPoint(positiveStrideSpecial);
            builder.CreateStore(builder.CreateGEP(value.val, builder.CreateLoad(startpos)), retval);
            builder.CreateStore(builder.CreateSub(value.size, builder.CreateLoad(startpos)), retsize);
            builder.CreateBr(retBlock);

            // negative stride
            builder.SetInsertPoint(negativeStrideBlk);
            // rescale first
            if (!start) builder.CreateStore(builder.CreateSub(stringLen, _env->i64Const(1)), startpos);
            else builder.CreateStore(processSliceIndex(builder, start, stringLen, stride), startpos);
            if (!end) builder.CreateStore(_env->i64Const(-1), endpos); // SPECIAL, allow negative
            else builder.CreateStore(processSliceIndex(builder, end, stringLen, stride), endpos);

            // check if start > end; else, return empty
            auto nonemptyResNeg = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SGT, builder.CreateLoad(startpos),
                                                     builder.CreateLoad(endpos));
            builder.CreateCondBr(nonemptyResNeg, validRangeBlk, emptyBlock);

            // valid range, do the loop
            builder.SetInsertPoint(validRangeBlk);
            // newstrlen = ceiling(end-start/stride)
            auto diff = builder.CreateSub(builder.CreateLoad(endpos), builder.CreateLoad(startpos));
            auto newstrlen = _env->floorDivision(builder, diff, stride);
            auto hasnorem = builder.CreateICmpEQ(builder.CreateSRem(diff, stride), _env->i64Const(0));
            newstrlen = builder.CreateSelect(hasnorem, newstrlen, builder.CreateAdd(newstrlen, _env->i64Const(1)));
            auto newlen = builder.CreateAdd(newstrlen, _env->i64Const(1));
            auto allocmem = _env->malloc(builder, newlen); // allocate memory
            builder.CreateStore(_env->i8Const('\0'), builder.CreateGEP(builder.getInt8Ty(),
                                                                       allocmem,
                                                                       newstrlen)); // null terminate the result
            builder.CreateStore(newlen, retsize); // save resulting size
            builder.CreateStore(allocmem, retval); // save resulting pointer
            builder.CreateStore(builder.CreateLoad(startpos), looppos); // start loop
            builder.CreateStore(_env->i64Const(0), newstrpos);
            builder.CreateBr(loopEntryBlock);

            // loop entry block
            builder.SetInsertPoint(loopEntryBlock);
            auto enterloop = builder.CreateSelect(
                    strideIsPositive,
                    builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, builder.CreateLoad(looppos),
                                       builder.CreateLoad(endpos)),
                    builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SGT, builder.CreateLoad(looppos),
                                       builder.CreateLoad(endpos)));
            builder.CreateCondBr(enterloop, loopBlock, retBlock);

            // loop block
            builder.SetInsertPoint(loopBlock);
            auto newstrposval = builder.CreateLoad(newstrpos);
            auto loopposval = builder.CreateLoad(looppos);
            auto charptr = builder.CreateGEP(builder.getInt8Ty(), value.val, loopposval);
            builder.CreateStore(builder.CreateLoad(charptr),
                                builder.CreateGEP(builder.getInt8Ty(), allocmem, newstrposval));
            builder.CreateStore(builder.CreateAdd(newstrposval, _env->i64Const(1)), newstrpos);
            builder.CreateStore(builder.CreateAdd(loopposval, stride), looppos);
            builder.CreateBr(loopEntryBlock);

            // empty return string
            builder.SetInsertPoint(emptyBlock);
            auto emptystr = _env->malloc(builder, _env->i64Const(1)); // make null terminated empty string
            builder.CreateStore(_env->i8Const('\0'), emptystr);
            builder.CreateStore(emptystr, retval); // save result in ret local vars
            builder.CreateStore(_env->i64Const(1), retsize);
            builder.CreateBr(retBlock); // jump to return block

            // Overall Return Block (from lambda function)
            builder.SetInsertPoint(retBlock);
            auto ret = SerializableValue(builder.CreateLoad(retval), builder.CreateLoad(retsize));
            _lfb->setLastBlock(retBlock);
            return ret;
        }

        llvm::Value *
        BlockGeneratorVisitor::processSliceIndex(IRBuilder<> &builder, llvm::Value *index, llvm::Value *len,
                                                 llvm::Value *stride) {
            // case 1: (-inf, -stringLen) => 0 // for negative stride, goes to -1
            // case 2: [-stringLen, -1] => +stringLen
            // case 3: [0, stringlen-1] => +0
            // case 4: [stringLen, inf) => stringLen // for negative stride, goes to stringLen-1
            auto block1 = BasicBlock::Create(_env->getContext(), "case 1", builder.GetInsertBlock()->getParent());
            auto block1pos = BasicBlock::Create(_env->getContext(), "case 1, positive stride",
                                                builder.GetInsertBlock()->getParent());
            auto block1neg = BasicBlock::Create(_env->getContext(), "case 1, negative stride",
                                                builder.GetInsertBlock()->getParent());
            auto notblock1 = BasicBlock::Create(_env->getContext(), "not case 1",
                                                builder.GetInsertBlock()->getParent());
            auto block2 = BasicBlock::Create(_env->getContext(), "case 2", builder.GetInsertBlock()->getParent());
            auto notblock2 = BasicBlock::Create(_env->getContext(), "not case 2",
                                                builder.GetInsertBlock()->getParent());
            auto block3 = BasicBlock::Create(_env->getContext(), "case 3", builder.GetInsertBlock()->getParent());
            auto block4 = BasicBlock::Create(_env->getContext(), "case 4", builder.GetInsertBlock()->getParent());
            auto block4pos = BasicBlock::Create(_env->getContext(), "case 4, positive stride",
                                                builder.GetInsertBlock()->getParent());
            auto block4neg = BasicBlock::Create(_env->getContext(), "case 4, negative stride",
                                                builder.GetInsertBlock()->getParent());
            auto retBlock = BasicBlock::Create(_env->getContext(), "retblock", builder.GetInsertBlock()->getParent());

            auto ret = builder.CreateAlloca(builder.getInt64Ty(), 0, nullptr);

            auto case1 = builder.CreateICmp(llvm::ICmpInst::Predicate::ICMP_SLT, index,
                                            builder.CreateMul(len, _env->i64Const(-1)));
            auto case2 = builder.CreateICmp(llvm::ICmpInst::Predicate::ICMP_SLE, index, _env->i64Const(-1));
            auto case3 = builder.CreateICmp(llvm::ICmpInst::Predicate::ICMP_SLT, index, len);
            auto posstride = builder.CreateICmp(llvm::ICmpInst::Predicate::ICMP_SGT, stride, _env->i64Const(0));

            builder.CreateCondBr(case1, block1, notblock1);

            builder.SetInsertPoint(block1);
            builder.CreateCondBr(posstride, block1pos, block1neg);

            builder.SetInsertPoint(block1pos);
            builder.CreateStore(_env->i64Const(0), ret);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(block1neg);
            builder.CreateStore(_env->i64Const(-1), ret);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(notblock1);
            builder.CreateCondBr(case2, block2, notblock2);

            builder.SetInsertPoint(block2);
            builder.CreateStore(builder.CreateAdd(index, len), ret);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(notblock2);
            builder.CreateCondBr(case3, block3, block4);

            builder.SetInsertPoint(block3);
            builder.CreateStore(index, ret);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(block4);
            builder.CreateCondBr(posstride, block4pos, block4neg);

            builder.SetInsertPoint(block4pos);
            builder.CreateStore(builder.CreateSub(len, _env->i64Const(0)), ret);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(block4neg);
            builder.CreateStore(builder.CreateSub(len, _env->i64Const(1)), ret);
            builder.CreateBr(retBlock);

            builder.SetInsertPoint(retBlock);
            auto retval = builder.CreateLoad(ret);
            return retval;
        }

        SerializableValue BlockGeneratorVisitor::tupleStaticSliceInst(ASTNode *tuple_node, ASTNode *start_node,
                                                                      ASTNode *end_node, ASTNode *stride_node,
                                                                      const SerializableValue &tuple,
                                                                      llvm::Value *start,
                                                                      llvm::Value *end, llvm::Value *stride) {
            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            if ((!start_node || start_node->type() == ASTNodeType::Number || start_node->type() == ASTNodeType::Boolean)
                && (!end_node || end_node->type() == ASTNodeType::Number || end_node->type() == ASTNodeType::Boolean)
                && (!stride_node || stride_node->type() == ASTNodeType::Number ||
                    stride_node->type() == ASTNodeType::Boolean)) {
                int startpos = 0, endpos = 0, strideval = 0, size = 0;

                if (!_allowUndefinedBehaviour) {
                    auto strideIsZero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_EQ, stride,
                                                           _env->i64Const(0));
                    _lfb->addException(builder, ExceptionCode::VALUEERROR, strideIsZero);
                }

                // get size of top level tuple
                size = (int) tuple_node->getInferredType().parameters().size();

                // get value of stride
                if (!stride_node) strideval = 1;
                else if (stride_node->type() == ASTNodeType::Number)
                    strideval = (int) static_cast<NNumber *>(stride_node)->getI64();
                else strideval = static_cast<NBoolean *>(stride_node)->_value;

                // correct start/end values
                // case 1: (-inf, -len) => 0 // for negative stride, goes to -1
                // case 2: [-len, -1] => +len
                // case 3: [0, len-1] => +0
                // case 4: [len, inf) => len // for negative stride, goes to len-1

                // get value of start
                if (start_node) {
                    // get value
                    if (start_node->type() == ASTNodeType::Number)
                        startpos = (int) static_cast<NNumber *>(start_node)->getI64();
                    else startpos = static_cast<NBoolean *>(start_node)->_value;

                    // correct value
                    if (startpos < -size) {
                        if (strideval < 0) startpos = -1;
                        else startpos = 0;
                    } else if (startpos <= -1) startpos += size;
                    else if (startpos >= size) {
                        if (strideval < 0) startpos = size - 1;
                        else startpos = size;
                    }
                } else {
                    if (strideval < 0) startpos = size - 1;
                    else startpos = 0;
                }

                // get value of end
                if (end_node) {
                    // get value
                    if (end_node->type() == ASTNodeType::Number)
                        endpos = (int) static_cast<NNumber *>(end_node)->getI64();
                    else endpos = static_cast<NBoolean *>(end_node)->_value;

                    // correct value
                    if (endpos < -size) {
                        if (strideval < 0) endpos = -1;
                        else endpos = 0;
                    } else if (endpos <= -1) endpos += size;
                    else if (endpos >= size) {
                        if (strideval < 0) endpos = size - 1;
                        else endpos = size;
                    }
                } else {
                    if (strideval < 0) endpos = -1;
                    else endpos = size;
                }

                FlattenedTuple ft_orig = FlattenedTuple::fromLLVMStructVal(_env, builder, tuple.val,
                                                                           tuple_node->getInferredType());
                FlattenedTuple ft_new(_env);
                std::vector<SerializableValue> sliceVals;
                std::vector<python::Type> sliceTypes;

                for (int i = startpos; strideval * i < strideval * endpos; i += strideval) {
                    // important to use here vector index!
                    auto load = ft_orig.getLoad(builder, std::vector<int>{i});
                    sliceVals.push_back(load);
                    auto type = ft_orig.fieldType(std::vector<int>{i});
                    sliceTypes.push_back(type);
                }

                auto ft_new_type = python::Type::makeTupleType(sliceTypes);

                if (ft_new_type == python::Type::EMPTYTUPLE) {
                    auto alloc = builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr);
                    auto load = builder.CreateLoad(alloc);

                    // size of empty tuple is also 8 bytes (serialized size!)
                    return {load, _env->i64Const(sizeof(int64_t))};
                }

                ft_new.init(ft_new_type);
                for (int i = 0; i < (int) sliceVals.size(); i++) {
                    // important to use set and not setelement!
                    ft_new.set(builder, std::vector<int>{i}, sliceVals[i].val, sliceVals[i].size, sliceVals[i].is_null);
                }
                return {ft_new.getLoad(builder), ft_new.getSize(builder)};
            } else {
                error("We do not currently support slicing with static, unevaluated expressions");
            }

            return SerializableValue();
        }

        SerializableValue BlockGeneratorVisitor::popWithNullCheck(llvm::IRBuilder<> &builder, tuplex::ExceptionCode ec,
                                                                  const std::string &message) {
            using namespace llvm;

            assert(!_blockStack.empty());

            auto val = _blockStack.back();
            _blockStack.pop_back();

            // null check necessary?
            if (val.is_null) {
                // create exception
                assert(val.is_null->getType() == _env->i1Type());

                // _env->debugPrint(builder, "throwing '" + message + "' exception if isnull is 1: ", val.is_null);

                _lfb->addException(builder, ec, val.is_null);
                // normal code continues
            }

            // remove isnull!
            return SerializableValue(val.val, val.size, nullptr);
        }

        void BlockGeneratorVisitor::visit(NAssert* as) {
            if(earlyExit())return;

            auto num_stack_before = _blockStack.size();
            ApatheticVisitor::visit(as);

            // this is a weird case again, basically while doing the assert check another exception occurred!
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // end assert early!
            }

            // according to https://docs.python.org/3/reference/simple_stmts.html#grammar-token-assert-stmt
            // assert expression is equal to
            // if __debug__:
            //    if not expression: raise AssertionError
            // and
            // assert expression1, expression2 is equal to
            // if __debug__:
            //    if not expression1: raise AssertionError(expression2)

            // @TODO: check __debug__ variable for correct assert handling.
            assert(as->_expression);


            // ignore errorExpression in NAssert b.c. no support for try/except yet.
            // if try and except are added, then actual exception objects need to be produced.
            // still need to pop though!
            if(as->_errorExpression) {
                _blockStack.pop_back();
            }

            assert(_blockStack.size() >= 1);
            auto val = _blockStack.back();
            _blockStack.pop_back();

            auto builder = _lfb->getLLVMBuilder();
            auto expr_type = as->_expression->getInferredType();
            auto test = _env->truthValueTest(builder, val, expr_type);
            auto cond = _env->i1neg(builder, test); // flip for assert
            _lfb->addException(builder, ExceptionCode::ASSERTIONERROR, cond);
        }

        void BlockGeneratorVisitor::visit(NRaise* raise) {
            if(earlyExit())return;
            auto num_stack_before = _blockStack.size();
            ApatheticVisitor::visit(raise);

            // statement done? => here this basically means when raising an exception another excepiton occurred...
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return; // end statement early...
            }

            auto builder = _lfb->getLLVMBuilder();

            // @TODO: use symbol table here! And the env of the function!
            auto baseExceptionType = python::TypeFactory::instance().createOrGetPrimitiveType("BaseException");

            // raise is a tricky one: Python supports exception propagation
            // in Tuplex we should only use the topmost exception
            // right now, there's no support for try/except in Tuplex
            // hence, simplifies here to simply just use the first part of the raise statement
            // 3 options:
            // raise
            if(!raise->_expression && !raise->_fromExpression) {
                // simple, always raise RuntimeError
                //_lfb->addException(builder, ExceptionCode::RUNTIMEERROR, _env->i1Const(true));

                // NEW: exit path!
                _lfb->exitWithException(ExceptionCode::RUNTIMEERROR);
            }
            // raise expression [from expression]
            else {
                assert(raise->_expression);

                if(raise->_fromExpression) {
                    // pop fromExpression & ignore
                    _blockStack.pop_back();
                    _logger.info("Ignoring from exception, only top-level exception reported.");
                }

                // pop the dummy value
                _blockStack.pop_back();

                // check type is base exception
                if(!baseExceptionType.isSubclass(raise->_expression->getInferredType())) {
                    error("TypeError: exceptions must derive from BaseException");
                    return;
                }

                auto exceptionType = raise->_expression->getInferredType();
                auto exceptionName = exceptionType.desc();

                // get exception code corresponding to exceptionName!
                auto ecCode = pythonClassToExceptionCode(exceptionName); // TODO: Custom exceptions?

                if(ecCode == ExceptionCode::UNKNOWN) {
                    error("could not decode raise ExceptionClass to code");
                    return;
                }

                // _lfb->addException(builder, ecCode, _env->i1Const(true));

                // NEW: exit with exception!
                _lfb->exitWithException(ecCode);
            }
        }

        void BlockGeneratorVisitor::addGlobals(ASTNode* root, const ClosureEnvironment &ce) {

            // get builder in global init function!
            auto builder = _env->getInitGlobalBuilder("init_closure");

            auto table = SymbolTable::createFromEnvironment(&ce);

            // note: to speed this up, use only variables being accessed within this AST!
            std::set<std::string> accessedNames;
            ApplyVisitor av([](const ASTNode* node) { return node->type() == ASTNodeType::Identifier; },
                            [&accessedNames](ASTNode& node) {
                assert(node.type() == ASTNodeType::Identifier);
                auto id = static_cast<NIdentifier&>(node);
                if(id.hasAnnotation() && id.annotation().symbol)
                    accessedNames.insert(id.annotation().symbol->fullyQualifiedName());
                else
                    accessedNames.insert(id._name);
            });
            assert(root);
            root->accept(av);

            // from symbol table, add constants. E.g., things like math.e, math.pi, ...
            // go through all detected names, check if there's a symbol, add it as constant if possible
            for(const auto& name : accessedNames) {
                auto sym = table->findFullyQualifiedSymbol(name); // need to use fully qualified name here...
                // @TODO: aliasing??
                if(sym && sym->symbolType == SymbolType::VARIABLE) {
                    // skip module types
                    if(sym->types.front() == python::Type::MODULE)
                        continue;

                    // skip functions
                    if(sym->types.front().isFunctionType())
                        continue;

                    // skip exceptions though note that they can get redefined...
                    if(sym->types.front().isExceptionType())
                        continue;

                    auto it = _globals.find(name);
                    if(it == _globals.end()) {
                        // add new llvm const
                        auto const_val = _env->primitiveFieldToLLVM(builder, sym->constantData);
                        auto var = Variable::asGlobal(*_env, builder, sym->type(), name, const_val);
                        _globals[name] = std::make_tuple(sym->type(), var);
                    } else {
                        // internal error
                        error("duplicate global symbol " + name + " found in table");
                    }
                }
            }

//            for(const auto& c : ce.constants()) {
//               auto it = _globals.find(c.identifier);
//               if(it == _globals.end()) {
//                   // add new llvm const
//                   auto const_val = _env->primitiveFieldToLLVM(builder, c.value);
//                   auto var = Variable::asGlobal(*_env, builder, c.type, c.identifier, const_val);
//                   _globals[c.identifier] = std::make_tuple(c.type, var);
//               } else {
//                   // internal error
//                   error("duplicate global symbol " + c.identifier + " found in table");
//               }
//            }
        }

        llvm::Value *BlockGeneratorVisitor::binaryInst(llvm::Value *R, NBinaryOp *op, llvm::Value *L) {
            using namespace python;

            assert(_lfb);
            auto builder = _lfb->getLLVMBuilder();

            python::Type ltype = op->_left->getInferredType().withoutOptions();
            python::Type rtype = op->_right->getInferredType().withoutOptions();

            // only support i64, boolean
            if (!(ltype == python::Type::I64 || ltype == python::Type::BOOLEAN) ||
                !(rtype == python::Type::I64 || rtype == python::Type::BOOLEAN)) {
                return logErrorV(
                        "TypeError: unsupported operand type(s) for <<: '" + ltype.desc() + "' and '" + rtype.desc() +
                        "'");
            }

            // upcast to 64 bits if one is integer
            auto stype = python::Type::superType(ltype, rtype);
            auto uL = stype == python::Type::I64 && ltype == python::Type::BOOLEAN ? upCast(builder, L, _env->i64Type()) : L;
            auto uR = stype == python::Type::I64 && rtype == python::Type::BOOLEAN ? upCast(builder, R, _env->i64Type()) : R;

            switch(op->_op) {
                case TokenType::AMPER: {
                    return builder.CreateAnd(uL, uR);
                    break;
                }
                case TokenType::VBAR: {
                    return builder.CreateOr(uL, uR);
                    break;
                }
                case TokenType::CIRCUMFLEX: {
                    return builder.CreateXor(uL, uR);
                    break;
                }
#ifndef NDEBUG
                default:
                    error("internal compiler error, wrong Tokentype given.");
#endif
            }
            return nullptr;
        }

        void BlockGeneratorVisitor::updateSlotsBasedOnRealizations(llvm::IRBuilder<>& builder,
                const std::unordered_map<std::string, VariableRealization>& var_realizations,
                const std::string &branch_name,
                bool allowNumericUpcasting) {
            // update slots to super type if necessary
            for(const auto& var : var_realizations) {
                // slot exists?
                auto name = var.first;
                auto it = _variableSlots.find(name);
                if(it == _variableSlots.end()) {
                    // no, hence add the if type.
                    VariableSlot slot;
                    slot.type = var.second.type;
                    slot.var = Variable(*_env, builder, slot.type, name);
                    // alloc pointer and assign false
                    // => later in ifbuilder assign true to this variable!
                    slot.definedPtr = _env->CreateFirstBlockVariable(builder, _env->i1Const(false));
                    _variableSlots[name] = slot;
                } else {
                    // check if we can unify type
                    // if not, error. Type annotation failed then!
                    auto& slot = it->second;

                    auto uni_type = unifyTypes(slot.type, var.second.type, allowNumericUpcasting);
                    if(uni_type == python::Type::UNKNOWN) {
                        error("variable " + name + " declared in " + branch_name + " with type "
                              + var.second.type.desc() + " conflicts with slot type" +
                              slot.type.desc());
                    }

                    // same type? no problem.
                    // else, require to realloc variable and then need to assign to it.
                    if(uni_type != slot.type) {
                        // create new var. Values will be assigned to it below.
                        VariableSlot new_slot;
                        new_slot.type = uni_type;
                        new_slot.var = Variable(*_env, builder, uni_type, name);

                        // alloc pointer and assign false
                        // => later in ifbuilder assign true to this variable!
                        new_slot.definedPtr = _env->CreateFirstBlockVariable(builder, _env->i1Const(false));
                        _variableSlots[name] = new_slot;
                    }
                }
            }
        }

        void BlockGeneratorVisitor::updateSlotsWithSharedTypes(IRBuilder<> &builder,
                                                               const std::unordered_map<std::string, VariableRealization> &if_var_realizations,
                                                               const std::unordered_map<std::string, VariableRealization> &else_var_realizations) {

            // fast skip if there's no overlap.
            if(if_var_realizations.empty() || else_var_realizations.empty())
                return;

            // get shared names & types
            std::vector<std::pair<std::string, python::Type>> shared_types;
            for(const auto& if_var : if_var_realizations) {
                auto it = else_var_realizations.find(if_var.first);
                if(it != else_var_realizations.end()) {
                    if(it->second.type == if_var.second.type)
                        shared_types.push_back(std::make_pair(if_var.first, if_var.second.type));
                }
            }
            // for all these shared types, now do check whether they conflict with the beforeIf state:
            // -> exchange the slot then!
            for(const auto& st : shared_types) {
                // replace slot if type doesn't match
                auto name = st.first;
                auto it = _variableSlots.find(name);
                if(it != _variableSlots.end()) {
                    if(it->second.type == st.second)
                        continue; // skip replace code below
                }
                // replace, because shared type is different than the before type!
                VariableSlot slot;
                slot.type = st.second;
                slot.var = Variable(*_env, builder, slot.type, name);

                // alloc pointer and assign true
                // => vars are shared so they're defined in both!
                // => we could even get rid off the runtime define here!
                slot.definedPtr = _env->CreateFirstBlockVariable(builder, _env->i1Const(true));
                _variableSlots[name] = slot;
            }
        }

        BlockGeneratorVisitor::Variable BlockGeneratorVisitor::Variable::asGlobal(LLVMEnvironment &env, llvm::IRBuilder<> &builder,
                                                           const python::Type &t, const std::string &name,
                                                           const SerializableValue &value) {
            assert(value.size && value.val);

            Variable var;
            var.name = name;
            var.ptr = env.createNullInitializedGlobal(name + "_val", env.pythonToLLVMType(t));
            var.sizePtr = env.createNullInitializedGlobal(name + "_size", env.i64Type());

            if(t.isOptionType() || t == python::Type::NULLVALUE) {
                assert(value.is_null);
                var.nullPtr = env.createNullInitializedGlobal(name + "_isnull", env.i1Type());

                builder.CreateStore(value.is_null, var.nullPtr);
            } else {
                var.nullPtr = nullptr;
            }

            // store in builder into global vars!
            builder.CreateStore(value.val, var.ptr);
            builder.CreateStore(value.size, var.sizePtr);

            // TODO: shortcut for constants, when invoking var.load(...)? i.e. replace them?
            return var;
        }

        void BlockGeneratorVisitor::visit(NAttribute* attr) {
            // check if it's an identifier.identifier.....identifier attribute tree
            // => lookup symbol with fully qualified name!!!
            assert(attr);

            // example: math.pi
            // -> value is math and attribute is pi
            if((attr->_value->type() == ASTNodeType::Identifier || attr->_value->type() == ASTNodeType::Attribute)
               && attr->_attribute->type() == ASTNodeType::Identifier) {
                std::string fullyQualifiedName;
                auto cur_attr = attr;
                // recurse over attribute tree...
                while(true) {
                    fullyQualifiedName = fullyQualifiedName + "." + cur_attr->_attribute->_name;
                    if(cur_attr->_value->type() == ASTNodeType::Identifier)  {
                        fullyQualifiedName = ((NIdentifier*)(cur_attr->_value))->_name + fullyQualifiedName;
                        break;
                    } else if(cur_attr->_value->type() == ASTNodeType::Attribute) {
                        cur_attr = (NAttribute*)cur_attr->_value;
                    } else {
                            ApatheticVisitor::visit(attr);
                            return;
                    }
                }

                // check if in globals!
                auto it = _globals.find(fullyQualifiedName);
                if(it != _globals.end()) {
                    // check type and then return
                    assert(std::get<0>(it->second) == attr->getInferredType());
                    auto var = std::get<1>(it->second);
                    auto builder = _lfb->getLLVMBuilder();
                    auto val = var.load(builder);
                    addInstruction(val.val, val.size, val.is_null);
                    return;
                } else {
                    ApatheticVisitor::visit(attr);
                    return;
                }
            } else {
                ApatheticVisitor::visit(attr);
            }
        }

        void BlockGeneratorVisitor::visit(NFor *forStmt) {
            using namespace python;
            assert(forStmt);
            assert(forStmt->target);
            assert(forStmt->expression);
            assert(forStmt->suite_body);

            auto builder = _lfb->getLLVMBuilder();
            auto num_stack_before = _blockStack.size();
            auto exprType = forStmt->expression->getInferredType();
            auto targetType = forStmt->target->getInferredType();
            auto targetASTType = forStmt->target->type();
            std::vector<NIdentifier*> loopVal;
            if(targetASTType == ASTNodeType::Identifier) {
                auto id = static_cast<NIdentifier*>(forStmt->target);
                loopVal.push_back(id);
            } else if(targetASTType == ASTNodeType::Tuple || targetASTType == ASTNodeType::List) {
                auto idTuple = getForLoopMultiTarget(forStmt->target);
                loopVal.resize(idTuple.size());
                std::transform(idTuple.begin(), idTuple.end(), loopVal.begin(), [](ASTNode* x){return static_cast<NIdentifier*>(x);});
            } else {
                fatal_error("Unsupported target type");
            }

            // emit expression
            forStmt->expression->accept(*this);
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }

            // get parent function
            llvm::Function *parentFunc = _lfb->getLastBlock()->getParent();

            // get entry block
            auto *entryBB = _lfb->getLastBlock();
            builder.SetInsertPoint(entryBB);

            // create loop blocks
            auto *condBB = llvm::BasicBlock::Create(_env->getContext(), "condBB", parentFunc);
            auto *loopBB = llvm::BasicBlock::Create(_env->getContext(), "loopBB", parentFunc);
            auto *iterEndBB = llvm::BasicBlock::Create(_env->getContext(), "iterEndBB", parentFunc);
            auto *elseBB = llvm::BasicBlock::Create(_env->getContext(), "elseBB", parentFunc);
            auto *afterLoop = llvm::BasicBlock::Create(_env->getContext(), "afterLoop", parentFunc);
            // 'continue' will create an unconditional branch to 'iterEndBB'
            // 'break' will create an unconditional branch to 'afterLoop'
            _loopBlockStack.push_back(afterLoop);
            _loopBlockStack.push_back(iterEndBB);

            // retrieve expression (list, string, range, or iterator)
            auto exprAlloc = _blockStack.back();
            _blockStack.pop_back();

            llvm::Value *start=nullptr, *step=nullptr, *end=nullptr;
            std::shared_ptr<IteratorInfo> iteratorInfo = nullptr;
            if(exprType.isListType()) {
                start = _env->i64Const(0);
                step = _env->i64Const(1);
                if(exprType == python::Type::EMPTYLIST) {
                    end = _env->i64Const(0);
                } else {
                    end = builder.CreateExtractValue(exprAlloc.val, {1});
                }
            } else if(exprType == python::Type::STRING) {
                start = _env->i64Const(0);
                step = _env->i64Const(1);
                end = builder.CreateSub(exprAlloc.size, _env->i64Const(1));
            } else if(exprType == python::Type::RANGE) {
                start = builder.CreateLoad(_env->CreateStructGEP(builder, exprAlloc.val, 0));
                end = builder.CreateLoad(_env->CreateStructGEP(builder, exprAlloc.val, 1));
                step = builder.CreateLoad(_env->CreateStructGEP(builder, exprAlloc.val, 2));
            } else if(exprType.isIteratorType()) {
                assert(forStmt->expression->hasAnnotation() && forStmt->expression->annotation().iteratorInfo);
                iteratorInfo = forStmt->expression->annotation().iteratorInfo;
            } else {
                fatal_error("unsupported expression type '" + exprType.desc() + "' in for loop encountered");
            }
            builder.CreateBr(condBB);

            // loop ending condition test. If loop ends, jump to elseBB, otherwise jump to loopBB (the main loop body).
            // loopCond indicates whether loop should continue. i.e. loopCond == false will end the loop.
            llvm::PHINode *curr = nullptr;
            llvm::Value *loopCond = nullptr;
            builder.SetInsertPoint(condBB);
            if(exprType.isIteratorType()) {
                // if expression (testlist) is an iterator. Check if iterator exhausted
                if(exprType == python::Type::EMPTYITERATOR) {
                    loopCond = _env->i1Const(true);
                }
                auto iteratorExhausted = _iteratorContextProxy->updateIteratorIndex(builder, exprAlloc.val, iteratorInfo);
                // loopCond = !iteratorExhausted i.e. if iterator exhausted, ends the loop
                loopCond = builder.CreateICmpEQ(iteratorExhausted, _env->i1Const(false));
            } else {
                // expression is list, string or range. Check if curr exceeds end.
                curr = builder.CreatePHI(_env->i64Type(), 2);
                curr->addIncoming(start, entryBB);
                curr->addIncoming(builder.CreateAdd(curr, step), iterEndBB);
                if(exprType == python::Type::RANGE) {
                    // step can be negative in range. Check if curr * stepSign < end * stepSign
                    // positive step -> stepSign = 1, negative step -> stepSign = -1
                    // stepSign = (step >> 63) | 1 , use arithmetic shift
                    auto stepSign = builder.CreateOr(builder.CreateAShr(step, _env->i64Const(63)), _env->i64Const(1));
                    loopCond = builder.CreateICmpSLT(builder.CreateMul(curr, stepSign), builder.CreateMul(end, stepSign));
                } else {
                    loopCond = builder.CreateICmpSLT(curr, end);
                }
            }
            builder.CreateCondBr(loopCond, loopBB, elseBB);

            // handle loop body
            builder.SetInsertPoint(loopBB);
            _lfb->setLastBlock(loopBB);

            if(exprType.isListType()) {
                if(exprType != python::Type::EMPTYLIST) {
                    auto currVal = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(exprAlloc.val, {2}), curr));
                    if(targetType == python::Type::I64 || targetType == python::Type::F64) {
                        // loop variable is of type i64 or f64 (has size 8)
                        addInstruction(currVal, _env->i64Const(8));
                    } else if(targetType == python::Type::STRING || targetType.isDictionaryType()) {
                        // loop variable is of type string or dictionary (need to extract size)
                        auto currSize = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(exprAlloc.val, {3}), curr));
                        addInstruction(currVal, currSize);
                    } else if(targetType == python::Type::BOOLEAN) {
                        // loop variable is of type bool (has size 1)
                        addInstruction(currVal, _env->i64Const(1));
                    } else if(targetType.isTupleType()) {
                        // target is of tuple type
                        // either target is a single identifier and needs to be assigned the entire tuple
                        // or target has multiple identifiers and each corresponds to a value in the tuple
                        FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env, builder, currVal, targetType);
                        // add value to stack for all identifiers in target
                        if(loopVal.size() == 1) {
                            // single identifier
                            addInstruction(currVal, ft.getSize(builder));
                        } else {
                            // multiple identifiers, add each value in tuple to stack in reverse order
                            for (int i = loopVal.size() - 1; i >= 0 ; --i) {
                                auto idVal = ft.getLoad(builder, {i}); // get the value for the ith identifier from tuple
                                addInstruction(idVal.val, idVal.size);
                            }
                        }
                    } else {
                        fatal_error("unsupported target type '" + targetType.desc() + "' in for loop encountered");
                    }
                }
            } else if(exprType == python::Type::STRING) {
                // target is a single character
                // allocate new string (1-byte character with a 1-byte null terminator)
                auto currCharPtr = builder.CreateGEP(exprAlloc.val, curr);
                auto currSize = _env->i64Const(2);
                auto currVal = builder.CreatePointerCast(_env->malloc(builder, currSize), _env->i8ptrType());
                builder.CreateStore(builder.CreateLoad(currCharPtr), currVal);
                auto nullCharPtr = builder.CreateGEP(_env->i8Type(), currVal, _env->i32Const(1));
                builder.CreateStore(_env->i8Const(0), nullCharPtr);
                addInstruction(currVal, currSize);
            } else if(exprType == python::Type::RANGE) {
                addInstruction(curr, _env->i64Const(8));
            } else if(exprType.isIteratorType()) {
                if(exprType != python::Type::EMPTYITERATOR) {
                    auto currVal = _iteratorContextProxy->getIteratorNextElement(builder, exprType.yieldType(), exprAlloc.val, iteratorInfo);
                    if(exprType.yieldType().isListType()) {
                        if(loopVal.size() == 1) {
                            addInstruction(currVal.val, currVal.size);
                        } else {
                            // multiple identifiers, add each value in list to stack in reverse order
                            for (int i = loopVal.size() - 1; i >= 0 ; --i) {
                                auto idVal = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(currVal.val, {2}), _env->i32Const(i)));
                                auto idType = loopVal[i]->getInferredType();
                                if(idType == python::Type::I64 || targetType == python::Type::F64) {
                                    addInstruction(idVal, _env->i64Const(8));
                                } else if(idType == python::Type::BOOLEAN) {
                                    addInstruction(idVal, _env->i64Const(1));
                                } else if(idType == python::Type::STRING || idType.isDictionaryType()) {
                                    auto idValSize = builder.CreateLoad(builder.CreateGEP(builder.CreateExtractValue(currVal.val, {3}), _env->i32Const(i)));
                                    addInstruction(idVal, idValSize);
                                } else if(idType.isTupleType()) {
                                    FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env, builder, idVal, idType);
                                    addInstruction(idVal, ft.getSize(builder));
                                } else {
                                    fatal_error("unsupported target type '" + idType.desc() + "' in for loop encountered");
                                }
                            }
                        }
                    } else if(exprType.yieldType().isTupleType()) {
                        FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env, builder, currVal.val, targetType);
                        // add value to stack for all identifiers in target
                        if(loopVal.size() == 1) {
                            addInstruction(currVal.val, currVal.size);
                        } else {
                            // multiple identifiers, add each value in tuple to stack in reverse order
                            for (int i = loopVal.size() - 1; i >= 0 ; --i) {
                                auto idVal = ft.getLoad(builder, {i}); // get the value for the ith identifier from tuple
                                addInstruction(idVal.val, idVal.size);
                            }
                        }
                    } else {
                        addInstruction(currVal.val, currVal.size);
                    }
                }
            } else {
                fatal_error("unsupported expression type '" + exprType.desc() + "' in for loop encountered");
            }

            if(exprType != python::Type::EMPTYLIST && exprType != python::Type::EMPTYITERATOR) {
                // assign value for each identifier in target
                for (const auto & id : loopVal) {
                    assignToSingleVariable(id, id->getInferredType());
                }
                // codegen for body
                forStmt->suite_body->accept(*this);
            }

            if(blockOpen(_lfb->getLastBlock())) {
                builder.SetInsertPoint(_lfb->getLastBlock());
                builder.CreateBr(iterEndBB);
            }

            builder.SetInsertPoint(iterEndBB);
            builder.CreateBr(condBB);

            _loopBlockStack.pop_back();
            _loopBlockStack.pop_back();

            // handle else
            builder.SetInsertPoint(elseBB);
            _lfb->setLastBlock(elseBB);

            if(forStmt->suite_else) {
                forStmt->suite_else->accept(*this);
            }

            if(blockOpen(_lfb->getLastBlock())) {
                builder.SetInsertPoint(_lfb->getLastBlock());
                builder.CreateBr(afterLoop);
            }

            builder.SetInsertPoint(afterLoop);
            _lfb->setLastBlock(afterLoop);
        }

        void BlockGeneratorVisitor::visit(NWhile *whileStmt) {
            assert(whileStmt);
            assert(whileStmt->expression);
            assert(whileStmt->suite_body);

            auto builder = _lfb->getLLVMBuilder();
            auto num_stack_before = _blockStack.size();

            // get parent function
            llvm::Function *parentFunc = _lfb->getLastBlock()->getParent();

            // create loop blocks
            auto *condBB = llvm::BasicBlock::Create(_env->getContext(), "condBB", parentFunc);
            auto *loopBB = llvm::BasicBlock::Create(_env->getContext(), "loopBB", parentFunc);
            auto *elseBB = llvm::BasicBlock::Create(_env->getContext(), "elseBB", parentFunc);
            auto *afterLoop = llvm::BasicBlock::Create(_env->getContext(), "afterLoop", parentFunc);
            _loopBlockStack.push_back(afterLoop);
            _loopBlockStack.push_back(condBB);
            builder.SetInsertPoint(_lfb->getLastBlock());
            builder.CreateBr(condBB);

            builder.SetInsertPoint(condBB);
            _lfb->setLastBlock(condBB);

            // emit condition expression
            whileStmt->expression->accept(*this);
            if(earlyExit()) {
                while(_blockStack.size() > num_stack_before)
                    _blockStack.pop_back();
                return;
            }
            builder.SetInsertPoint(_lfb->getLastBlock());

            // retrieve condition value
            auto cond = _blockStack.back();
            _blockStack.pop_back();

            // convert condition value to i1
            auto whileCond = _env->truthValueTest(builder, cond, whileStmt->expression->getInferredType());
            builder.CreateCondBr(whileCond, loopBB, elseBB);

            // handle loop body
            builder.SetInsertPoint(loopBB);
            _lfb->setLastBlock(loopBB);
            whileStmt->suite_body->accept(*this);

            if(blockOpen(_lfb->getLastBlock())) {
                builder.SetInsertPoint(_lfb->getLastBlock());
                builder.CreateBr(condBB);
            }

            _loopBlockStack.pop_back();
            _loopBlockStack.pop_back();

            // handle else
            builder.SetInsertPoint(elseBB);
            _lfb->setLastBlock(elseBB);

            if(whileStmt->suite_else) {
                whileStmt->suite_else->accept(*this);
            }

            if(blockOpen(_lfb->getLastBlock())) {
                builder.SetInsertPoint(_lfb->getLastBlock());
                builder.CreateBr(afterLoop);
            }

            builder.SetInsertPoint(afterLoop);
            _lfb->setLastBlock(afterLoop);
        }

        void BlockGeneratorVisitor::visit(NContinue *continueStmt) {
            if(_loopBlockStack.size() < 2) {
                fatal_error("'continue' outside loop");
            }

            auto builder = _lfb->getLLVMBuilder();
            auto condBB = _loopBlockStack.back();

            builder.SetInsertPoint(_lfb->getLastBlock());
            builder.CreateBr(condBB);
        }

        void BlockGeneratorVisitor::visit(NBreak *breakStmt) {
            if(_loopBlockStack.size() < 2) {
                fatal_error("'break' outside loop");
            }

            auto builder = _lfb->getLLVMBuilder();
            auto afterLoop = _loopBlockStack.rbegin()[1];

            builder.SetInsertPoint(_lfb->getLastBlock());
            builder.CreateBr(afterLoop);
        }

        void BlockGeneratorVisitor::visitUnrolledLoopSuite(NSuite *loopSuite) {
            assert(loopSuite);

            auto builder = _lfb->getLLVMBuilder();

            // get parent function
            llvm::Function *parentFunc = _lfb->getLastBlock()->getParent();

            // create ending block for the entire loop
            auto *afterLoop = llvm::BasicBlock::Create(_env->getContext(), "afterLoop", parentFunc);
            _loopBlockStack.push_back(afterLoop);

            // emit each loop iteration
            int loopSize = loopSuite->_statements.size();
            for (int i = 0; i < loopSize - 1; i += 2) {
                builder.SetInsertPoint(_lfb->getLastBlock());
                auto *iterEndBB = llvm::BasicBlock::Create(_env->getContext(), "iterEndBB", parentFunc);
                _loopBlockStack.push_back(iterEndBB);

                // emit assign statement
                loopSuite->_statements[i]->accept(*this);
                // emit suite_body
                loopSuite->_statements[i+1]->accept(*this);

                builder.SetInsertPoint(_lfb->getLastBlock());
                builder.CreateBr(iterEndBB);

                _lfb->setLastBlock(iterEndBB);
                _loopBlockStack.pop_back();
            }

            _loopBlockStack.pop_back();

            // emit suite_else if exists
            if(loopSize % 2 == 1) {
                builder.SetInsertPoint(_lfb->getLastBlock());
                loopSuite->_statements.back()->accept(*this);
            }

            builder.SetInsertPoint(_lfb->getLastBlock());
            builder.CreateBr(afterLoop);

            builder.SetInsertPoint(afterLoop);
            _lfb->setLastBlock(afterLoop);
        }

        // helper function to deal with int or float mul
        inline llvm::Value* mul_op(llvm::IRBuilder<>& builder, llvm::Value* R, llvm::Value* L) {
           // needs to be same type!
           assert(R->getType() == L->getType());
           if(R->getType()->isIntegerTy())
               return builder.CreateMul(R, L);
           else {
               assert(R->getType()->isDoubleTy());
               return builder.CreateFMul(R, L);
           }
        }

        llvm::Value *BlockGeneratorVisitor::generateConstantIntegerPower(llvm::IRBuilder<>& builder, llvm::Value *base,
                                                                         int64_t exponent) {
            assert(base);

            // there are a couple special cases
            if(0 == exponent)
                return base->getType()->isIntegerTy() ? _env->i64Const(1) : _env->f64Const(1.0);

            // is base 0? => special cases happening here!
            // note: no eps compare here!
            auto base_is_zero = base->getType()->isIntegerTy() ?
                    builder.CreateICmpEQ(base, _env->i64Const(0)) :
                    builder.CreateFCmpOEQ(base, _env->f64Const(0.0));

            auto bbBaseZero = BasicBlock::Create(_env->getContext(), "base_is_zero", builder.GetInsertBlock()->getParent());
            auto bbBaseNonZero = BasicBlock::Create(_env->getContext(), "base_non_zero", builder.GetInsertBlock()->getParent());
            auto bbPowerDone = BasicBlock::Create(_env->getContext(), "power_done", builder.GetInsertBlock()->getParent());

            llvm::Value* base_zero_power = base->getType()->isIntegerTy() ? _env->i64Const(0) : _env->f64Const(0.0); // dummy

            builder.CreateCondBr(base_is_zero, bbBaseZero, bbBaseNonZero);
            builder.SetInsertPoint(bbBaseZero);
            if(exponent < 0) {
                // always zero division error!
                _lfb->addException(builder, ExceptionCode::ZERODIVISIONERROR, base_is_zero); // always true
            } else {
                // result is always 0 if base == 0
                // note: exponent == 0 handled above!
                if(base->getType()->isIntegerTy())
                    base_zero_power = _env->i64Const(0);
                else {
                    assert(base->getType()->isDoubleTy());
                    base_zero_power = _env->f64Const(0.0);
                }
            }
            bbBaseZero = builder.GetInsertBlock(); // if exceptions were generated...
            builder.CreateBr(bbPowerDone);

            //--------------------------------------------------------------------------------
            builder.SetInsertPoint(bbBaseNonZero);
            // for a couple special cases use addition chain exponentiation (table)
            // https://en.wikipedia.org/wiki/Addition-chain_exponentiation
            llvm::Value* power = nullptr;
            switch(std::abs(exponent)) {
                case 1: {
                    power = base;
                    break;
                }
                case 2: {
                    power = mul_op(builder, base, base);
                    break;
                }
                case 3: {
                    auto base2 = mul_op(builder, base, base);
                    power = mul_op(builder, base2, base);
                    break;
                }
                case 4: {
                    auto base2 = mul_op(builder, base, base);
                    power = mul_op(builder, base2, base2);
                    break;
                }
                case 5: {
                    auto base2 = mul_op(builder, base, base);
                    auto base4 = mul_op(builder, base2, base2);
                    power = mul_op(builder, base, base4);
                    break;
                }
                case 6: {
                    auto base2 = mul_op(builder, base, base);
                    auto base4 = mul_op(builder, base2, base2);
                    power = mul_op(builder, base2, base4);
                    break;
                }
                // could do more, yet let's stop here...
                default: {
                    // use loop (squared exponentiation)
                    llvm::Value *curSq = base;
                    int64_t cur_exponent = std::abs(exponent);
                    while (cur_exponent > 1) {
                        // fmul or int mul
                        curSq = mul_op(builder, curSq, curSq);
                        cur_exponent -= 2;
                    }

                    assert(cur_exponent >= 0);

                    if (1 == exponent)
                        power = mul_op(builder, curSq, base);
                    power = curSq;
                    break;
                }
            }

            if(exponent < 0) {
                // convert to float! => numerical stability?
                // i.e. 1.0 / ...
                if(power->getType()->isIntegerTy())
                    power = builder.CreateSIToFP(power, _env->doubleType());
                power = builder.CreateFDiv(_env->f64Const(1.0), power);
            }
            bbBaseNonZero = builder.GetInsertBlock(); // if exceptions were generated...
            builder.CreateBr(bbPowerDone);

            // phi based return value
            builder.SetInsertPoint(bbPowerDone);
            _lfb->setLastBlock(bbPowerDone);
            auto base_type = base->getType();

            // upcast_base_zero_power
            auto upcast_base_zero_power = base_zero_power;
            auto upcast_power = power;
            if(base_zero_power->getType() != power->getType()) {
                // upcast to larger type
                assert(base_zero_power->getType()->isDoubleTy() || power->getType()->isDoubleTy());
                upcast_power = _env->upCast(builder, power, _env->doubleType());
                upcast_base_zero_power = _env->upCast(builder, base_zero_power, _env->doubleType());
            }

            base_type = upcast_power->getType();
            auto phi = builder.CreatePHI(base_type, 2);
            phi->addIncoming(upcast_base_zero_power, bbBaseZero);
            phi->addIncoming(upcast_power, bbBaseNonZero);
            return phi;
        }

        void BlockGeneratorVisitor::updateIteratorVariableSlot(llvm::IRBuilder<> &builder, VariableSlot *slot,
                                                               const SerializableValue &val,
                                                               const python::Type &targetType,
                                                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            if (targetType != slot->type) {
                // set curr slot to iteratorType if it's not.
                slot->type = targetType;
            }

            llvm::Type *newPtrType = nullptr;
            if(targetType == python::Type::EMPTYITERATOR) {
                newPtrType = _env->i64Type();
            } else {
                newPtrType = llvm::PointerType::get(_env->createOrGetIteratorType(iteratorInfo), 0);
            }

            if(!slot->var.ptr || slot->var.ptr->getType() != newPtrType) {
                // Since python iteratorType to llvm iterator type is a one-to-many mapping,
                // may need to update ptr later even if current slot type is iteratorType
                slot->var.ptr = _env->CreateFirstBlockAlloca(builder, newPtrType, slot->var.name);
            }
            slot->var.store(builder, val);
        }
    }
}