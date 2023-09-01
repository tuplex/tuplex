//
// Created by leonhards on 4/2/23.
//

#include <physical/codegen/CheckHelper.h>

namespace tuplex {
    namespace codegen {
//        llvm::Value* generate_cell_based_checks(LLVMEnvironment& env,
//                                                llvm::IRBuilder<>& builder,
//                                                llvm::Value* cellsPtr,
//                                                llvm::Value* sizesPtr,
//                                                const std::vector<NormalCaseCheck>& checks) {
//
//            auto& logger = Logger::instance().logger("codegen");
//
//            // no checks? -> pass.
//            if(checks.empty())
//                return env.i1Const(true);
//
//            logger.debug("cell based checks with " + pluralize(checks.size(), "check"));
//
//            // sanity check, emit warning if check was given but col not read?
//            for(const auto& check : _checks) {
//                if(check.isSingleColCheck()) {
//                    if(check.colNo() >= _generalCaseColumnsToSerialize.size())
//                        logger.warn("check has invalid column number");
//                    else {
//                        if(!_generalCaseColumnsToSerialize[check.colNo()])
//                            logger.warn("CellSourceTaskBuilder received check for col=" + std::to_string(check.colNo()) + ", but column is eliminated in pushdown!");
//                    }
//                    std::stringstream ss;
//                    if(check.type == CheckType::CHECK_CONSTANT) {
//                        ss<<"detected possible constant check == "<<check.constant_type().constant()<<" for column "<<check.colNo()<<"\n";
//                    } else {
//                        ss<<"emitting unknown check for column "<<check.colNo()<<"\n";
//                    }
//                    logger.info(ss.str());
//                } else {
//                    logger.info("found multi-col check (filter)");
//                }
//            }
//
//            // Interesting questions re. checks: => these checks should be performed first. What is the optimal order of checks to perform?
//            // what to test first for?
//
//            llvm::Value* allChecksPassed = _env->i1Const(true);
//            // _env->debugPrint(builder, "Checking normalcase for rowno=", rowNumber);
//
//            // Also, need to have some optimization re parsing. Parsing is quite expensive, so only parse if required!
//            for(int i = 0; i < _generalCaseColumnsToSerialize.size(); ++i) {
//                // should column be serialized? if so emit type logic!
//                if(_generalCaseColumnsToSerialize[i]) {
//                    // find all checks for that column
//                    for(const auto& check : _checks) {
//
//                        if(check.isSingleColCheck()) {
//                            if(check.colNo() == i) {
//                                // string type? direct compare
//                                llvm::Value* check_cond = nullptr;
//
//                                // emit code for check
//                                auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
//                                auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(i)), "s" + std::to_string(i));
//
//                                // what type of check is it?
//                                // only support constant check yet
//                                if(check.type == CheckType::CHECK_CONSTANT) {
//                                    {
//                                        std::stringstream ss;
//                                        ss<<"general-case requires column, emit code for constant check == "
//                                          <<check.constant_type().constant()
//                                          <<" for column "<<check.colNo();
//                                        logger.info(ss.str());
//                                    }
//
//                                    auto const_type = check.constant_type();
//                                    // performing check against string constant
//                                    assert(const_type.isConstantValued());
//                                    auto elementType = const_type.underlying();
//
//                                    //  auto t = rowType.parameters()[rowTypePos];?
////                                assert(elementType == )
//
//                                    auto value = const_type.constant();
//                                    if(elementType.isOptionType()) {
//                                        // is the constant null? None?
//                                        if(value == "None" || value == "null")  {
//                                            elementType = python::Type::NULLVALUE;
//                                        } else
//                                            elementType = elementType.elementType();
//                                    }
//
//                                    if(python::Type::STRING == elementType) {
//                                        // direct compare
//                                        auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
//                                        // _env->debugPrint(builder, "Checking whether cellsize==", _env->i64Const(const_type.constant().size() + 1));
//                                        auto constant_value = const_type.constant(); // <-- constant has the actual value here!
//                                        check_cond = builder.CreateICmpEQ(val.size, _env->i64Const(constant_value.size() + 1));
//                                        check_cond = builder.CreateAnd(check_cond, _env->fixedSizeStringCompare(builder, val.val, constant_value));
//                                    } else if(python::Type::NULLVALUE == elementType) {
//                                        // special case: perform null check against array!
//                                        // null check (??)
//                                        auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
//                                        check_cond =  val.is_null;
//                                    } else if(python::Type::BOOLEAN == elementType) {
//                                        auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
//
//                                        // compare value
//                                        auto c_val = parseBoolString(const_type.constant());
//                                        check_cond = builder.CreateICmpEQ(_env->boolConst(c_val), val.val);
//                                    } else if(python::Type::I64 == elementType) {
//
//                                        // this check can be performed faster (if no leading 0s are assumed).
//                                        // i.e. use https://news.ycombinator.com/item?id=21019007 intrinsic.
//                                        // can also use that intrinsic for string comparison!
//
//                                        auto const_string = value;
//
//                                        auto sizePtr = builder.CreateGEP(sizesPtr, env().i64Const(i));
//                                        auto str_size = builder.CreateLoad(sizePtr);
//                                        // size correct?
//                                        auto size_ok = builder.CreateICmpEQ(str_size, env().i64Const(value.size() + 1));
//                                        auto ptr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)));
//                                        const auto& DL = env().getModule()->getDataLayout();
//                                        //const TargetLibraryInfo *TLI = nullptr; // <-- this may be wrong?
//
//                                        //    if (!BaselineInfoImpl)
//                                        //     BaselineInfoImpl =
//                                        //         TargetLibraryInfoImpl(Triple(F.getParent()->getTargetTriple()));
//                                        //   return TargetLibraryInfo(*BaselineInfoImpl, &F);
//
////                                    TargetLibraryAnalysis TLA;
////                                    FunctionAnalysisManager DummyFAM;
////                                    auto F  = builder.GetInsertBlock()->getParent();
////                                    auto TLI = TLA.run(*F, DummyFAM);
////                                    auto M = builder.GetInsertBlock()->getParent()->getParent();
////                                    auto BaselineInfoImpl = TargetLibraryInfoImpl(Triple(M->getTargetTriple()));
////                                    auto TLI2 = TargetLibraryInfo(BaselineInfoImpl);
////
////                                    memcmp_prototype()
////                                    // bcmp means simply compare memory, but no need to retrieve first pos of different byte.
////                                    // -> cheaper to execute.
////                                    auto cmp_ok = llvm::emitBCmp(ptr,
////                                                                 env().strConst(builder, value),
////                                                                 env().i64Const(value.size()),
////                                                                 builder,
////                                                                 DL,
////                                                                 &TLI);
//
//                                        // should be optimized to bcmp
//                                        auto memcmpFunc = memcmp_prototype(env().getContext(), env().getModule().get());
//                                        auto n = env().i64Const(value.size());
//                                        auto cmp_ptr = env().strConst(builder, value);
//                                        assert(ptr);
//                                        assert(ptr->getType() == env().i8ptrType());
//                                        assert(cmp_ptr->getType() == env().i8ptrType());
//                                        auto cmp_ok = builder.CreateICmpEQ(env().i64Const(0), builder.CreateCall(memcmpFunc, {ptr, cmp_ptr, n}));
//                                        assert(cmp_ok);
//                                        assert(cmp_ok->getType() == env().i1Type());
//                                        check_cond = builder.CreateAnd(size_ok, cmp_ok);
//
//
//                                        // #error "prevent expensive check here, simply compare length of string and value contents!"
//                                        // old code here: i.e., full parse...
//                                        // auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
////
//                                        // // compare value
//                                        // auto c_val = parseI64String(const_type.constant());
//                                        // check_cond = builder.CreateICmpEQ(_env->i64Const(c_val), val.val);
//                                    } else if(python::Type::F64 == elementType) {
//                                        auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
//
//                                        // compare value
//                                        auto c_val = parseF64String(const_type.constant());
//                                        // todo: compare maybe a with abs?
//                                        check_cond = builder.CreateFCmpOEQ(_env->f64Const(c_val), val.val);
//                                    } else {
//                                        // fail check, because unsupported type
//                                        std::stringstream ss;
//                                        ss<<"unsupported type for check "<<elementType.desc()<<" found, fail normal check for all rows";
//                                        logger.error(ss.str());
//                                        check_cond = _env->i1Const(false);
//                                    }
//
//                                    // now perform check
//                                    // if !check -> normal_case violation!
//                                    // else, all good!
//
//                                    // // debug:
//                                    // _env->debugPrint(builder, "performing constant check for col=" + std::to_string(i) + " , " + check.constant_type().desc() + " (1=passed): ", check_cond);
//                                    // _env->debugPrint(builder, "cellStr", cellStr);
//                                    // _env->debugPrint(builder, "cellSize", cellSize);
//
//                                } else {
//                                    logger.warn("unsupported check type encountered");
//                                }
//                                // append to all checks
//                                allChecksPassed = builder.CreateAnd(allChecksPassed, check_cond);
//                            }
//                        } else {
//                            // skip... emit multi-col checks separately...
//                        }
//
//                    }
//                }
//            }
//
//            return nullptr;
//        }
    }
}