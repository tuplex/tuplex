//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <LLVMEnvironment.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <TypeSystem.h>
#include <Logger.h>
#include <StringUtils.h>
#include <Base.h>
#include <CodegenHelper.h>
#include <Utils.h>
#include <pcre2.h>
#include <TupleTree.h>

using namespace llvm;

// helper functions for debugging.
extern "C" {
void _llvmPrintI64(char *msg, int64_t value) {
    std::cout << "debug: " << msg << " " << value << std::endl;
}

void _cellPrint(char *start, char *end) {
    using namespace std;
    if (end<start)
        std::cout << "illegal start/end" << endl;

    std::string s = tuplex::fromCharPointers(start, end);

    std::cout << "Cell: " << s << endl;
}
}

namespace tuplex {
    namespace codegen {

        void LLVMEnvironment::init(const std::string &moduleName) {

            initLLVM();

            // init module
            _module.reset(new llvm::Module(moduleName, _context));

            // fetch for executing process the target machine & set data layout to it
            llvm::TargetMachine *TM = llvm::EngineBuilder().selectTarget();
            assert(TM);
            _module->setDataLayout(TM->createDataLayout());

            if (TM)
                delete TM;
            TM = nullptr;

            // setup defaults in typeMapping (ignore bool)
            _typeMapping[llvm::Type::getDoubleTy(_context)] = python::Type::F64;
            _typeMapping[llvm::Type::getInt64Ty(_context)] = python::Type::I64;

            // set up global init/release blocks

            // create functions
            FunctionType *globalFuncType = llvm::FunctionType::get(i64Type(), false);

            auto initGlobalFunc = llvm::Function::Create(globalFuncType, llvm::Function::ExternalLinkage, "initGlobal", _module.get());
            auto releaseGlobalFunc = llvm::Function::Create(globalFuncType, llvm::Function::ExternalLinkage, "releaseGlobal", _module.get());

            // create entry blocks
            _initGlobalEntryBlock = BasicBlock::Create(_context, "initGlobalEntry", initGlobalFunc);
            _releaseGlobalEntryBlock = BasicBlock::Create(_context, "releaseGlobalEntry", releaseGlobalFunc);
            // create return blocks
            _initGlobalRetBlock = BasicBlock::Create(_context, "initGlobalReturn", initGlobalFunc);
            _releaseGlobalRetBlock = BasicBlock::Create(_context, "releaseGlobalReturn", releaseGlobalFunc);

            // create local variables to hold return value
            llvm::IRBuilder<> builder(_context);
            builder.SetInsertPoint(_initGlobalEntryBlock);
            _initGlobalRetValue = builder.CreateAlloca(i64Type());
            builder.CreateStore(i64Const(0), _initGlobalRetValue);
            builder.CreateCondBr(builder.CreateICmpNE(builder.CreateLoad(_initGlobalRetValue), i64Const(0)), _initGlobalRetBlock, _initGlobalRetBlock);

            builder.SetInsertPoint(_releaseGlobalEntryBlock);
            _releaseGlobalRetValue = builder.CreateAlloca(i64Type());
            builder.CreateStore(i64Const(0), _releaseGlobalRetValue);
            builder.CreateCondBr(builder.CreateICmpNE(builder.CreateLoad(_releaseGlobalRetValue), i64Const(0)), _releaseGlobalRetBlock, _releaseGlobalRetBlock);

            // create return statement
            builder.SetInsertPoint(_initGlobalRetBlock);
            builder.CreateRet(builder.CreateLoad(_initGlobalRetValue));
            builder.SetInsertPoint(_releaseGlobalRetBlock);
            builder.CreateRet(builder.CreateLoad(_releaseGlobalRetValue));
        }

        llvm::IRBuilder<> LLVMEnvironment::getInitGlobalBuilder(const std::string &block_name) {
            // get the successor block
            auto globalEntryTerminator = llvm::dyn_cast<llvm::BranchInst>(_initGlobalEntryBlock->getTerminator());
            auto successorBlock = globalEntryTerminator->getSuccessor(1); // the block if ret == 0
            // create a new block in the init function
            auto initGlobalFunc = _initGlobalEntryBlock->getParent();
            auto newBlock = BasicBlock::Create(_context, block_name + "_block", initGlobalFunc, successorBlock);
            auto retBuilder = llvm::IRBuilder<>(newBlock);
            // insert the new block in between the entry block and it's successor
            globalEntryTerminator->setSuccessor(1, newBlock);
            auto loadInst = retBuilder.CreateLoad(_initGlobalRetValue);
            retBuilder.CreateCondBr(retBuilder.CreateICmpNE(loadInst, i64Const(0)), _initGlobalRetBlock, successorBlock);

            // return a builder
            retBuilder.SetInsertPoint(loadInst);
            return retBuilder;
        }

        llvm::IRBuilder<> LLVMEnvironment::getReleaseGlobalBuilder(const std::string &block_name) {
            // get the successor block
            auto globalEntryTerminator = llvm::dyn_cast<llvm::BranchInst>(_releaseGlobalEntryBlock->getTerminator());
            auto successorBlock = globalEntryTerminator->getSuccessor(1); // the block if ret == 0
            // create a new block in the release function
            auto releaseGlobalFunc = _releaseGlobalEntryBlock->getParent();
            auto newBlock = BasicBlock::Create(_context, block_name + "_block", releaseGlobalFunc, successorBlock);
            auto retBuilder = llvm::IRBuilder<>(newBlock);
            // insert the new block in between the entry block and it's successor
            globalEntryTerminator->setSuccessor(1, newBlock);
            auto loadInst = retBuilder.CreateLoad(_releaseGlobalRetValue);
            retBuilder.CreateCondBr(retBuilder.CreateICmpNE(loadInst, i64Const(0)), _releaseGlobalRetBlock, successorBlock);

            // return a builder
            retBuilder.SetInsertPoint(loadInst);
            return retBuilder;
        }

        void LLVMEnvironment::release() {

        }

        size_t calcBitmapElementCount(const python::Type &tupleType) {
            assert(tupleType.isTupleType());
            size_t num = 0;
            for (auto t : tupleType.parameters())
                num += t.isOptionType();

            return core::ceilToMultiple(num, 64ul) / 64;
        }

        // this function itself is super-slow, so cache its results
        static std::unordered_map<int, std::vector<std::tuple<unsigned, unsigned, unsigned>>> _tupleIndexCache;

        std::tuple<size_t, size_t, size_t> getTupleIndices(const python::Type &tupleType, size_t index) {

            // find cache
            auto it = _tupleIndexCache.find(tupleType.hash());
            if (it == _tupleIndexCache.end()) {
                assert(tupleType.isTupleType());
                assert(index < tupleType.parameters().size());

                // compute entry

                _tupleIndexCache[tupleType.hash()] = std::vector<std::tuple<unsigned, unsigned, unsigned>>(
                        tupleType.parameters().size());

                for (int j = 0; j < tupleType.parameters().size(); ++j) {
                    // compute offsets
                    unsigned int sizeOffset = 0;
                    unsigned int valueOffset = 0;
                    unsigned int bitmapPos = 0;
                    sizeOffset += tupleType.isOptional(); // +1 for bitmap array b/c first element is then vector type!
                    valueOffset += tupleType.isOptional(); // +1 for bitmap array

                    // all the serialized elements
                    for (int i = 0; i < tupleType.parameters().size(); ++i) {
                        auto paramType = tupleType.parameters()[i];

                        auto is_in_fixed_len = !(paramType.isSingleValued() || (paramType.isOptionType() && paramType.getReturnType().isSingleValued()));
                        // always inc sizeoffset
                        if(is_in_fixed_len)
                            sizeOffset++; // if only single value possible for type, no need to represent

                        if(i < j && is_in_fixed_len)
                            valueOffset++;

                        if (i < j && paramType.isOptionType())
                            bitmapPos++;

                        // increase varlen offset as long as j not reached
                        if (i < j && (python::Type::STRING == paramType.withoutOptions() ||
                                      python::Type::GENERICDICT == paramType.withoutOptions() ||
                                      paramType.withoutOptions().isDictionaryType() ||
                                      (paramType.withoutOptions().isListType() && paramType.withoutOptions() != python::Type::EMPTYLIST && !paramType.withoutOptions().elementType().isSingleValued()))) // TODO: this should probably be generalized to type.isVarLen() or something
                            sizeOffset++;
                    }

                    _tupleIndexCache[tupleType.hash()][j] = std::make_tuple(valueOffset, sizeOffset, bitmapPos);
                }
            }

            assert(index < _tupleIndexCache[tupleType.hash()].size());

            // return entry
            return _tupleIndexCache[tupleType.hash()][index];
        }

        llvm::Type *LLVMEnvironment::createTupleStructType(const python::Type &type, const std::string &twine) {
            using namespace llvm;

            python::Type T = python::Type::propagateToTupleType(type);
            assert(T.isTupleType());

            auto &ctx = _context;
            auto size_field_type = llvm::Type::getInt64Ty(ctx); // what type to use for size fields.

            bool packed = false;

            // empty tuple?
            // is special type
            if (type.parameters().size() == 0) {
                llvm::ArrayRef<llvm::Type *> members;
                llvm::Type *structType = llvm::StructType::create(ctx, members, "emptytuple", packed);

                // add to mapping (make sure it doesn't exist yet!)
                assert(_typeMapping.find(structType) == _typeMapping.end());
                _typeMapping[structType] = type;
                return structType;
            }

            assert(type.parameters().size() > 0);
            // define type
            std::vector<llvm::Type *> memberTypes;

            auto params = type.parameters();
            // count optional elements
            int numNullables = 0;
            for (int i = 0; i < params.size(); ++i) {
                if (params[i].isOptionType()) {
                    numNullables++;
                    params[i] = params[i].withoutOptions();
                }

                // empty tuple is ok, b.c. it's a primitive
                assert(!params[i].isTupleType() ||
                       params[i] == python::Type::EMPTYTUPLE); // no nesting at this level here supported!
            }

            // first, create bitmap as array of i1
            if (numNullables > 0) {
                // i1 array!
                memberTypes.emplace_back(ArrayType::get(Type::getInt1Ty(ctx), numNullables));
            }

            // size fields at end
            int numVarlenFields = 0;

            // define bitmap on the fly
            for (const auto &el: T.parameters()) {
                auto t = el.isOptionType() ? el.getReturnType() : el; // get rid of most outer options

                // @TODO: special case empty tuple! also doesn't need to be represented

                if (python::Type::BOOLEAN == t) {
                    // i8
                    memberTypes.push_back(getBooleanType());
                } else if (python::Type::I64 == t) {
                    // i64
                    memberTypes.push_back(i64Type());
                } else if (python::Type::F64 == t) {
                    // double
                    memberTypes.push_back(llvm::Type::getDoubleTy(ctx));
                } else if (python::Type::STRING == t) {
                    memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
                    numVarlenFields++;
                } else if (python::Type::PYOBJECT == t) {
                    memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
                    numVarlenFields++;
                } else if ((python::Type::GENERICDICT == t || t.isDictionaryType()) && t != python::Type::EMPTYDICT) { // dictionary
                    memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
                    numVarlenFields++;
                } else if (t.isSingleValued()) {
                    // leave out. Not necessary to represent it in memory.
                } else if(t.isListType()) {
                    memberTypes.push_back(getListType(t));
                    if(!t.elementType().isSingleValued()) numVarlenFields++;
                } else {
                    // nested tuple?
                    // ==> do lookup!
                    // add i64 (for length)
                    // and pointer type
                    // previously defined? => get!
                    if (t.isTupleType()) {
                        // recurse!
                        // add struct into it (can be accessed via recursion then!!!)
                        memberTypes.push_back(getOrCreateTupleType(t, twine));
                    } else {
                        Logger::instance().logger("codegen").error(
                                "not supported type " + el.desc() + " encountered in LLVM struct type creation");
                        return nullptr;
                    }
                }
            }

            for (int i = 0; i < numVarlenFields; ++i)
                memberTypes.emplace_back(size_field_type); // 64 bit int as size

            llvm::ArrayRef<llvm::Type *> members(memberTypes);
            llvm::Type *structType = llvm::StructType::create(ctx, members, "struct." + twine, packed);

            // // add to mapping (make sure it doesn't exist yet!)
            assert(_typeMapping.find(structType) == _typeMapping.end());
            _typeMapping[structType] = type;

            return structType;
        }

        llvm::Type *LLVMEnvironment::getListType(const python::Type &listType, const std::string &twine) {
            if(listType == python::Type::EMPTYLIST) return i8ptrType(); // dummy type
            auto it = _generatedListTypes.find(listType);
            if(_generatedListTypes.end() != it) {
                return it->second;
            }

            assert(listType.isListType() && listType != python::Type::EMPTYLIST);

            auto elementType = listType.elementType();
            llvm::Type *retType;
            if(elementType.isSingleValued()) {
                retType = i64Type();
            } else if(elementType == python::Type::I64 || elementType == python::Type::F64 || elementType == python::Type::BOOLEAN || elementType.isTupleType()) {
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                // array
                // TODO: probably can replace with just llvm::PointerType::get(pythonToLLVMType(elementType), 0)
                if(elementType == python::Type::I64) {
                    memberTypes.push_back(i64ptrType());
                } else if(elementType == python::Type::F64) {
                    memberTypes.push_back(doublePointerType());
                } else if(elementType == python::Type::BOOLEAN) {
                    memberTypes.push_back(getBooleanPointerType());
                } else if(elementType.isTupleType()) {
                    if(elementType.isFixedSizeType()) {
                        // list stores the actual tuple structs
                        memberTypes.push_back(llvm::PointerType::get(getOrCreateTupleType(flattenedType(elementType)), 0));
                    } else {
                        // list stores pointers to tuple structs
                        memberTypes.push_back(llvm::PointerType::get(llvm::PointerType::get(getOrCreateTupleType(flattenedType(elementType)), 0), 0));
                    }
                }
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else if(elementType == python::Type::STRING || elementType.isDictionaryType()) {
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                memberTypes.push_back(llvm::PointerType::get(i8ptrType(), 0)); // str array
                memberTypes.push_back(i64ptrType()); // strlen array (with the +1 for \0)
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else {
                throw std::runtime_error("Unsupported list element type: " + listType.desc());
            }

            _generatedListTypes[listType] = retType;
            return retType;
        }

        llvm::Type *LLVMEnvironment::createOrGetIteratorType(const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto iteratorName = iteratorInfo->iteratorName;
            auto argsType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;
            if(iteratorName == "iter") {
                if(argsType.isIteratorType()) {
                    return createOrGetIteratorType(argsIteratorInfo.front());
                } else {
                    // arg is of type list, tuple, string, or range
                    return createOrGetIterIteratorType(argsType);
                }
            } else if(iteratorName == "reversed") {
                return createOrGetReversedIteratorType(argsType);
            } else if(iteratorName == "zip") {
                return createOrGetZipIteratorType(argsType, argsIteratorInfo);
            } else if(iteratorName == "enumerate") {
                return createOrGetEnumerateIteratorType(argsType, argsIteratorInfo.front());
            }
            throw std::runtime_error("cannot get iterator type for" + iteratorName);
            return nullptr;
        }

        llvm::Type *LLVMEnvironment::createOrGetIterIteratorType(const python::Type &iterableType, const std::string &twine) {
            using namespace llvm;

            if(iterableType == python::Type::EMPTYLIST || iterableType == python::Type::EMPTYTUPLE) {
                // use dummy type for empty iterator
                return i64Type();
            }

            std::string iteratorName;
            std::vector<llvm::Type*> memberTypes;
            // iter iterator struct: { pointer to block address (i8*), current index (i64 for range otherwise i32), pointer to iterable struct type,
            // iterable length (for string and tuple)}
            memberTypes.push_back(llvm::Type::getInt8PtrTy(_context, 0));
            if(iterableType == python::Type::RANGE) {
                iteratorName = "range_";
                memberTypes.push_back(llvm::Type::getInt64Ty(_context));
                memberTypes.push_back(llvm::PointerType::get(getRangeObjectType(), 0));
            } else {
                memberTypes.push_back(llvm::Type::getInt32Ty(_context));
                if(iterableType.isListType()) {
                    iteratorName = "list_";
                    memberTypes.push_back(llvm::PointerType::get(getListType(iterableType), 0));
                } else if(iterableType == python::Type::STRING) {
                    iteratorName = "str_";
                    memberTypes.push_back(llvm::Type::getInt8PtrTy(_context, 0));
                    memberTypes.push_back(llvm::Type::getInt64Ty(_context));
                } else if(iterableType.isTupleType()) {
                    iteratorName = "tuple_";
                    memberTypes.push_back(llvm::PointerType::get(getOrCreateTupleType(flattenedType(iterableType)), 0));
                    memberTypes.push_back(llvm::Type::getInt64Ty(_context));
                } else {
                    throw std::runtime_error("unsupported iterable type" + iterableType.desc());
                }
            }

            auto it = _generatedIteratorTypes.find(memberTypes);
            if(_generatedIteratorTypes.end() != it) {
                return it->second;
            }

            llvm::ArrayRef<llvm::Type*> members(memberTypes);
            auto iteratorContextType = llvm::StructType::create(_context, members, "struct." + iteratorName + twine);
            _generatedIteratorTypes[memberTypes] = iteratorContextType;
            return iteratorContextType;
        }

        llvm::Type *LLVMEnvironment::createOrGetReversedIteratorType(const python::Type &argType, const std::string &twine) {
            using namespace llvm;

            if(argType == python::Type::EMPTYLIST || argType == python::Type::EMPTYTUPLE) {
                // use dummy type for empty iterator
                return i64Type();
            }

            if(argType == python::Type::RANGE) {
                return createOrGetIterIteratorType(argType);
            }

            std::string iteratorName;
            std::vector<llvm::Type*> memberTypes;
            // iter iterator struct: { pointer to block address (i8*), current index (i64 for range otherwise i32), pointer to arg object struct type,
            // iterable length (for string and tuple)}
            memberTypes.push_back(llvm::Type::getInt8PtrTy(_context, 0));
            memberTypes.push_back(llvm::Type::getInt32Ty(_context));
            if(argType.isListType()) {
                iteratorName = "list_";
                memberTypes.push_back(llvm::PointerType::get(getListType(argType), 0));
            } else if(argType == python::Type::STRING) {
                iteratorName = "str_";
                memberTypes.push_back(llvm::Type::getInt8PtrTy(_context, 0));
            } else if(argType.isTupleType()) {
                iteratorName = "tuple_";
                memberTypes.push_back(llvm::PointerType::get(getOrCreateTupleType(flattenedType(argType)), 0));
            } else {
                throw std::runtime_error("unsupported argument type for reversed()" + argType.desc());
            }

            auto it = _generatedIteratorTypes.find(memberTypes);
            if(_generatedIteratorTypes.end() != it) {
                return it->second;
            }

            llvm::ArrayRef<llvm::Type*> members(memberTypes);
            auto iteratorContextType = llvm::StructType::create(_context, members, "struct." + iteratorName + twine);
            _generatedIteratorTypes[memberTypes] = iteratorContextType;
            return iteratorContextType;
        }

        llvm::Type *LLVMEnvironment::createOrGetZipIteratorType(const python::Type &argsType, const std::vector<std::shared_ptr<IteratorInfo>> &argsIteratorInfo, const std::string &twine) {
            using namespace llvm;

            if(argsType.parameters().empty()) {
                // no argument provided, zip is an empty iterator
                // use dummy type for empty iterator
                return i64Type();
            }

            std::vector<llvm::Type*> memberTypes;
            // zip iterator struct: { pointer to each child iterator struct type }
            for (int i = 0; i < argsType.parameters().size(); ++i) {
                auto iterableType = argsType.parameters()[i];
                if(iterableType == python::Type::EMPTYITERATOR || iterableType == python::Type::EMPTYLIST || iterableType == python::Type::EMPTYTUPLE) {
                    // if any argument is empty, zip returns an empty iterator.
                    // use dummy type for empty iterator
                    return i64Type();
                }
                assert(iterableType.isIterableType());
                if(iterableType.isIteratorType()) {
                    memberTypes.push_back(llvm::PointerType::get(createOrGetIteratorType(argsIteratorInfo[i]), 0));
                } else {
                    memberTypes.push_back(llvm::PointerType::get(createOrGetIterIteratorType(iterableType), 0));
                }
            }

            auto it = _generatedIteratorTypes.find(memberTypes);
            if(_generatedIteratorTypes.end() != it) {
                return it->second;
            }

            llvm::ArrayRef<llvm::Type*> members(memberTypes);
            auto iteratorContextType = llvm::StructType::create(_context, members, "struct." + twine);
            _generatedIteratorTypes[memberTypes] = iteratorContextType;
            return iteratorContextType;
        }

        llvm::Type *LLVMEnvironment::createOrGetEnumerateIteratorType(const python::Type &argType, const std::shared_ptr<IteratorInfo> &argIteratorInfo, const std::string &twine) {
            using namespace llvm;

            if(argType == python::Type::EMPTYITERATOR || argType == python::Type::EMPTYLIST || argType == python::Type::EMPTYTUPLE) {
                // use dummy type for empty iterator
                return i64Type();
            }

            std::vector<llvm::Type*> memberTypes;
            // zip iterator struct: { count (i64), pointer to the child iterator struct type }
            memberTypes.push_back(llvm::Type::getInt64Ty(_context));
            if(argType.isIteratorType()) {
                memberTypes.push_back(llvm::PointerType::get(createOrGetIteratorType(argIteratorInfo), 0));
            } else {
                memberTypes.push_back(llvm::PointerType::get(createOrGetIterIteratorType(argType), 0));
            }

            auto it = _generatedIteratorTypes.find(memberTypes);
            if(_generatedIteratorTypes.end() != it) {
                return it->second;
            }

            llvm::ArrayRef<llvm::Type*> members(memberTypes);
            auto iteratorContextType = llvm::StructType::create(_context, members, "struct." + twine);
            _generatedIteratorTypes[memberTypes] = iteratorContextType;
            return iteratorContextType;
        }


        SerializableValue
        LLVMEnvironment::extractTupleElement(llvm::IRBuilder<> &builder, const python::Type &tupleType,
                                             llvm::Value *tupleVal, unsigned int index) {

            using namespace llvm;
            assert(!tupleVal->getType()->isPointerTy());

            auto &ctx = builder.getContext();
            auto elementType = tupleType.parameters()[index];

            // special types (not serialized in memory, i.e. constants to be constructed from typing)
            if (python::Type::NULLVALUE == elementType)
                return SerializableValue(nullptr, nullptr, llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx),
                                                                                           llvm::APInt(1, true)));

            // empty tuple, empty dict
            if (python::Type::EMPTYTUPLE == elementType) {
                // special case, just empty object
                return SerializableValue(nullptr, nullptr, nullptr);
            }

            if (python::Type::EMPTYDICT == elementType) {
                // create empty dict (i.e. i8* pointer with cJSON)
                // note, this could be directly done as str const!
                auto ret = builder.CreateCall(cJSONCreateObject_prototype(ctx, _module.get()), {});
                assert(ret->getType()->isPointerTy());
                auto cjsonstr = builder.CreateCall(
                        cJSONPrintUnformatted_prototype(ctx, _module.get()),
                        {ret});
                auto size = builder.CreateAdd(
                        builder.CreateCall(strlen_prototype(ctx, _module.get()), {cjsonstr}),
                        i64Const(1));
                return SerializableValue{ret, size};
            }

            if(python::Type::EMPTYLIST == elementType) {
                return SerializableValue(nullptr, nullptr, nullptr);
            }

            auto indices = getTupleIndices(tupleType, index);
            unsigned int valueOffset = std::get<0>(indices);
            unsigned int sizeOffset = std::get<1>(indices);
            auto bitmapPos = std::get<2>(indices);


            auto i64Zero = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 0));
            auto i64Size = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                                           llvm::APInt(64, sizeof(int64_t)));

            // optional? ==> extract bit from struct type
            Value *value = nullptr;
            Value *size = nullptr;
            Value *isnull = nullptr;
            if (elementType.isOptionType()) {

                auto tv_type = getLLVMTypeName(tupleVal->getType());

                // // extract bit (pos)
                auto structBitmapIdx = builder.CreateExtractValue(tupleVal, {0});

                auto struct_ty = getLLVMTypeName(structBitmapIdx->getType());

                assert(structBitmapIdx->getType()->isAggregateType());
                assert(bitmapPos < structBitmapIdx->getType()->getArrayNumElements());
                isnull = builder.CreateExtractValue(structBitmapIdx, {static_cast<unsigned int>(bitmapPos)});
                // empty tuple, empty dict
                if(python::Type::EMPTYTUPLE == elementType.getReturnType()) {
                    // special case, just empty object
                    return SerializableValue(nullptr, nullptr, isnull);
                }

                if(python::Type::EMPTYDICT == elementType.getReturnType()) {
                    // create empty dict (i.e. i8* pointer with cJSON)
                    // note, this could be directly done as str const!
                    auto ret = builder.CreateCall(cJSONCreateObject_prototype(ctx, _module.get()), {});
                    assert(ret->getType()->isPointerTy());
                    auto cjsonstr = builder.CreateCall(
                            cJSONPrintUnformatted_prototype(ctx, _module.get()),
                            {ret});
                    auto size = builder.CreateAdd(
                            builder.CreateCall(strlen_prototype(ctx, _module.get()), {cjsonstr}),
                            i64Const(1));
                    return SerializableValue(ret, size, isnull);
                }
            }

            // extract elements
            value = builder.CreateExtractValue(tupleVal, {valueOffset});

            // size existing? ==> only for varlen types
            if (!elementType.isFixedSizeType()) {
                size = builder.CreateExtractValue(tupleVal, {sizeOffset});
            } else {
                // size from type
                size = i64Size;
            }

            return SerializableValue(value, size, isnull);
        }

        SerializableValue LLVMEnvironment::getTupleElement(llvm::IRBuilder<> &builder, const python::Type &tupleType,
                                                           llvm::Value *tuplePtr, unsigned int index) {
            using namespace llvm;

            assert(tuplePtr->getType()->isPointerTy()); // must be a pointer
            assert(tupleType.isTupleType());
            assert(tupleType.parameters().size() > index);

            auto& ctx = builder.getContext();
            auto elementType = tupleType.parameters()[index];

            // special types (not serialized in memory, i.e. constants to be constructed from typing)
            if(python::Type::NULLVALUE == elementType)
                return SerializableValue(nullptr, nullptr, llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, true)));

            // empty tuple, empty dict
            if(python::Type::EMPTYTUPLE == elementType) {
                // special case, just empty object
                return SerializableValue(nullptr, nullptr, nullptr);
            }

            if(python::Type::EMPTYDICT == elementType) {
                // create empty dict (i.e. i8* pointer with cJSON)
                // note, this could be directly done as str const!
                auto ret = builder.CreateCall(cJSONCreateObject_prototype(ctx, _module.get()), {});
                assert(ret->getType()->isPointerTy());
                auto cjsonstr = builder.CreateCall(
                        cJSONPrintUnformatted_prototype(ctx, _module.get()),
                        {ret});
                auto size = builder.CreateAdd(
                        builder.CreateCall(strlen_prototype(ctx, _module.get()), {cjsonstr}),
                        i64Const(1));
                return SerializableValue{ret, size};
            }

            if(python::Type::EMPTYLIST == elementType) {
                return SerializableValue(nullptr, nullptr, nullptr);
            }

            // ndebug: safety check, right llvm type!
            //assert(tuplePtr->getType() == createStructType(ctx, tupleType, "tuple")->getPointerTo()); ??

            auto indices = getTupleIndices(tupleType, index);
            auto valueOffset = std::get<0>(indices);
            auto sizeOffset = std::get<1>(indices);
            auto bitmapPos = std::get<2>(indices);


            auto i64Zero = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 0));
            auto i64Size = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                                           llvm::APInt(64, sizeof(int64_t)));

            // optional? ==> extract bit from struct type
            Value *value = nullptr;
            Value *size = nullptr;
            Value *isnull = nullptr;
            if (elementType.isOptionType()) {
                // // extract bit (pos)
                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
                // auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0, bitmapPos / 64);
                // auto bitmapElement = builder.CreateLoad(bitmapIdx);
                // isnull = builder.CreateICmpNE(i64Zero, builder.CreateAnd(bitmapElement, 0x1ul << (bitmapPos % 64)));

                // i1 array extract (easier)
                // LLVM 9 API here...
                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
                auto structBitmapIdx = CreateStructGEP(builder, tuplePtr, 0); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0, bitmapPos);
                isnull = builder.CreateLoad(bitmapIdx);
            }

            // remove option
            elementType = elementType.withoutOptions();

            // empty tuple, empty dict
            if (python::Type::EMPTYTUPLE == elementType) {
                // special case, just empty object
                return SerializableValue(nullptr, nullptr, isnull);
            }

            if (python::Type::EMPTYDICT == elementType) {
                // create empty dict (i.e. i8* pointer with cJSON)
                // note, this could be directly done as str const!
                auto ret = builder.CreateCall(cJSONCreateObject_prototype(ctx, _module.get()), {});
                assert(ret->getType()->isPointerTy());
                auto cjsonstr = builder.CreateCall(
                        cJSONPrintUnformatted_prototype(ctx, _module.get()),
                        {ret});
                auto size = builder.CreateAdd(
                        builder.CreateCall(strlen_prototype(ctx, _module.get()), {cjsonstr}),
                        i64Const(1));
                return SerializableValue{ret, size, isnull};
            }



            // extract elements
            // auto structValIdx = builder.CreateStructGEP(tuplePtr, valueOffset);
            auto structValIdx = CreateStructGEP(builder, tuplePtr, valueOffset);
            value = builder.CreateLoad(structValIdx);

            // size existing? ==> only for varlen types
            if (!elementType.isFixedSizeType()) {
                //  auto structSizeIdx = builder.CreateStructGEP(tuplePtr, sizeOffset);
                auto structSizeIdx = CreateStructGEP(builder, tuplePtr, sizeOffset);
                size = builder.CreateLoad(structSizeIdx);
            } else {
                // size from type
                size = i64Size;
            }

            return SerializableValue(value, size, isnull);
        }

        void LLVMEnvironment::setTupleElement(llvm::IRBuilder<> &builder, const python::Type &tupleType,
                                              llvm::Value *tuplePtr, unsigned int index,
                                              const SerializableValue &value) {
            using namespace llvm;

            assert(tupleType.isTupleType());
            assert(tupleType.parameters().size() > index);

            auto &ctx = builder.getContext();
            auto elementType = tupleType.parameters()[index];

            // special types which don't need to be stored because the type determines the value
            if (elementType.isSingleValued())
                return;

            // ndebug: safety check, right llvm type!
            //assert(tuplePtr->getType() == createStructType(ctx, tupleType, "tuple")->getPointerTo()); ??

            auto indices = getTupleIndices(tupleType, index);
            auto valueOffset = std::get<0>(indices);
            auto sizeOffset = std::get<1>(indices);
            auto bitmapPos = std::get<2>(indices);

            // optional? ==> set bit for struct type
            if (elementType.isOptionType() && value.is_null) {
                // // 64 bit bitmap logic
                // // extract bit (pos)
                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
                // auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0, bitmapPos / 64);

                // i1 array logic
                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
                auto structBitmapIdx = CreateStructGEP(builder, tuplePtr, 0ull); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                builder.CreateStore(value.is_null, bitmapIdx);
            }

            // get rid off option & check again for special emptytuple/emptydict/...
            elementType = elementType.withoutOptions();

            if(elementType.isSingleValued())
                return; // do not need to store, but bitmap is stored for them already.

            // extract elements
            // auto structValIdx = builder.CreateStructGEP(tuplePtr, valueOffset);
            auto structValIdx = CreateStructGEP(builder, tuplePtr, valueOffset);
            if (value.val)
                builder.CreateStore(value.val, structValIdx);

            // size existing? ==> only for varlen types
            if (!elementType.isFixedSizeType()) {
                // auto structSizeIdx = builder.CreateStructGEP(tuplePtr, sizeOffset);
                auto structSizeIdx = CreateStructGEP(builder, tuplePtr, sizeOffset);
                if (value.size)
                    builder.CreateStore(value.size, structSizeIdx);
            }
        }


        llvm::Value *LLVMEnvironment::truthValueTest(llvm::IRBuilder<> &builder, const SerializableValue &val,
                                                     const python::Type &type) {
            // from the offical python documentation:
            // Truth Value Testing
            // Any object can be tested for truth value, for use in an if or while condition or as operand
            // of the Boolean operations below.
            // By default, an object is considered true unless its class defines either a __bool__() method
            // that returns False or a __len__() method that returns zero, when called with the object.
            // ----
            // Here are most of the built-in objects considered false:
            // constants defined to be false: None and False.
            // zero of any numeric type: 0, 0.0, 0j, Decimal(0), Fraction(0, 1)
            // empty sequences and collections: '', (), [], {}, set(), range(0)
            // Operations and built-in functions that have a Boolean result always return 0 or False for false
            // and 1 or True for true, unless otherwise stated. (Important exception: the Boolean operations or and
            // and always return one of their operands.)
            using namespace llvm;

            // ==> return i1 directly
            if (val.val && val.val->getType() == i1Type())
                return val.val;

            // (), {}, None, ... are all False
            if (python::Type::NULLVALUE == type || python::Type::EMPTYTUPLE == type || python::Type::EMPTYDICT == type || python::Type::EMPTYLIST == type)
                return i1Const(false);

            if (python::Type::BOOLEAN == type) {
                assert(val.val && val.val->getType() == getBooleanType());
                return booleanToCondition(builder, val.val);
            }

            // numeric types?
            if (python::Type::I64 == type) {
                assert(val.val && val.val->getType() == i64Type());

                // i64 compare
                return builder.CreateICmpNE(val.val, i64Const(0));
            }

            if (python::Type::F64 == type) {
                assert(val.val && val.val->getType() == doubleType());

                // f64 compare
                return builder.CreateFCmpONE(val.val, f64Const(0.0));
            }

            // string type? compare against ''!
            if (python::Type::STRING == type) {
                assert(val.val && val.val->getType() == i8ptrType());
                assert(val.size && val.size->getType() == i64Type());

                // here is the bug: without basic blocks, in select this gets executed...
                // debugPrint(builder, "executing strcmp against emptystr with: ", val.val);

                auto strcmp_res = fixedSizeStringCompare(builder, val.val, "", true);
                // i1 flip (DO NOT USE CreateNeg here!!!)
                return builder.CreateNot(strcmp_res);
            }

            // tuple type? always true! (except for nulltuple) - also true for match object
            if (type.isTupleType() || type.isDictionaryType() || type == python::Type::MATCHOBJECT || type.isListType()) {
                assert(type != python::Type::EMPTYTUPLE);
                assert(type != python::Type::EMPTYDICT);
                assert(type != python::Type::EMPTYLIST);
                return i1Const(true);
            }

            // option type? ==> if blocks?
            if (type.isOptionType()) {

                // if val.is_null:
                //    false
                // else:
                //    val.val

                // minimal godbolt.org example
                // #include <cstring>
                //
                //bool compare(const char *ptr) {
                //    return ptr ? strcmp(ptr, "") : false;
                //}

                // use phi block logic here!
                // ==> cf. -O3 godbolt.org!
                auto curBlock = builder.GetInsertBlock();
                assert(curBlock);

                // create new block for testing and new block to continue
                BasicBlock *bbTest = BasicBlock::Create(builder.getContext(), "test",
                                                        builder.GetInsertBlock()->getParent());
                BasicBlock *bbTestDone = BasicBlock::Create(builder.getContext(), "test_done",
                                                            builder.GetInsertBlock()->getParent());


                // then create phi node. ==> if None, ret false. I.e. when coming from curBlock
                // else, ret the result of the truth test in the newly created block!
                builder.CreateCondBr(val.is_null, bbTestDone, bbTest);
                builder.SetInsertPoint(bbTest);
                Value *testValue = truthValueTest(builder, SerializableValue(val.val, val.size), type.withoutOptions());
                builder.CreateBr(bbTestDone);

                // set insert point
                builder.SetInsertPoint(bbTestDone);

                // phi node as result
                auto phiNode = builder.CreatePHI(i1Type(), 2);
                phiNode->addIncoming(i1Const(false), curBlock);
                phiNode->addIncoming(testValue, bbTest);
                return phiNode;
            }

            throw std::runtime_error("unsupported type for truth testing found: " + type.desc());
            return nullptr;
        }


        llvm::Value *LLVMEnvironment::CreateTernaryLogic(llvm::IRBuilder<> &builder, llvm::Value *condition,
                                                         std::function<llvm::Value *(
                                                                 llvm::IRBuilder<> &)> ifBlock,
                                                         std::function<llvm::Value *(
                                                                 llvm::IRBuilder<> &)> elseBlock) {

            using namespace llvm;
            assert(condition);
            assert(condition->getType() == i1Type());
            auto &ctx = builder.getContext();
            auto parent = builder.GetInsertBlock()->getParent();

            // phi logic like in truth value testing

            BasicBlock *bbIf = BasicBlock::Create(ctx, "if", parent);
            BasicBlock *bbElse = BasicBlock::Create(ctx, "else", parent);
            BasicBlock *bbIfElseDone = BasicBlock::Create(ctx, "if_else_done", parent);

            // branch on condition
            builder.CreateCondBr(condition, bbIf, bbElse);

            // execute if block
            builder.SetInsertPoint(bbIf);
            Value *ifValue = ifBlock(builder);
            assert(ifValue);
            bbIf = builder.GetInsertBlock(); // ifBlock may have had multiple BBs
            builder.CreateBr(bbIfElseDone);

            // execute else block
            builder.SetInsertPoint(bbElse);
            Value *elseValue = elseBlock(builder);
            assert(elseValue);
            assert(ifValue->getType() == elseValue->getType());
            bbElse = builder.GetInsertBlock(); // elseBlock may have had multiple BBs
            builder.CreateBr(bbIfElseDone);

            // done block, phi based value
            builder.SetInsertPoint(bbIfElseDone);
            auto phiNode = builder.CreatePHI(ifValue->getType(), 2);
            phiNode->addIncoming(ifValue, bbIf);
            phiNode->addIncoming(elseValue, bbElse);

            return phiNode;
        }

        llvm::Value *LLVMEnvironment::malloc(llvm::IRBuilder<> &builder, llvm::Value *size) {

            // make sure size_t is 64bit
            static_assert(sizeof(size_t) == sizeof(int64_t), "sizeof must be 64bit compliant");
            static_assert(sizeof(size_t) == 8, "sizeof must be 64bit wide");
            assert(size->getType() == llvm::Type::getInt64Ty(_context));


            // create external call to rtmalloc function
            auto func = _module.get()->getOrInsertFunction("rtmalloc", llvm::Type::getInt8PtrTy(_context, 0),
                                                           llvm::Type::getInt64Ty(_context));
            return builder.CreateCall(func, size);
        }

        llvm::Value* LLVMEnvironment::cmalloc(llvm::IRBuilder<> &builder, llvm::Value *size) {
            using namespace llvm;

            // make sure size_t is 64bit
            static_assert(sizeof(size_t) == sizeof(int64_t), "sizeof must be 64bit compliant");
            static_assert(sizeof(size_t) == 8, "sizeof must be 64bit wide");
            assert(size->getType() == llvm::Type::getInt64Ty(_context));

            // create external call to rtmalloc function
            Function* func = cast<Function>(_module.get()->getOrInsertFunction("malloc", llvm::Type::getInt8PtrTy(_context, 0),
                                                           llvm::Type::getInt64Ty(_context)).getCallee());

            func->addFnAttr(Attribute::NoUnwind); // like in godbolt
            return builder.CreateCall(func, size);
        }

        llvm::Value* LLVMEnvironment::cfree(llvm::IRBuilder<> &builder, llvm::Value *ptr) {
            using namespace llvm;

            assert(ptr);

            // create external call to rtmalloc function
            Function* func = cast<Function>(_module.get()->getOrInsertFunction("free",
                    llvm::Type::getVoidTy(_context), llvm::Type::getInt8PtrTy(_context, 0)).getCallee());

            func->addFnAttr(Attribute::NoUnwind); // like in godbolt
            return builder.CreateCall(func, ptr);
        }

        void LLVMEnvironment::freeAll(llvm::IRBuilder<> &builder) {
            // call runtime free all function
            // create external call to rtmalloc function
            auto func = _module.get()->getOrInsertFunction("rtfree_all", llvm::Type::getVoidTy(_context));
            builder.CreateCall(func);
        }

        std::string LLVMEnvironment::getLLVMTypeName(llvm::Type *t) {
            auto& ctx = t->getContext();

            if(t->isIntegerTy()) {
                return "i" + std::to_string(t->getIntegerBitWidth());
            }

            if(llvm::Type::getFloatTy(ctx) == t)
                return "float";
            if (llvm::Type::getDoubleTy(ctx) == t)
                return "double";

            // // go through all registered types
            // for (const auto& it : _typeMapping) {
            //     if (it.first == t) {
            //         return "llvm_" + it.second.desc();
            //     }
            // }

            // struct type? then just print its twine!
            if (t->isStructTy())
                return ((llvm::StructType *) t)->getName();

            // check if t is pointer type to struct type
            if (t->isPointerTy()) {
                // recurse:
                return getLLVMTypeName(t->getPointerElementType()) + "*";
            }

            if (t->isArrayTy()) {
                return "[" + getLLVMTypeName(t->getArrayElementType()) + "x" + std::to_string(t->getArrayNumElements()) + "]";
            }

            return "unknown";
        }

        llvm::Type *LLVMEnvironment::getEmptyTupleType() {
            return getOrCreateTupleType(python::Type::EMPTYTUPLE, "emptytuple");
        }

        llvm::Value *
        LLVMEnvironment::indexCheck(llvm::IRBuilder<> &builder, llvm::Value *val, llvm::Value *numElements) {
            assert(val->getType()->isIntegerTy());
            assert(numElements->getType()->isIntegerTy());
            // code for 0 <= val < numElements
            auto condGETzero = builder.CreateICmpSLE(i64Const(0), val);
            auto condLTnum = builder.CreateICmpSLT(val, numElements);
            return builder.CreateAnd(condGETzero, condLTnum);
        }

        void LLVMEnvironment::debugPrint(llvm::IRBuilder<> &builder, const std::string &message, llvm::Value *val) {
            if (!val) {
                // only print value (TODO: better printf!)
                auto printf_func = printf_prototype(_context, _module.get());
                llvm::Value *sConst = builder.CreateGlobalStringPtr(message);
                llvm::Value *sFormat = builder.CreateGlobalStringPtr("%s\n");
                builder.CreateCall(printf_func, {sFormat, sConst});

            } else {
                printValue(builder, val, message);
            }
        }

        void LLVMEnvironment::debugCellPrint(llvm::IRBuilder<> &builder, llvm::Value *cellStart, llvm::Value *cellEnd) {
            using namespace llvm;
            auto i8ptr_type = Type::getInt8PtrTy(_context, 0);

            // cast to i8* ptr if necessary
            assert(cellStart->getType() == i8ptr_type);
            assert(cellEnd->getType() == i8ptr_type);

            std::vector<Type *> argtypes{i8ptr_type, i8ptr_type};
            FunctionType *FT = FunctionType::get(Type::getInt8PtrTy(_context, 0), argtypes, false);
            auto func = _module->getOrInsertFunction("_cellPrint", FT);
            builder.CreateCall(func, {cellStart, cellEnd});

        }

        void LLVMEnvironment::printValue(llvm::IRBuilder<> &builder, llvm::Value *val, std::string msg) {
            using namespace llvm;

            auto printf_F = printf_prototype(_context, _module.get());
            llvm::Value *sconst = builder.CreateGlobalStringPtr("unknown type: ??");

            llvm::Value *casted_val = val;
            // check type of value
            if (val->getType() == Type::getInt1Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i1] : %s\n");
                casted_val = builder.CreateSelect(val, builder.CreateGlobalStringPtr("true"),
                                                  builder.CreateGlobalStringPtr("false"));
            } else if (val->getType() == Type::getInt8Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i8] : %d\n");
                casted_val = builder.CreateSExt(val, i64Type()); // also extent to i64 (avoid weird printing errors).
            } else if (val->getType() == Type::getInt32Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i32] : %d\n");
            } else if (val->getType() == Type::getInt64Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i64] : %lu\n");
            } else if (val->getType() == Type::getDoubleTy(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [f64] : %f\n");
            } else if (val->getType() == Type::getInt8PtrTy(_context, 0)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i8*] : [%p] %s\n");
            }
            auto fmt = builder.CreatePointerCast(sconst, llvm::Type::getInt8PtrTy(_context, 0));
            if (val->getType() != Type::getInt8PtrTy(_context, 0))
                builder.CreateCall(printf_F, {fmt, casted_val});
            else
                builder.CreateCall(printf_F, {fmt, casted_val, casted_val});
        }

        llvm::Type *LLVMEnvironment::pythonToLLVMType(const python::Type &t) {
            if (t == python::Type::BOOLEAN)
                return getBooleanType(); // i64 maybe in the future?
            if (t == python::Type::I64)
                return Type::getInt64Ty(_context);
            if (t == python::Type::F64)
                return Type::getDoubleTy(_context);

            // hack NULL VALUE as i8ptr ==> it's actually a const and thus not needed!
            if (t == python::Type::NULLVALUE)
                return Type::getInt8PtrTy(_context);

            // string type is a primitive, hence we can return it
            if (t == python::Type::STRING || t == python::Type::GENERICDICT ||
                t.isDictionaryType() || t == python::Type::PYOBJECT)
                return Type::getInt8PtrTy(_context);

            if(t == python::Type::MATCHOBJECT)
                return getMatchObjectPtrType();

            if(t == python::Type::RANGE)
                return llvm::PointerType::get(getRangeObjectType(), 0);

            if (t == python::Type::EMPTYTUPLE)
                return getEmptyTupleType();

            if (t.isTupleType()) {
                // tuples will get always flattened for optimization purposes!
                auto flat_type = flattenedType(t);
                return getOrCreateTupleType(flat_type);
            }

            if(t.isListType())
                return getListType(t);

            if(t.isIteratorType()) {
                // python iteratorType to LLVM iterator type is a one-to-many mapping, so not able to return LLVM type given only python type t
                // this is only called during variable declaration to avoid iterator variable's slot ptr being nullptr
                // slot ptr needs to be assigned pointer to concrete iterator LLVM struct later
                // to get concrete LLVM type, use createOrGetIteratorType()
                return Type::getInt64Ty(_context);
            }

            // option type ==> create mini struct type consisting of underlying type + i1 do indicate null status.
            if (t.isOptionType()) {
                auto rt = t.getReturnType();

                bool packed = false;

                if (rt == python::Type::BOOLEAN) {
                    llvm::ArrayRef<llvm::Type *> members(
                            std::vector<llvm::Type *>{Type::getInt64Ty(_context), Type::getInt1Ty(_context)});
                    return llvm::StructType::create(_context, members, "bool_opt", packed);
                }

                if (rt == python::Type::I64) {
                    llvm::ArrayRef<llvm::Type *> members(
                            std::vector<llvm::Type *>{Type::getInt64Ty(_context), Type::getInt1Ty(_context)});
                    return llvm::StructType::create(_context, members, "i64_opt", packed);
                }

                if (rt == python::Type::F64) {
                    llvm::ArrayRef<llvm::Type *> members(
                            std::vector<llvm::Type *>{Type::getDoubleTy(_context), Type::getInt1Ty(_context)});
                    return llvm::StructType::create(_context, members, "f64_opt", packed);
                }

                if (rt == python::Type::STRING || rt == python::Type::GENERICDICT || rt.isDictionaryType()) {
                    // could theoretically also use nullptr?
                    llvm::ArrayRef<llvm::Type *> members(
                            std::vector<llvm::Type *>{Type::getInt8PtrTy(_context), Type::getInt1Ty(_context)});
                    return llvm::StructType::create(_context, members, "str_opt", packed);
                }

                if (rt.isListType()) {
                    llvm::ArrayRef<llvm::Type *> members(
                            std::vector<llvm::Type *>{getListType(rt), Type::getInt1Ty(_context)});
                    return llvm::StructType::create(_context, members, "list_opt", packed);
                }
            }

            throw std::runtime_error("could not convert type '" + t.desc() + "' to LLVM compliant type");
            return nullptr;
        }


        llvm::Value *LLVMEnvironment::floorDivision(llvm::IRBuilder<> &builder, llvm::Value *left, llvm::Value *right) {
            assert(left);
            assert(right);

            assert(left->getType() == i64Type());
            assert(right->getType() == i64Type());

            // C code would be
            // int div_floor(int x, int y) {
            //    int q = x/y;
            //    int r = x%y;
            //    if ((r!=0) && ((r<0) != (y<0))) --q;
            //    return q;
            //}

            auto div_res = builder.CreateSDiv(left, right);
            auto Rval = builder.CreateSRem(left, right);
            auto rnezero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_NE, Rval, i64Const(0));
            auto rltzero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, Rval, i64Const(0));
            auto rightltzero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, right, i64Const(0));
            auto cond = builder.CreateAnd(rnezero,
                                          builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_NE, rltzero, rightltzero));
            return builder.CreateSelect(cond, builder.CreateSub(div_res, i64Const(1)), div_res);
        }

        llvm::Value *LLVMEnvironment::floorModulo(llvm::IRBuilder<> &builder, llvm::Value *left, llvm::Value *right) {
            assert(left);
            assert(right);

            // assert both have the same type
            assert(left->getType() == right->getType());
            assert(left->getType() == i64Type() || left->getType() == doubleType());
            assert(right->getType() == i64Type() || right->getType() == doubleType());

            // C code
            // int mod_floor(int x, int y) {
            //     int r = x%y;
            //     if ((r!=0) && ((r<0) != (y<0))) { r += y; }
            //     return r;
            // }

            // generate modulo (floored)
            if (left->getType() == doubleType()) {
                auto rem = builder.CreateFRem(left, right);
                auto rnezero = builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_ONE, rem, f64Const(0.0));
                auto rltzero = builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OLT, rem, f64Const(0.0));
                auto rightltzero = builder.CreateFCmp(llvm::CmpInst::Predicate::FCMP_OLT, right, f64Const(0.0));
                auto cond = builder.CreateAnd(rnezero, builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_NE, rltzero,
                                                                          rightltzero));
                return builder.CreateSelect(cond, builder.CreateFAdd(rem, right), rem);
            } else {
                auto rem = builder.CreateSRem(left, right);
                auto rnezero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_NE, rem, i64Const(0));
                auto rltzero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, rem, i64Const(0));
                auto rightltzero = builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_SLT, right, i64Const(0));
                auto cond = builder.CreateAnd(rnezero, builder.CreateICmp(llvm::CmpInst::Predicate::ICMP_NE, rltzero,
                                                                          rightltzero));
                return builder.CreateSelect(cond, builder.CreateAdd(rem, right), rem);
            }
        }

        std::string LLVMEnvironment::getAssembly() const {
            return "";
            //return tuplex::codegen::moduleToAssembly(std::make_shared(_module));
        }

        void LLVMEnvironment::storeIfNotNull(llvm::IRBuilder<> &builder, llvm::Value *val, llvm::Value *ptr) {
            // check types match
            assert(val && ptr);
            assert(val->getType()->getPointerTo(0) == ptr->getType());

            // now build if then blocks
            using namespace llvm;

            auto func = builder.GetInsertBlock()->getParent();
            BasicBlock *bbIf = BasicBlock::Create(_context, "if_block", func);
            BasicBlock *bbThen = BasicBlock::Create(_context, "then_block", func);

            auto isNotNullCond = builder.CreateICmpNE(ptr, ConstantPointerNull::get(val->getType()->getPointerTo(0)));
            builder.CreateCondBr(isNotNullCond, bbIf, bbThen);
            builder.SetInsertPoint(bbIf);
            builder.CreateStore(val, ptr);
            builder.CreateBr(bbThen);
            builder.SetInsertPoint(bbThen);
        }

        llvm::Value *
        LLVMEnvironment::zeroTerminateString(llvm::IRBuilder<> &builder, llvm::Value *str, llvm::Value *size,
                                             bool copy) {
            using namespace llvm;

            assert(str->getType() == i8ptrType());
            assert(size->getType() == i64Type());

            // if no copy, simply zero terminate
            auto lastCharPtr = builder.CreateGEP(str, builder.CreateSub(size, i64Const(1)));
            if (!copy) {
                builder.CreateStore(i8Const('\0'), lastCharPtr);
                return str;
            } else {
                // if logic
                auto func = builder.GetInsertBlock()->getParent();
                BasicBlock *bbZeroTerminate = BasicBlock::Create(_context, "zeroTerminateStr", func);
                BasicBlock *bbNext = BasicBlock::Create(_context, "next", func);

                // check whether non-zero terminated
                auto lastChar = builder.CreateLoad(lastCharPtr);

                // if non-zero, rtmalloc, copy and zero terminate!
                auto lastCharIsZeroCond = builder.CreateICmpEQ(lastChar, i8Const('\0'));

                auto var = CreateFirstBlockAlloca(builder, i8ptrType()); //builder.CreateAlloca(i8ptrType(), 0, nullptr);
                builder.CreateStore(str, var);
                builder.CreateCondBr(lastCharIsZeroCond, bbNext, bbZeroTerminate);

                builder.SetInsertPoint(bbZeroTerminate);
                auto new_ptr = malloc(builder, size);
                builder.CreateStore(new_ptr, var);

#if LLVM_VERSION_MAJOR < 9
                builder.CreateMemCpy(new_ptr, str, size, 0, true);
#else
                builder.CreateMemCpy(new_ptr, 0, str, 0, size, true);
#endif
                builder.CreateStore(i8Const(0),
                                    builder.CreateGEP(new_ptr, builder.CreateSub(size, i64Const(1)))); // zero terminate

                builder.CreateBr(bbNext);

                // load variable
                builder.SetInsertPoint(bbNext);
                auto val = builder.CreateLoad(var);
                return val;
            }
        }

        llvm::Value *LLVMEnvironment::extractNthBit(llvm::IRBuilder<> &builder, llvm::Value *value, llvm::Value *idx) {
            assert(idx->getType()->isIntegerTy());
            assert(idx->getType() == value->getType());
            assert(idx->getType() == i64Type());

            auto mask = builder.CreateShl(i64Const(1), idx);
            auto check = builder.CreateAnd(value, mask);

            return builder.CreateICmpNE(check, i64Const(0));
        }

        llvm::Value *
        LLVMEnvironment::fixedSizeStringCompare(llvm::IRBuilder<> &builder, llvm::Value *ptr, const std::string &str,
                                                bool include_zero) {

            // how many bytes to compare?
            int numBytes = include_zero ? str.length() + 1 : str.length();

            assert(ptr->getType() == i8ptrType());

            // compare in 64bit (8 bytes) blocks if possible, else smaller blocks.
            llvm::Value *cond = i1Const(true);
            int pos = 0;
            while (numBytes >= 8) {

                uint64_t str_const = 0;

                // create str const by extracting string data
                str_const = *((int64_t *) (str.c_str() + pos));

                auto val = builder.CreateLoad(builder.CreatePointerCast(builder.CreateGEP(ptr, i32Const(pos)), i64ptrType()));

                auto comp = builder.CreateICmpEQ(val, i64Const(str_const));
                cond = builder.CreateAnd(cond, comp);
                numBytes -= 8;
                pos += 8;
            }

            // 32 bit compare?
            if(numBytes >= 4) {
                uint32_t str_const = 0;

                // create str const by extracting string data
                str_const = *((uint32_t *) (str.c_str() + pos));
                auto val = builder.CreateLoad(builder.CreatePointerCast(builder.CreateGEP(ptr, i32Const(pos)), i32ptrType()));
                auto comp = builder.CreateICmpEQ(val, i32Const(str_const));
                cond = builder.CreateAnd(cond, comp);

                numBytes -= 4;
                pos += 4;
            }

            // only 0, 1, 2, 3 bytes left.
            // do 8 bit compares
            for (int i = 0; i < numBytes; ++i) {
                auto val = builder.CreateLoad(builder.CreateGEP(ptr, i32Const(pos)));
                auto comp = builder.CreateICmpEQ(val, i8Const(str.c_str()[pos]));
                cond = builder.CreateAnd(cond, comp);
                pos++;
            }

            return cond;
        }


        SerializableValue LLVMEnvironment::f64ToString(llvm::IRBuilder<> &builder, llvm::Value *value) {
            using namespace llvm;
            using namespace std;

            assert(value->getType() == doubleType());

            // python converts str(1.) to '1.0' so at least one digit after .
            // this is behavior not supported by sprintf, hence use fmt for doubles!
            //fmtString += "%g"; // use %g to get rid off trailing zeros,
            //fmtSize = builder.CreateAdd(fmtSize, _env.i64Const(20)); // roughly estimate formatted size with 20 bytes

            // call runtime function which implements this special behavior
            auto floatfmt_func = floatToStr_prototype(getContext(), getModule().get());

            auto str_size = CreateFirstBlockAlloca(builder, i64Type());
            auto str = builder.CreateCall(floatfmt_func, {value, str_size});

            return SerializableValue(str, builder.CreateLoad(str_size));
        }

        SerializableValue LLVMEnvironment::i64ToString(llvm::IRBuilder<> &builder, llvm::Value *value) {
            using namespace llvm;
            using namespace std;

            assert(value->getType() == i64Type());

            string key = "i64ToStr";

            // check if already generated, else generate helper function
            auto it = _generatedFunctionCache.find(key);
            if (it == _generatedFunctionCache.end()) {
                FunctionType *FT = FunctionType::get(i8ptrType(), {i64Type(), i64ptrType()}, false);

#if LLVM_VERSION_MAJOR < 9
                Function* func = cast<Function>(_module->getOrInsertFunction(key, FT));
#else
                Function *func = cast<Function>(_module->getOrInsertFunction(key, FT).getCallee());
#endif
                _generatedFunctionCache[key] = func;
                auto argMap = mapLLVMFunctionArgs(func, {"value", "res_size_ptr"});

                // Disable attributes to prevent wrong use
                // func->addFnAttr(Attribute::NoUnwind);
                // func->addFnAttr(Attribute::NoRecurse);
                // func->addFnAttr(Attribute::InlineHint);

                BasicBlock *bbEntry = BasicBlock::Create(_context, "entry", func);
                IRBuilder<> b(bbEntry);

                // use sprintf and speculate a bit on size upfront!
                // then do logic to extend buffer if necessary
                BasicBlock *bbCastDone = BasicBlock::Create(getContext(), "castDone_block",
                                                            b.GetInsertBlock()->getParent());
                BasicBlock *bbLargerBuf = BasicBlock::Create(getContext(), "strformat_realloc",
                                                             b.GetInsertBlock()->getParent());

                auto bufVar = b.CreateAlloca(i8ptrType());
                auto fmtSize = i64Const(20); // 20 bytes for i64 should be fine
                string fmtString = "%lld";

                b.CreateStore(malloc(b, fmtSize), bufVar);
                auto snprintf_func = snprintf_prototype(getContext(), getModule().get());

                //{csvRow, fmtSize, env().strConst(b, fmtString), ...}
                auto charsRequired = b.CreateCall(snprintf_func, {b.CreateLoad(bufVar), fmtSize, strConst(b, fmtString),
                                                                  argMap["value"]});
                auto sizeWritten = b.CreateAdd(b.CreateZExt(charsRequired, i64Type()), i64Const(1));

                // now condition, is this greater than allocSize + 1?
                auto notEnoughSpaceCond = b.CreateICmpSGT(sizeWritten, fmtSize);

                // two checks: if size is too small, alloc larger buffer!!!
                b.CreateCondBr(notEnoughSpaceCond, bbLargerBuf, bbCastDone);

                // -- block begin --
                b.SetInsertPoint(bbLargerBuf);
                // realloc with sizeWritten
                // store new malloc in bufVar
                b.CreateStore(malloc(b, sizeWritten), bufVar);
                b.CreateCall(snprintf_func,
                             {b.CreateLoad(bufVar), sizeWritten, strConst(b, fmtString), argMap["value"]});

                b.CreateBr(bbCastDone);
                b.SetInsertPoint(bbCastDone);

                b.CreateStore(sizeWritten, argMap["res_size_ptr"]);
                b.CreateRet(b.CreateLoad(bufVar));
            }

            auto func = _generatedFunctionCache[key];
            assert(func);
            auto str_size = CreateFirstBlockAlloca(builder, i64Type());
            auto str = builder.CreateCall(func, {value, str_size});

            return SerializableValue(str, builder.CreateLoad(str_size));
        }


        llvm::Value *LLVMEnvironment::CreateMaximum(llvm::IRBuilder<> &builder, llvm::Value *rhs, llvm::Value *lhs) {

            // @TODO: Note, CreateMaximum fails...

            // check types
            if (rhs->getType()->isIntegerTy() && lhs->getType()->isIntegerTy()) {

                // max bitwidth
                auto max_bitwidth = std::max(rhs->getType()->getIntegerBitWidth(),
                                             lhs->getType()->getIntegerBitWidth());

                if (rhs->getType()->getIntegerBitWidth() != max_bitwidth)
                    rhs = builder.CreateSExt(rhs, llvm::Type::getIntNTy(_context, max_bitwidth));
                if (lhs->getType()->getIntegerBitWidth() != max_bitwidth)
                    lhs = builder.CreateSExt(lhs, llvm::Type::getIntNTy(_context, max_bitwidth));

                return builder.CreateSelect(builder.CreateICmpSGT(lhs, rhs), lhs, rhs);
            } else throw std::runtime_error(
                        "maximum not yet implemented for llvm types " + getLLVMTypeName(rhs->getType()) + " and " +
                        getLLVMTypeName(lhs->getType()));
        }

        llvm::Value *LLVMEnvironment::addGlobalRegexPattern(const std::string& twine, const std::string &regexPattern) {
            // check cache
            if(_global_regex_cache.count(regexPattern)) return _global_regex_cache[regexPattern];

            // static check whether pattern is valid
            // @TODO: check whether actual compile works! else, compile error!
            //global_pattern_str, i64Const(regexPattern.length()), i32Const(0),
            //                     errornumber,
            //                     erroroffset, i8nullptr()
             int err_number = 0;
             size_t err_offset = 0;
             auto compile_result = pcre2_compile_8(reinterpret_cast<PCRE2_SPTR8>(regexPattern.c_str()),
                                                   regexPattern.length(),
                                                   0, &err_number, &err_offset, nullptr);
             if(!compile_result)
                 throw std::runtime_error("could not compile regex pattern");

            // create unique name for global variable
            std::string name = twine + std::to_string(_global_counters[twine]++);

            // create global variable
            auto gvar = createNullInitializedGlobal(name, llvm::Type::getInt8PtrTy(_context, 0));

            // get the builders
            auto initGlobalBuilder = getInitGlobalBuilder(name);
            auto releaseGlobalBuilder = getReleaseGlobalBuilder(name);

            // create initializer code
            // create global pointer to regex pattern
            auto global_pattern_str = initGlobalBuilder.CreateGlobalStringPtr(regexPattern);
            // allocate some error space
            auto errornumber = initGlobalBuilder.CreateAlloca(initGlobalBuilder.getInt32Ty());
            auto erroroffset = initGlobalBuilder.CreateAlloca(initGlobalBuilder.getInt64Ty());
            auto compiled_pattern = initGlobalBuilder.CreateCall(
                    pcre2Compile_prototype(_context, _module.get()),
                    {global_pattern_str, i64Const(regexPattern.length()), i32Const(0),
                     errornumber,
                     erroroffset, i8nullptr()});

            auto jitErr = initGlobalBuilder.CreateCall(pcre2JITCompile_prototype(_context, _module.get()), {compiled_pattern, i32Const(1)}); // TODO: use PCRE2_JIT_COMPLETE instead of 1
            initGlobalBuilder.CreateStore(compiled_pattern, gvar);

            // set _initGlobalRetValue
            auto compileFailed = initGlobalBuilder.CreateICmpEQ(compiled_pattern, i8nullptr());
            // auto compileFailed = initGlobalBuilder.CreateICmpEQ(initGlobalBuilder.CreatePtrDiff(compiled_pattern, i8nullptr()), i64Const(0));
            // according to https://www.pcre.org/current/doc/html/pcre2_jit_compile.html, a negative value indicates failure
            static_assert(sizeof(int) == 4, "int should be 32 bit integer, else change below constant and API for pcre2");
            auto jitFailed = initGlobalBuilder.CreateICmpSLT(jitErr, i32Const(0));
#ifndef NDEBUG
            // debugPrint(initGlobalBuilder, "compiledFailed for regex " + regexPattern + ": ", compileFailed);
            // debugPrint(initGlobalBuilder, "jitFailed for regex " + regexPattern + ": ", jitFailed);
#endif
            auto initFailed = initGlobalBuilder.CreateOr(compileFailed, jitFailed);
            initGlobalBuilder.CreateStore(initGlobalBuilder.CreateIntCast(initFailed, i64Type(), false), _initGlobalRetValue);

            // create release code
            releaseGlobalBuilder.CreateCall(pcre2CodeFree_prototype(_context, _module.get()),{releaseGlobalBuilder.CreateLoad(gvar)});
            releaseGlobalBuilder.CreateStore(i64Const(0), _releaseGlobalRetValue);

            // cache the result and return
            _global_regex_cache[regexPattern] = gvar;
            return gvar;
        }

        std::tuple<llvm::Value*, llvm::Value*, llvm::Value*> LLVMEnvironment::addGlobalPCRE2RuntimeContexts() {
            std::string name = "pcre2_runtime_context";
            std::string gen_name = "pcre2_runtime_general_context";
            std::string match_name = "pcre2_runtime_match_context";
            std::string compile_name = "pcre2_runtime_compile_context";

            // if the contexts have already been created, return them
            if(_global_counters[name] == 1) {
                llvm::GlobalVariable *generalContextVar = _module->getNamedGlobal(gen_name);
                llvm::GlobalVariable *matchContextVar = _module->getNamedGlobal(match_name);
                llvm::GlobalVariable *compileContextVar = _module->getNamedGlobal(compile_name);
                return std::make_tuple(generalContextVar, matchContextVar, compileContextVar);
            }

            // get the builders
            auto initGlobalBuilder = getInitGlobalBuilder(name);
            auto releaseGlobalBuilder = getReleaseGlobalBuilder(name);

            // create global variables
            // general context
            auto generalContextVar = createNullInitializedGlobal(gen_name, i8ptrType());
            // match context
            auto matchContextVar = createNullInitializedGlobal(match_name, i8ptrType());
            // compile context
            auto compileContextVar = createNullInitializedGlobal(compile_name, i8ptrType());

            // create initializer code
            auto general_context = initGlobalBuilder.CreateCall(
                    pcre2GetGlobalGeneralContext_prototype(_context, _module.get()), {});
            auto match_context = initGlobalBuilder.CreateCall(
                    pcre2GetGlobalMatchContext_prototype(_context, _module.get()), {});
            auto compile_context = initGlobalBuilder.CreateCall(
                    pcre2GetGlobalCompileContext_prototype(_context, _module.get()), {});
            initGlobalBuilder.CreateStore(general_context, generalContextVar);
            initGlobalBuilder.CreateStore(match_context, matchContextVar);
            initGlobalBuilder.CreateStore(compile_context, compileContextVar);

            auto generalContextFailed = initGlobalBuilder.CreateICmpEQ(initGlobalBuilder.CreatePtrDiff(general_context, i8nullptr()), i64Const(0));
            auto matchContextFailed = initGlobalBuilder.CreateICmpEQ(initGlobalBuilder.CreatePtrDiff(match_context, i8nullptr()), i64Const(0));
            auto compileContextFailed = initGlobalBuilder.CreateICmpEQ(initGlobalBuilder.CreatePtrDiff(compile_context, i8nullptr()), i64Const(0));
            auto initFailed = initGlobalBuilder.CreateOr(generalContextFailed,
                                                         initGlobalBuilder.CreateOr(matchContextFailed,compileContextFailed));
            initGlobalBuilder.CreateStore(initGlobalBuilder.CreateIntCast(initFailed, i64Type(), false), _initGlobalRetValue);

            // create release code
            releaseGlobalBuilder.CreateCall(pcre2ReleaseGlobalGeneralContext_prototype(_context, _module.get()), {releaseGlobalBuilder.CreateLoad(generalContextVar)});
            releaseGlobalBuilder.CreateCall(pcre2ReleaseGlobalMatchContext_prototype(_context, _module.get()), {releaseGlobalBuilder.CreateLoad(matchContextVar)});
            releaseGlobalBuilder.CreateCall(pcre2ReleaseGlobalCompileContext_prototype(_context, _module.get()), {releaseGlobalBuilder.CreateLoad(compileContextVar)});
            releaseGlobalBuilder.CreateStore(i64Const(0), _releaseGlobalRetValue);

            // cache the creation
            _global_counters["pcre2_runtime_context"] = 1;
            return std::make_tuple(generalContextVar, matchContextVar, compileContextVar);
        }

        llvm::Value * LLVMEnvironment::callGlobalsInit(llvm::IRBuilder<> &builder) {
            assert(_initGlobalEntryBlock);
            auto func = _initGlobalEntryBlock->getParent(); assert(func);
            return builder.CreateCall(func, {});
        }

        llvm::Value* LLVMEnvironment::callGlobalsRelease(llvm::IRBuilder<>& builder) {
            assert(_releaseGlobalEntryBlock);
            auto func = _releaseGlobalEntryBlock->getParent(); assert(func);
            return builder.CreateCall(func, {});
        }

        llvm::Value * LLVMEnvironment::callBytesHashmapGet(llvm::IRBuilder<>& builder, llvm::Value *hashmap, llvm::Value *key, llvm::Value *key_size, llvm::Value *returned_bucket) {
            using namespace llvm;

            assert(hashmap && key && returned_bucket);
            assert(hashmap->getType() == i8ptrType());
            assert(key->getType() == i8ptrType());
            if(key_size)
               assert(key_size->getType()->isIntegerTy());
            assert(returned_bucket->getType() == i8ptrType()->getPointerTo(0)); // should be i8**

            // probe step
            // hmap i8*, i8*, i8**
            FunctionType *hmap_func_type = FunctionType::get(Type::getInt32Ty(_context),
                                                             {i8ptrType(), i8ptrType(), i64Type(),
                                                              i8ptrType()->getPointerTo(0)}, false);
#if LLVM_VERSION_MAJOR < 9
            auto hmap_get_func = env->getModule()->getOrInsertFunction("hashmap_get", hmap_func_type);
#else
            auto hmap_get_func = getModule()->getOrInsertFunction("hashmap_get", hmap_func_type).getCallee();
#endif
            auto in_hash_map = builder.CreateCall(hmap_get_func, {hashmap, key, key_size, returned_bucket});
            auto found_val = builder.CreateICmpEQ(in_hash_map, i32Const(0));

            return found_val;
        }

        llvm::Value * LLVMEnvironment::callIntHashmapGet(llvm::IRBuilder<>& builder, llvm::Value *hashmap, llvm::Value *key, llvm::Value *returned_bucket) {
            using namespace llvm;

            assert(hashmap && key && returned_bucket);
            assert(hashmap->getType() == i8ptrType());
            assert(key->getType() == i64Type());
            assert(returned_bucket->getType() == i8ptrType()->getPointerTo(0)); // should be i8**

            // probe step
            // i8* hmap, i64 key, i8** bucket
            FunctionType *hmap_func_type = FunctionType::get(Type::getInt32Ty(_context),
                                                             {i8ptrType(), i64Type(),
                                                              i8ptrType()->getPointerTo(0)}, false);
#if LLVM_VERSION_MAJOR < 9
            auto hmap_get_func = env->getModule()->getOrInsertFunction("int64_hashmap_get", hmap_func_type);
#else
            auto hmap_get_func = getModule()->getOrInsertFunction("int64_hashmap_get", hmap_func_type).getCallee();
#endif
            auto in_hash_map = builder.CreateCall(hmap_get_func, {hashmap, key, returned_bucket});
            auto found_val = builder.CreateICmpEQ(in_hash_map, i32Const(0));

            return found_val;
        }

        SerializableValue LLVMEnvironment::primitiveFieldToLLVM(llvm::IRBuilder<> &builder, const Field &f) {
            // convert basically field to constant
            if(f.getType() == python::Type::NULLVALUE) {
                return SerializableValue(nullptr, nullptr, i1Const(true));
            } else if(f.getType().isOptionType()) {
                auto val = primitiveFieldToLLVM(builder, f.withoutOption());
                return SerializableValue(val.val, val.size, i1Const(f.isNull()));
            } else if(f.getType() == python::Type::BOOLEAN) {
                return SerializableValue(boolConst(f.getInt()), i64Const(sizeof(int64_t)), i1Const(false));
            } else if(f.getType() == python::Type::I64) {
                return SerializableValue(i64Const(f.getInt()), i64Const(sizeof(int64_t)), i1Const(false));
            } else if(f.getType() == python::Type::F64) {
                return SerializableValue(f64Const(f.getDouble()), i64Const(sizeof(double)), i1Const(false));
            } else if(f.getType() == python::Type::STRING) {
                auto cstr = (const char*)f.getPtr();
                return SerializableValue(strConst(builder, cstr), i64Const(strlen(cstr) + 1), i1Const(false));
            }

            // @TODO: tuples, lists, dicts, ...
#ifndef NDEBUG
            std::cerr<<"unsupported field type: " + f.desc() + " encountered"<<std::endl;
#endif
            throw std::runtime_error("unsupported field type: " + f.desc() + " encountered");
            return SerializableValue();
        }

        llvm::Value * LLVMEnvironment::matchExceptionHierarchy(llvm::IRBuilder<> &builder, llvm::Value *codeValue,
                                                               const ExceptionCode &ec) {
            // either 32 bit or 64bit
            assert(codeValue->getType()->isIntegerTy());
            codeValue = builder.CreateZExtOrTrunc(codeValue, i32Type());

            // get all derived exception classes
            auto exceptionType = python::Type::byName(exceptionCodeToPythonClass(ec));
            assert(exceptionType != python::Type::UNKNOWN);
            auto derivedExceptions = exceptionType.derivedClasses();

            Value* matchCond = builder.CreateICmpEQ(codeValue, i32Const(ecToI32(ec)));
            // @TODO: develop algorithm to compact the integers into intervals for faster checking?
            for(const auto& exc : derivedExceptions) {
                auto name = exc.desc();
                auto excEC = pythonClassToExceptionCode(name);
                assert(excEC != ExceptionCode::UNKNOWN);
                matchCond = builder.CreateOr(matchCond, builder.CreateICmpEQ(codeValue, i32Const(ecToI32(excEC))));
            }

            return matchCond;
        }

        llvm::Value * LLVMEnvironment::getListSize(llvm::IRBuilder<> &builder, llvm::Value *val,
                                                   const python::Type &listType) {
            // what list type do we have?
            if(listType == python::Type::EMPTYLIST)
                return i64Const(0);
#ifndef NDEBUG
            if(listType == python::Type::GENERICLIST)
                throw std::runtime_error("do not support codegen for generic list type. Must specialize!");
#endif
            auto elType = listType.elementType();
            if(elType.isSingleValued()) {
                // special case:
                throw std::runtime_error("not yet implemented!!!");
            } else {
                // size comes after capacity
                if(val->getType()->isStructTy()) {
                    auto list_len = builder.CreateExtractValue(val, std::vector<unsigned int>{1});
                    assert(list_len->getType() == i64Type());
                    return list_len;
                } else {
                    assert(val->getType()->isPointerTy() && val->getType()->getPointerElementType()->isStructTy());
                    auto list_len_ptr = CreateStructGEP(builder, val, 1);
                    auto list_len = builder.CreateLoad(list_len_ptr);
                    assert(list_len->getType() == i64Type());
                    return list_len;
                }
            }
        }

        SerializableValue parseBoolean(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::BasicBlock *bbFailed,
                                                              llvm::Value *str, llvm::Value *strSize,
                                                              llvm::Value *isnull) {

            using namespace llvm;

            assert(bbFailed);
            auto& ctx = env.getContext();
            auto func = builder.GetInsertBlock()->getParent(); assert(func);

            Value* bool_val = env.CreateFirstBlockAlloca(builder, env.getBooleanType());
            builder.CreateStore(env.boolConst(false), bool_val);

            // all the basicblocks
            BasicBlock* bbParse = BasicBlock::Create(ctx, "parse_bool_value", func);
            BasicBlock* bbParseDone = BasicBlock::Create(ctx, "parse_bool_done", func);

            // perform null check or directly go to parse block.
            if(isnull)
                builder.CreateCondBr(isnull, bbParseDone, bbParse);
            else
                builder.CreateBr(bbParse); // always go directly to parse block

            // the parse block with exception check
            builder.SetInsertPoint(bbParse);

            // call parse function
            std::vector<Type*> argtypes{env.i8ptrType(), env.i8ptrType(), bool_val->getType()};
            FunctionType *FT = FunctionType::get(Type::getInt32Ty(ctx), argtypes, false);

            auto conv_func = env.getModule().get()->getOrInsertFunction("fast_atob", FT);
            auto cellEnd = builder.CreateGEP(str, builder.CreateSub(strSize, env.i64Const(1)));
            auto resCode = builder.CreateCall(conv_func, {str, cellEnd, bool_val});

            auto parseSuccessCond = builder.CreateICmpEQ(resCode, env.i32Const(ecToI32(ExceptionCode::SUCCESS)));

            builder.CreateCondBr(parseSuccessCond, bbParseDone, bbFailed);

            // parse done, load result var
            builder.SetInsertPoint(bbParseDone);
            // load val & return result
            return SerializableValue(builder.CreateLoad(bool_val), env.i64Const(sizeof(int64_t)), isnull);
        }

        SerializableValue parseI64(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::BasicBlock *bbFailed,
                                   llvm::Value *str, llvm::Value *strSize,
                                   llvm::Value *isnull) {

            using namespace llvm;

            assert(bbFailed);
            auto& ctx = env.getContext();
            auto func = builder.GetInsertBlock()->getParent(); assert(func);

            Value* i64_val = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), i64_val);

            // all the basicblocks
            BasicBlock* bbParse = BasicBlock::Create(ctx, "parse_i64_value", func);
            BasicBlock* bbParseDone = BasicBlock::Create(ctx, "parse_i64_done", func);

            // perform null check or directly go to parse block.
            if(isnull)
                builder.CreateCondBr(isnull, bbParseDone, bbParse);
            else
                builder.CreateBr(bbParse); // always go directly to parse block

            // the parse block with exception check
            builder.SetInsertPoint(bbParse);

            // call parse function
            std::vector<Type*> argtypes{env.i8ptrType(), env.i8ptrType(), env.i64ptrType()};
            FunctionType *FT = FunctionType::get(Type::getInt32Ty(ctx), argtypes, false);
            auto conv_func = env.getModule().get()->getOrInsertFunction("fast_atoi64", FT);
            auto cellEnd = builder.CreateGEP(str, builder.CreateSub(strSize, env.i64Const(1)));
            auto resCode = builder.CreateCall(conv_func, {str, cellEnd, i64_val});

            auto parseSuccessCond = builder.CreateICmpEQ(resCode, env.i32Const(ecToI32(ExceptionCode::SUCCESS)));

            builder.CreateCondBr(parseSuccessCond, bbParseDone, bbFailed);

            // parse done, load result var
            builder.SetInsertPoint(bbParseDone);
            // load val & return result
            return SerializableValue(builder.CreateLoad(i64_val), env.i64Const(sizeof(int64_t)), isnull);
        }

        SerializableValue parseF64(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::BasicBlock *bbFailed,
                                   llvm::Value *str, llvm::Value *strSize,
                                   llvm::Value *isnull) {
            using namespace llvm;

            assert(bbFailed);
            auto& ctx = env.getContext();
            auto func = builder.GetInsertBlock()->getParent(); assert(func);

            Value* f64_val = env.CreateFirstBlockAlloca(builder, env.doubleType());
            builder.CreateStore(env.f64Const(0), f64_val);

            // all the basicblocks
            BasicBlock* bbParse = BasicBlock::Create(ctx, "parse_f64_value", func);
            BasicBlock* bbParseDone = BasicBlock::Create(ctx, "parse_f64_done", func);

            // perform null check or directly go to parse block.
            if(isnull)
                builder.CreateCondBr(isnull, bbParseDone, bbParse);
            else
                builder.CreateBr(bbParse); // always go directly to parse block

            // the parse block with exception check
            builder.SetInsertPoint(bbParse);

            // call parse function
            std::vector<Type*> argtypes{env.i8ptrType(), env.i8ptrType(), env.doubleType()->getPointerTo()};
            FunctionType *FT = FunctionType::get(Type::getInt32Ty(ctx), argtypes, false);
            auto conv_func = env.getModule().get()->getOrInsertFunction("fast_atod", FT);
            auto cellEnd = builder.CreateGEP(str, builder.CreateSub(strSize, env.i64Const(1)));
            auto resCode = builder.CreateCall(conv_func, {str, cellEnd, f64_val});

            auto parseSuccessCond = builder.CreateICmpEQ(resCode, env.i32Const(ecToI32(ExceptionCode::SUCCESS)));

            builder.CreateCondBr(parseSuccessCond, bbParseDone, bbFailed);

            // parse done, load result var
            builder.SetInsertPoint(bbParseDone);
            // load val & return result
            return SerializableValue(builder.CreateLoad(f64_val), env.i64Const(sizeof(double)), isnull);
        }

        llvm::Value* LLVMEnvironment::isInteger(llvm::IRBuilder<>& builder, llvm::Value* value, llvm::Value* eps) {
            // shortcut for integer types
            if(value->getType()->isIntegerTy())
                return i1Const(true);

            assert(value->getType()->isDoubleTy());

            if(!eps)
                eps = defaultEpsilon();

            // bool IsInteger(float value)
            //{
            //    return fabs(ceilf(value) - value) < EPSILON;
            //}
            auto cf = builder.CreateUnaryIntrinsic(llvm::Intrinsic::ID::ceil, value);
            auto fabs_value = builder.CreateUnaryIntrinsic(llvm::Intrinsic::ID::fabs, builder.CreateFSub(cf, value));

            return builder.CreateFCmpOLT(fabs_value, eps);
        }

        llvm::BlockAddress * LLVMEnvironment::createOrGetUpdateIteratorIndexFunctionDefaultBlockAddress(llvm::IRBuilder<> &builder,
                                                                                                        const python::Type &iterableType,
                                                                                                        bool reverse) {
            using namespace llvm;

            std::string funcName, prefix;
            if(reverse) {
                prefix = "reverse";
            } // else: empty string

            if(iterableType.isListType()) {
                funcName = "list_" + prefix + "iterator_update";
            } else if(iterableType == python::Type::STRING) {
                funcName = "str_" + prefix + "iterator_update";
            } else if(iterableType == python::Type::RANGE) {
                // range_iterator is always used
                funcName = "range_iterator_update";
            } else if(iterableType.isTupleType()) {
                funcName = "tuple_" + prefix + "iterator_update";
            } else {
                throw std::runtime_error("Cannot generate LLVM UpdateIteratorIndex function for iterator generated from iterable type" + iterableType.desc());
            }

            auto it = _generatedIteratorUpdateIndexFunctions.find(funcName);
            if(_generatedIteratorUpdateIndexFunctions.end() != it) {
                return it->second;
            }

            BasicBlock *currBB = builder.GetInsertBlock();

            llvm::Type *iteratorContextType = createOrGetIterIteratorType(iterableType);
            // function type: i1(*struct.iterator) ; bool value indicate whether iterator is exhausted
            FunctionType *ft = llvm::FunctionType::get(llvm::Type::getInt1Ty(_context),
                                                       {llvm::PointerType::get(iteratorContextType, 0)}, false);

            Function *func = Function::Create(ft, Function::InternalLinkage, funcName, getModule().get());

            BasicBlock *entryBB = BasicBlock::Create(_context, "entryBB", func);
            BasicBlock *updateIndexBB = BasicBlock::Create(_context, "updateIndexBB", func);
            BasicBlock *loopCondBB = BasicBlock::Create(_context, "loopCondBB", func);
            BasicBlock *loopBB = BasicBlock::Create(_context, "loopBB", func);
            BasicBlock *loopExitBB = BasicBlock::Create(_context, "loopExitBB", func);
            BasicBlock *endBB = BasicBlock::Create(_context, "endBB", func);

            // redirect based on the block address in the iterator struct
            builder.SetInsertPoint(entryBB);
            // retrieve the block address to resume
            auto blockAddrPtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                  {i32Const(0), i32Const(0)});
            auto blockAddr = builder.CreateLoad(blockAddrPtr);
            // indirect branch to block updateIndexBB or endBB
            auto indirectBr = builder.CreateIndirectBr(blockAddr, 2);
            indirectBr->addDestination(updateIndexBB);
            indirectBr->addDestination(endBB);

            // increment index in iterator struct
            builder.SetInsertPoint(updateIndexBB);
            auto indexPtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                  {i32Const(0), i32Const(1)});
            if(iterableType == python::Type::RANGE) {
                auto rangePtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                 {i32Const(0), i32Const(2)});
                auto rangeAlloc = builder.CreateLoad(rangePtr);
                auto stepPtr = builder.CreateGEP(getRangeObjectType(), rangeAlloc, {i32Const(0), i32Const(2)});
                auto step = builder.CreateLoad(stepPtr);
                builder.CreateStore(builder.CreateAdd(builder.CreateLoad(indexPtr), step), indexPtr);
            } else {
                if(reverse) {
                    builder.CreateStore(builder.CreateSub(builder.CreateLoad(indexPtr), i32Const(1)), indexPtr);
                } else {
                    builder.CreateStore(builder.CreateAdd(builder.CreateLoad(indexPtr), i32Const(1)), indexPtr);
                }
            }
            builder.CreateBr(loopCondBB);

            // check whether iterator is exhausted
            builder.SetInsertPoint(loopCondBB);
            // retrieve iterable length
            llvm::Value *iterableLength = nullptr;
            if(!reverse) {
                // need to retrieve length for list, tuple or string
                if(iterableType.isListType()) {
                    if (iterableType == python::Type::EMPTYLIST) {
                        iterableLength = i32Const(0);
                    } else {
                        auto listPtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                         {i32Const(0), i32Const(2)});
                        auto listAlloc = builder.CreateLoad(listPtr);
                        auto listLengthPtr = builder.CreateGEP(pythonToLLVMType(iterableType), listAlloc, {i32Const(0), i32Const(1)});
                        iterableLength = builder.CreateLoad(listLengthPtr);
                    }
                } else if(iterableType == python::Type::STRING || iterableType.isTupleType()) {
                    auto iterableLengthPtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                               {i32Const(0), i32Const(3)});
                    iterableLength = builder.CreateLoad(iterableLengthPtr);
                }
            }
            // retrieve current index (i64 for range_iterator, i32 for others)
            auto currIndex = builder.CreateLoad(indexPtr);
            llvm::Value *loopContinue;
            if(iterableType == python::Type::RANGE) {
                auto rangePtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                  {i32Const(0), i32Const(2)});
                auto rangeAlloc = builder.CreateLoad(rangePtr);
                auto stepPtr = builder.CreateGEP(getRangeObjectType(), rangeAlloc, {i32Const(0), i32Const(2)});
                auto step = builder.CreateLoad(stepPtr);
                // positive step -> stepSign = 1, negative step -> stepSign = -1
                // stepSign = (step >> 63) | 1 , use arithmetic shift
                auto stepSign = builder.CreateOr(builder.CreateAShr(step, i64Const(63)), i64Const(1));
                auto endPtr = builder.CreateGEP(getRangeObjectType(), rangeAlloc, {i32Const(0), i32Const(1)});
                auto end = builder.CreateLoad(endPtr);
                // step can be negative in range. Check if curr * stepSign < end * stepSign
                loopContinue = builder.CreateICmpSLT(builder.CreateMul(currIndex, stepSign), builder.CreateMul(end, stepSign));
            } else {
                if(reverse) {
                    loopContinue = builder.CreateICmpSGE(currIndex, i32Const(0));
                } else {
                    loopContinue = builder.CreateICmpSLT(builder.CreateZExt(currIndex, i64Type()), iterableLength);
                }
            }
            builder.CreateCondBr(loopContinue, loopBB, loopExitBB);

            // current index inside iterable index range, set block address in iterator struct to updateIndexBB and return false
            builder.SetInsertPoint(loopBB);
            auto nextBlockAddrPtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                      {i32Const(0), i32Const(0)});
            builder.CreateStore(llvm::BlockAddress::get(func, updateIndexBB), nextBlockAddrPtr);
            builder.CreateRet(i1Const(false));

            // current index out of iterable index range (iterator exhausted), set block address to endBB
            builder.SetInsertPoint(loopExitBB);
            auto endBlockAddrPtr = builder.CreateGEP(iteratorContextType, func->arg_begin(),
                                                     {i32Const(0), i32Const(0)});
            builder.CreateStore(llvm::BlockAddress::get(func, endBB), endBlockAddrPtr);
            builder.CreateBr(endBB);

            // iterator exhausted, function will always return true when called from now on
            builder.SetInsertPoint(endBB);
            builder.CreateRet(i1Const(true));

            // restore insert point
            builder.SetInsertPoint(currBB);

            auto retAddr = llvm::BlockAddress::get(func, updateIndexBB);
            _generatedIteratorUpdateIndexFunctions[funcName] = retAddr;
            return retAddr;
        }
    }
}