//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <codegen/LLVMEnvironment.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <TypeSystem.h>
#include <Logger.h>
#include <StringUtils.h>
#include <Base.h>
#include <codegen/CodegenHelper.h>
#include <Utils.h>
#include <pcre2.h>
#include <TupleTree.h>

#include <codegen/FlattenedTuple.h>

#include <regex>
#include <set>

#include <experimental/StructDictHelper.h>
#include <experimental/ListHelper.h>

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


        // @TODO: refactor codegenHelper???
        SerializableValue constantValuedTypeToLLVM(llvm::IRBuilder<>& builder, const python::Type& const_type) {

            if(!const_type.isConstantValued())
                throw std::runtime_error("Given type " + const_type.desc() + " is not constant velued, error");

            auto& ctx = builder.getContext();
            auto ut = const_type.underlying();
            auto constant_value = const_type.constant();
            auto not_null = llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, false));
            if(ut.isOptionType()) {
                // check if null?
                if(constant_value == "None")
                    return SerializableValue(nullptr, nullptr, llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, true)));
                else {
                    ut = ut.elementType();
                }
            }

            if(ut == python::Type::NULLVALUE) {
                auto is_null = llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(ctx), llvm::APInt(1, true));
                return SerializableValue(nullptr, nullptr, is_null);
            } else if(ut == python::Type::BOOLEAN) {
                bool b = stringToBool(constant_value);
                //auto t = getBooleanType();
                auto t = llvm::Type::getInt8Ty(ctx);
                auto bconst = llvm::Constant::getIntegerValue(t, llvm::APInt(t->getIntegerBitWidth(), b));
                auto bconst_size =  llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, sizeof(int64_t)));
                return SerializableValue(bconst, bconst_size, not_null);
            } else if(ut == python::Type::I64) {
                auto ival = std::stoll(constant_value);
                auto i64const = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, ival));
                auto i64const_size = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, sizeof(int64_t)));
                return SerializableValue(i64const, i64const_size, not_null);
            } else if(ut == python::Type::F64) {
                auto dval = std::stod(constant_value);
                auto f64const = llvm::ConstantFP::get(ctx, llvm::APFloat(dval));
                auto f64const_size = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, sizeof(double)));
                return SerializableValue(f64const, f64const_size, not_null);
            } else if(ut == python::Type::STRING) {
                assert(builder.GetInsertBlock()->getParent()); // make sure block has a parent, else pretty bad bugs could happen...
                auto sconst = builder.CreateGlobalStringPtr(constant_value);
                auto sval = builder.CreatePointerCast(sconst, llvm::Type::getInt8PtrTy(ctx, 0));
                auto s64const_size = llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, constant_value.length() + 1));
                return SerializableValue(sval, s64const_size, not_null);
            } else {
                std::cerr << "wrong type found in gettupleelement constant valued: " << const_type.desc() << std::endl;
                throw std::runtime_error("unknown constant valued type");
            }
        }

        void LLVMEnvironment::init(const std::string &moduleName) {

            auto& logger = Logger::instance().logger("codegen");

            initLLVM();

            // init module
            _module.reset(new llvm::Module(moduleName, _context));

            // IR is target independent, set target when explicitly compiling!
            // // fetch for executing process the target machine & set data layout to it
            // llvm::TargetMachine *TM = llvm::EngineBuilder().selectTarget();
            // assert(TM);
            // _module->setDataLayout(TM->createDataLayout());

            // if (TM)
            //     delete TM;
            // TM = nullptr;

            // always set data layout!
            // std::string default_target_triple = llvm::sys::getProcessTriple();
            // logger.info("Compiling with target " + default_target_triple);
            // _module->setTargetTriple(default_target_triple);


            // e-m:e-i64:64-f80:128-n8:16:32:64-S128
            // e-m:e-i64:64-f80:128-n8:16:32:64-S128
            logger.info("module's data layout is: " + _module->getDataLayoutStr());

            // setup defaults in typeMapping (ignore bool)
            // _typeMapping[llvm::Type::getDoubleTy(_context)] = python::Type::F64;
            // _typeMapping[llvm::Type::getInt64Ty(_context)] = python::Type::I64;
            addType(llvm::Type::getDoubleTy(_context), python::Type::F64);
            addType(llvm::Type::getInt64Ty(_context), python::Type::I64);

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
        static std::unordered_map<int, std::vector<std::tuple<int, int, int>>> g_tupleIndexCache;
        static std::mutex g_tupleIndexCacheMutex;

        std::tuple<int, int, int> getTupleIndices(const python::Type &tupleType, size_t index) {
            // special case: empty tuple
            if(tupleType == python::Type::EMPTYTUPLE)
                return std::make_tuple(-1, -1, -1);

            // general checks
            assert(tupleType.isTupleType());
            assert(index < tupleType.parameters().size());

            auto err_bad_index_message = "specified invalid index of " + std::to_string(index) + " for tuple " + tupleType.desc();

            std::lock_guard<std::mutex> lock(g_tupleIndexCacheMutex);
            // find cache
            auto it = g_tupleIndexCache.find(tupleType.hash());

            // found? then return entry!
            if(it != g_tupleIndexCache.end()) {

                // quick check that everything is ok
                if(index >= it->second.size())
                    throw std::runtime_error(err_bad_index_message);
                // ok
                return it->second[index];
            }

            // not found, so create full entry for tuple type!
            std::vector<std::tuple<int, int, int>> vec_of_indices;

            int values_to_store_so_far = 0;
            int sizes_to_store_so_far = 0;
            int cur_bitmap_idx = 0;
            auto num_tuple_elements = tupleType.parameters().size();
            for(int i = 0; i < num_tuple_elements; ++i) {
                // what type is it?
                int value_idx = -1, size_idx = -1, bitmap_idx = -1;

                // is this value being stored at all?
                auto element_type = tupleType.parameters()[i];

                // option? => store bit!
                if(element_type.isOptionType()) {
                    bitmap_idx = cur_bitmap_idx++;
                    element_type = element_type.getReturnType();
                }

                // special case: constant valued type?
                if(element_type.isConstantValued()) {
                    vec_of_indices.emplace_back(value_idx, size_idx, bitmap_idx);
                    continue;
                }

                // single-valued? i.e. not stored?
                if(!element_type.isSingleValued()) {
                    // is it of var lenght? -> then store size field!
                    // only applies to general dicts, str, pyobject, ...
                    if(python::Type::STRING == element_type || python::Type::PYOBJECT == element_type) {
                        // store both value & size field
                        value_idx = values_to_store_so_far++;
                        size_idx = sizes_to_store_so_far++;
                    } else if(element_type.isDictionaryType() && !element_type.isStructuredDictionaryType()) {
                        // store both value & size field
                        assert(element_type != python::Type::EMPTYDICT);
                        value_idx = values_to_store_so_far++;
                        size_idx = sizes_to_store_so_far++;
                    } else if(element_type.isTupleType() && element_type != python::Type::EMPTYTUPLE) {
                        throw std::runtime_error("do not use tuple type " + element_type.desc() + " here, better flatten tuple type before");
                    } else {
                        // no size field stored! store only value.
                        value_idx = values_to_store_so_far++;
                    }
                } else {
                    // single-valued -> will not get stored. -> also applies to constants etc.
                }

                vec_of_indices.emplace_back(value_idx, size_idx, bitmap_idx);
            }


            // now correct for that sizes are stored after values and bitmap.
            bool has_bitmap = (cur_bitmap_idx > 0);
            auto size_offset = has_bitmap + values_to_store_so_far;
            std::vector<std::tuple<int, int, int>> adj_vec_of_indices;
            for(auto entry : vec_of_indices) {
                int value_idx = -1, size_idx = -1, bitmap_idx = -1;
                std::tie(value_idx, size_idx, bitmap_idx) = entry;

                // correct value idx for bitmap
                if(-1 != value_idx)
                    value_idx += has_bitmap;
                // correct size idx for both bitmap and values
                if(-1 != size_idx)
                    size_idx += size_offset;

                adj_vec_of_indices.emplace_back(value_idx, size_idx, bitmap_idx);
            }

            // add to tuple cache.
            g_tupleIndexCache[tupleType.hash()] = adj_vec_of_indices;

            if(index >= vec_of_indices.size())
                throw std::runtime_error(err_bad_index_message);
            return adj_vec_of_indices[index];
        }

        // Note: this function below does not support all the weird Option[Tuple] cases Yunzhi implemented.
        // handle these separately.
        llvm::Type *LLVMEnvironment::createTupleStructType(const python::Type &type, const std::string &twine) {
            using namespace llvm;

            python::Type T = python::Type::propagateToTupleType(type);
            assert(T.isTupleType());

            auto& logger = Logger::instance().logger("codegen");
            auto &ctx = _context;
            auto size_field_type = llvm::Type::getInt64Ty(ctx); // what llvm type to use for size fields.

            bool packed = false;

            // empty tuple?
            // is special type
            if (type.parameters().size() == 0) {
                llvm::ArrayRef<llvm::Type *> members;
                llvm::Type *structType = llvm::StructType::create(ctx, members, "emptytuple", packed);

                // add to mapping (make sure it doesn't exist yet!)
                assert(_typeMapping.find(structType) == _typeMapping.end());
                addType(structType, type);
                // _typeMapping[structType] = type;
                return structType;
            }


            // new -> base on tuple indices.
            // first, determine max index, fill everything with dummy types (i64)
            int max_value_idx = -1;
            int max_size_idx = -1;
            int max_bitmap_idx = -1;
            int num_value_entries = 0;
            int num_size_entries = 0;
            auto num_tuple_elements = type.parameters().size();
            for(unsigned i = 0; i < num_tuple_elements; ++i) {
                int value_idx = -1, size_idx = -1, bitmap_idx = -1;
                auto t_indices = getTupleIndices(type, i);
                std::tie(value_idx, size_idx, bitmap_idx) = t_indices;

                max_value_idx = std::max(max_value_idx, value_idx);
                max_size_idx = std::max(max_size_idx, max_size_idx);
                max_bitmap_idx = std::max(max_bitmap_idx, bitmap_idx);

                num_value_entries += (value_idx != -1);
                num_size_entries += (size_idx != -1);
            }

            // how many values? how many sizes? how many bitmap elements are required?
            auto num_bitmap_entries = max_bitmap_idx + 1;

            auto num_entries = (num_bitmap_entries > 0) + num_value_entries + num_size_entries;
            // some checks re indices
            assert(max_value_idx < num_entries);
            assert(max_size_idx < num_entries);

            // alloc vector
            std::vector<llvm::Type*> members(num_entries, nullptr);

            // fill in bitmap if present
            if(num_bitmap_entries > 0) {
                // round up to multiple of 64 (so alignment doesn't become an issue)
                auto rounded_num_bitmap_entries = core::ceilToMultiple(num_bitmap_entries, 64);
                members[0] = ArrayType::get(Type::getInt1Ty(ctx), rounded_num_bitmap_entries);

                // TODO: find out what broke this...
                // // this will cause issues with optimization and compilation (misalignment for avx instructions?)
                // members[0] = ArrayType::get(Type::getInt1Ty(ctx), num_bitmap_entries);
            }

            // fill in based on indices
            for(unsigned i = 0; i < num_tuple_elements; ++i) {
                auto py_element_type = type.parameters()[i];

                // get rid off opt -> it's handled in bitmap idx
                auto is_opt_type = py_element_type.isOptionType();
                if(is_opt_type)
                    py_element_type = py_element_type.getReturnType();

                auto llvm_element_type = pythonToLLVMType(py_element_type);

                // special case: boolean -> force to i64, b.c. some LLVM passes are broken else
                if(python::Type::BOOLEAN == py_element_type)
                    llvm_element_type = i64Type();

                // save according to indices
                int value_idx = -1, size_idx = -1, bitmap_idx = -1;
                auto t_indices = getTupleIndices(type, i);
                std::tie(value_idx, size_idx, bitmap_idx) = t_indices;

                // quick check: for opt, bitmap should be set!
                if(is_opt_type)
                    assert(-1 != bitmap_idx);

                if(-1 != value_idx) {
                    assert(nullptr == members[value_idx]);
                    members[value_idx] = llvm_element_type;
                }

                if(-1 != size_idx) {
                    assert(nullptr == members[size_idx]);
                    members[size_idx] = size_field_type;
                }
            }

            // fill in missing fields, warn if incomplete
            for(unsigned i = 0; i < members.size(); ++i) {
                if(!members[i]) {
                    logger.error("when creating type for tuple, encountered missing member type.");
                    members[i] = i64Type();
                }
            }

            // quick check on size member type (must be i64)
            for(unsigned i = 0; i < num_tuple_elements; ++i) {
                int value_idx = -1, size_idx = -1, bitmap_idx = -1;
                auto t_indices = getTupleIndices(type, i);
                std::tie(value_idx, size_idx, bitmap_idx) = t_indices;

                if(-1 != size_idx) {
                    assert(size_idx >= 0 && size_idx < members.size());
                    assert(members[size_idx] == i64Type());
                }
            }

            if(num_bitmap_entries > 0) {
                auto first_type = members.front();
                assert(first_type->isArrayTy());
                assert(first_type->getArrayNumElements() % 64 == 0); // rounded
            }

            llvm::ArrayRef<llvm::Type *> llvm_members(members);
            assert(!packed);
            llvm::Type *structType = llvm::StructType::create(ctx, llvm_members, "struct." + twine, packed);

            // add to mapping (make sure it doesn't exist yet!)
            // assert(_typeMapping.find(structType) == _typeMapping.end());
            // _typeMapping[structType] = type;
            addType(structType, type);

            return structType;

//            assert(type.parameters().size() > 0);
//            // define type
//            std::vector<llvm::Type *> memberTypes;
//
//            auto params = type.parameters();
//            // count optional elements
//            int numNullables = 0;
//            for (int i = 0; i < params.size(); ++i) {
//                if (params[i].isOptionType()) {
//                    numNullables++;
//                    params[i] = params[i].withoutOptionsRecursive();
//                }
//
//                // empty tuple is ok, b.c. it's a primitive
//                assert(!params[i].isTupleType() ||
//                       params[i] == python::Type::EMPTYTUPLE); // no nesting at this level here supported!
//            }
//
//            // first, create bitmap as array of i1
//            if (numNullables > 0) {
//                // i1 array!
//                memberTypes.emplace_back(ArrayType::get(Type::getInt1Ty(ctx), numNullables));
//            }
//
//            // size fields at end
//            int numVarlenFields = 0;
//
//            // define bitmap on the fly
//            for (const auto &el: T.parameters()) {
//
//                // optimizing types -> use the actual, underlying type here
//                auto t = el.isConstantValued() ? el.underlying() : el;
//
//                // option
//                t = t.isOptionType() ? t.getReturnType() : t; // get rid of most outer options
//
//
//                // @TODO: special case empty tuple! also doesn't need to be represented
//
//                if (python::Type::BOOLEAN == t) {
//                    // i8
//                    // memberTypes.push_back(getBooleanType());
//
//                    // compiler bug, use i64 so everything can get optimized...
//                    memberTypes.push_back(i64Type());
//
//                } else if (python::Type::I64 == t) {
//                    // i64
//                    memberTypes.push_back(i64Type());
//                } else if (python::Type::F64 == t) {
//                    // double
//                    memberTypes.push_back(llvm::Type::getDoubleTy(ctx));
//                } else if (python::Type::STRING == t) {
//                    memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
//                    numVarlenFields++;
//                } else if (python::Type::PYOBJECT == t) {
//                    memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
//                    numVarlenFields++;
//                } else if ((python::Type::GENERICDICT == t || t.isDictionaryType()) && t != python::Type::EMPTYDICT) { // dictionary
//                    // special case structured dict
//                    if(t.isStructuredDictionaryType()) {
//                        memberTypes.push_back(getOrCreateStructuredDictType(t));
//                        // not classified as var field (var within).
//                    } else {
//                        // general i8* pointer to hold C-struct
//                        memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
//                        numVarlenFields++;
//                    }
//                } else if (t.isSingleValued()) {
//                    // leave out. Not necessary to represent it in memory.
//                } else if(t.isListType()) {
//                    memberTypes.push_back(getOrCreateListType(t)); // internal size field...
//                } else {
//                    // nested tuple?
//                    // ==> do lookup!
//                    // add i64 (for length)
//                    // and pointer type
//                    // previously defined? => get!
//                    if (t.isTupleType()) {
//                        // recurse!
//                        // add struct into it (can be accessed via recursion then!!!)
//                        memberTypes.push_back(getOrCreateTupleType(t, twine));
//                    } else {
//                        Logger::instance().logger("codegen").error(
//                                "not supported type " + el.desc() + " encountered in LLVM struct type creation");
//                        return nullptr;
//                    }
//                }
//            }
//
//            for (int i = 0; i < numVarlenFields; ++i)
//                memberTypes.emplace_back(size_field_type); // 64 bit int as size
//
//            llvm::ArrayRef<llvm::Type *> members(memberTypes);
//            llvm::Type *structType = llvm::StructType::create(ctx, members, "struct." + twine, packed);
//
//            // // add to mapping (make sure it doesn't exist yet!)
//            assert(_typeMapping.find(structType) == _typeMapping.end());
//            _typeMapping[structType] = type;
//
//            return structType;
        }

        llvm::Type *LLVMEnvironment::getOrCreateListType(const python::Type &listType, const std::string &twine) {
            assert(listType == python::Type::EMPTYLIST || listType.isListType());

            if(listType == python::Type::EMPTYLIST) return i8ptrType(); // dummy type
            auto it = _generatedListTypes.find(listType);
            if(_generatedListTypes.end() != it) {
                return it->second;
            }

            assert(listType.isListType() && listType != python::Type::EMPTYLIST);

            auto elementType = listType.elementType();

            bool elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();
            assert(elementType != python::Type::UNKNOWN);

            llvm::Type *retType;
            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    std::vector<llvm::Type*> memberTypes;
                    memberTypes.push_back(i64Type()); // array capacity
                    memberTypes.push_back(i64Type()); // size
                    memberTypes.push_back(i8ptrType()); // bool-array
                    llvm::ArrayRef<llvm::Type *> members(memberTypes);
                    retType = llvm::StructType::create(_context, members, "struct." + twine, false);
                } else {
                    // simple counter will do
                    retType = i64Type(); // just need to figure out number of fields stored!
                }
            } else if(elementType == python::Type::I64 || elementType == python::Type::F64 || elementType == python::Type::BOOLEAN) {
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                // array
                if(elementType == python::Type::I64) {
                    memberTypes.push_back(i64ptrType());
                } else if(elementType == python::Type::F64) {
                    memberTypes.push_back(doublePointerType());
                } else if(elementType == python::Type::BOOLEAN) {
                    memberTypes.push_back(getBooleanPointerType());
                }
                if(elements_optional)
                    memberTypes.push_back(i8ptrType()); // bool-array
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                memberTypes.push_back(llvm::PointerType::get(i8ptrType(), 0)); // str array (or i8* pointer array)
                memberTypes.push_back(i64ptrType()); // strlen array (with the +1 for \0)
                if(elements_optional)
                    memberTypes.push_back(i8ptrType()); // bool-array
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else if(elementType.isStructuredDictionaryType()) {
                auto llvm_element_type = getOrCreateStructuredDictType(elementType);

                // pointer to the structured dict type!
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                memberTypes.push_back(llvm::PointerType::get(llvm_element_type, 0));
                if(elements_optional)
                    memberTypes.push_back(i8ptrType()); // bool-array
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else if(elementType.isListType()) {
                // pointers to the list type!
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                memberTypes.push_back(llvm::PointerType::get(getOrCreateListType(elementType), 0));
                if(elements_optional)
                    memberTypes.push_back(i8ptrType()); // bool-array
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else if(elementType.isTupleType()) {
                // pointers to the flattened tuple type!
                std::vector<llvm::Type*> memberTypes;
                memberTypes.push_back(i64Type()); // array capacity
                memberTypes.push_back(i64Type()); // size
                memberTypes.push_back(llvm::PointerType::get(getOrCreateTupleType(elementType), 0)->getPointerTo());
                if(elements_optional)
                    memberTypes.push_back(i8ptrType()); // bool-array
                llvm::ArrayRef<llvm::Type *> members(memberTypes);
                retType = llvm::StructType::create(_context, members, "struct." + twine, false);
            } else {
                throw std::runtime_error("Unsupported list element type: " + listType.desc());
            }

            _generatedListTypes[listType] = retType;
            addType(retType, listType);
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
                    memberTypes.push_back(llvm::PointerType::get(getOrCreateListType(iterableType), 0));
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
                memberTypes.push_back(llvm::PointerType::get(getOrCreateListType(argType), 0));
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

#ifndef NDEBUG
            // check that indices are ok.
            {
                auto t_indices = getTupleIndices(tupleType, index);
                assert(validate_tuple_indices(tupleType, t_indices));
            }
#endif

            auto &ctx = builder.getContext();
            auto elementType = tupleType.parameters()[index];

            if(elementType.isConstantValued())
                return constantValuedTypeToLLVM(builder, elementType);

            // special types (not serialized in memory, i.e. constants to be constructed from typing)
            if (python::Type::NULLVALUE == elementType)
                return SerializableValue(nullptr, nullptr, i1Const(true));

            // constants...
            if(elementType.isConstantValued())
                return constantValuedTypeToLLVM(builder, elementType);

            // single valued elements are represented through nullptr
            if(elementType.isSingleValued()) // i.e., empty tuple, empty dict, ...
                return SerializableValue(nullptr, nullptr, nullptr);

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

                // constants...
                if(elementType.getReturnType().isConstantValued()) {
                    auto v = constantValuedTypeToLLVM(builder, elementType.getReturnType());
                    v.is_null = isnull;
                    return v;
                }

                // empty tuple, empty dict, ...
                if(elementType.getReturnType().isSingleValued()) {
                    // special case, just empty object
                    return SerializableValue(nullptr, nullptr, isnull);
                }

                // remove option first (handled above!)
                elementType = elementType.getReturnType();
            }

            // extract elements
            value = builder.CreateExtractValue(tupleVal, {valueOffset});

            // size existing? ==> only for varlen types
            if (!elementType.isFixedSizeType()) {

                // struct and list are extra
                if(elementType.isListType() || elementType.isStructuredDictionaryType()) {
                    size = i64Const(0);
                } else {
                    size = builder.CreateExtractValue(tupleVal, {sizeOffset});
                }
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

#ifndef NDEBUG
            // check that indices are ok.
            {
                auto t_indices = getTupleIndices(tupleType, index);
                assert(validate_tuple_indices(tupleType, t_indices));
            }
#endif

            auto& ctx = builder.getContext();
            auto elementType = tupleType.parameters()[index];

            // special types (not serialized in memory, i.e. constants to be constructed from typing)
            if (python::Type::NULLVALUE == elementType)
                return SerializableValue(nullptr, nullptr, i1Const(true));

            // constants...
            if(elementType.isConstantValued())
                return constantValuedTypeToLLVM(builder, elementType);

            // single valued elements are represented through nullptr
            if(elementType.isSingleValued()) // i.e., empty tuple, empty dict, ...
                return SerializableValue(nullptr, nullptr, nullptr);

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
            elementType = elementType.withoutOption();

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
                size= builder.CreateAdd(
                        builder.CreateCall(strlen_prototype(ctx, _module.get()), {cjsonstr}),
                        i64Const(1));
                return SerializableValue{ret, size, isnull};
            }

            // extract elements
            // auto structValIdx = builder.CreateStructGEP(tuplePtr, valueOffset);
            auto structValIdx = CreateStructGEP(builder, tuplePtr, valueOffset);
            value = builder.CreateLoad(structValIdx);

            // size existing? ==> only for varlen types
            if (!elementType.isFixedSizeType() && !elementType.isStructuredDictionaryType() && !elementType.isListType() && !elementType.isExceptionType()) {
                //  auto structSizeIdx = builder.CreateStructGEP(tuplePtr, sizeOffset);
                auto structSizeIdx = CreateStructGEP(builder, tuplePtr, sizeOffset);
                size = builder.CreateLoad(structSizeIdx);
            } else {
                // size from type
                size = i64Size;

                if(elementType.isListType() || elementType.isStructuredDictionaryType())
                    size = nullptr; // need to explicitly compute (costly)
            }


            // // add debug print here:
            // if(value)
            //     printValue(builder, value, "got element " + std::to_string(index) + " of tuple " + tupleType.desc() + ", value: ");
            // if(size)
            //     printValue(builder, size, "got element " + std::to_string(index) + " of tuple " + tupleType.desc() + ", size: ");

            // boolean: special case
            if(python::Type::BOOLEAN == elementType)
                value = builder.CreateZExtOrTrunc(value, getBooleanType());

#ifndef NDEBUG
            // debug:
            if(size && size->getType() != i64Type()) {
                std::cerr<<"ERROR::"<<std::endl;
                std::cerr<<"tuple element access:\n "<<printAggregateType(tuplePtr->getType()->getPointerElementType(), true)<<std::endl;
            }
#endif

            return SerializableValue(value, size, isnull);
        }

        void LLVMEnvironment::setTupleElement(llvm::IRBuilder<> &builder, const python::Type &tupleType,
                                              llvm::Value *tuplePtr, unsigned int index,
                                              const SerializableValue &value,
                                              bool is_volatile) {
            using namespace llvm;

            assert(tupleType.isTupleType());
            assert(tupleType.parameters().size() > index);

#ifndef NDEBUG
            // check that indices are ok.
            {
                auto t_indices = getTupleIndices(tupleType, index);
                assert(validate_tuple_indices(tupleType, t_indices));
            }
#endif

            auto &ctx = builder.getContext();
            auto elementType = tupleType.parameters()[index];

            // // debug: pretty print struct
            // std::cout<<this->printAggregateType(tuplePtr->getType()->getPointerElementType())<<std::endl;

            // special types which don't need to be stored because the type determines the value
            if (elementType.isSingleValued())
                return;

            // special case, constant valued - do not need to store
            if(elementType.isConstantValued()) // @TODO: unify with single-valued???
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
                builder.CreateStore(value.is_null, bitmapIdx, is_volatile);
            }

            // get rid off option & check again for special emptytuple/emptydict/...
            elementType = elementType.withoutOption();

            if(elementType.isSingleValued())
                return; // do not need to store, but bitmap is stored for them already.

            // extract elements
            // auto structValIdx = builder.CreateStructGEP(tuplePtr, valueOffset);
            auto structValIdx = CreateStructGEP(builder, tuplePtr, valueOffset);
            if (value.val) {
                auto v = value.val;
                // special case isStructDict or isListType b.c. they may be represented through a lazy pointer
                if(elementType.isListType() || elementType.isStructuredDictionaryType()) {
                    if(v->getType() != structValIdx->getType()) {
                        if(v->getType() == structValIdx->getType()->getPointerElementType()) {
                            // load (special treatment for nested structures...)
                            auto ptr = CreateFirstBlockAlloca(builder, v->getType());

                            // store -> direct load?
                            auto struct_type = v->getType();
                            for(unsigned i = 0; i < struct_type->getStructNumElements(); ++i) {
                                auto item = CreateStructLoad(builder, v, i);
                                // what is the type?
                                auto item_type = getLLVMTypeName(item->getType());
                                // if(item->getType()->isStructTy()) {
                                //     Logger::instance().logger("codegen").info("found struct type: " + item_type);
                                // }

                                auto target_idx = CreateStructGEP(builder, ptr, i);
                                builder.CreateStore(item, target_idx, is_volatile);
                            }

                            v = ptr;

                        } else {
                            throw std::runtime_error("incompatible type " + getLLVMTypeName(v->getType()) + " found for storing data.");
                        }
                    }
//
//                    if(v->getType() == structValIdx->getType())
//                        v = builder.CreateLoad(v); // load in order to store!

                    // however, nested structs/aggs should be memcopied
                    auto i8_src = builder.CreatePointerCast(v, i8ptrType());
                    auto i8_dest = builder.CreatePointerCast(structValIdx, i8ptrType());
                    auto& DL = _module->getDataLayout();
                    auto struct_size = DL.getTypeAllocSize(v->getType()->getPointerElementType());
                    builder.CreateMemCpy(i8_dest, 0, i8_src, 0, struct_size);
                } else {
                    // primitives can be stored

                    // special case: bool
                    if(python::Type::BOOLEAN == elementType)
                        v = builder.CreateZExt(v, i64Type());

                    builder.CreateStore(v, structValIdx, is_volatile);
                }
            }

            // size existing? ==> only for varlen types
            if (!elementType.isFixedSizeType() &&
                !elementType.isListType() &&
                !elementType.isStructuredDictionaryType()) {
                // auto structSizeIdx = builder.CreateStructGEP(tuplePtr, sizeOffset);
                auto structSizeIdx = CreateStructGEP(builder, tuplePtr, sizeOffset);

                auto size_to_store = value.size ? value.size : i64Const(0);
                builder.CreateStore(size_to_store, structSizeIdx, is_volatile);

                // printValue(builder, size_to_store, "size stored at index " + std::to_string(index));
            } else {
                if(value.size) {
                    // std::cerr<<"got size field for element"<<std::endl;
                }
            }

            // if(value.val)
            //     printValue(builder, value.val, "set element " + std::to_string(index) + " of tuple " + tupleType.desc() + ", value: ");
            // if(value.size)
            //     printValue(builder, value.size, "set element " + std::to_string(index) + " of tuple " + tupleType.desc() + ", size: ");

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

            // constants?
            if(type.isConstantValued()) {
                auto const_val = constantValuedTypeToLLVM(builder, type);
                return truthValueTest(builder, const_val, type.underlying());
            }

            // ==> return i1 directly
            if (val.val && val.val->getType() == i1Type())
                return val.val;

            // (), {}, None, ... are all False
            if (python::Type::NULLVALUE == type || python::Type::EMPTYTUPLE == type || python::Type::EMPTYDICT == type || python::Type::EMPTYLIST == type)
                return i1Const(false);

            if (python::Type::BOOLEAN == type) {
                llvm::Value* value = val.val;
                if(value && value->getType() != getBooleanType() && value->getType()->isIntegerTy()) {
                    value = builder.CreateZExtOrTrunc(value, getBooleanType());
                }

                assert(value && value->getType() == getBooleanType());
                return booleanToCondition(builder, value);
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
                Value *testValue = truthValueTest(builder, SerializableValue(val.val, val.size),
                                                  type.withoutOption());
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
            Function* func = getOrInsertFunction(_module.get(), "malloc", llvm::Type::getInt8PtrTy(_context, 0),
                                                           llvm::Type::getInt64Ty(_context));

            func->addFnAttr(Attribute::NoUnwind); // like in godbolt
            return builder.CreateCall(func, size);
        }

        llvm::Value* LLVMEnvironment::cfree(llvm::IRBuilder<> &builder, llvm::Value *ptr) {
            using namespace llvm;

            assert(ptr);

            // create external call to rtmalloc function
            Function* func = getOrInsertFunction(_module.get(), "free",
                    llvm::Type::getVoidTy(_context), llvm::Type::getInt8PtrTy(_context, 0));

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

            size_t max_str_print = 1024; // maximum 1024 chars.

            auto printf_F = printf_prototype(_context, _module.get());
            llvm::Value *sconst = nullptr;

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
                sconst = builder.CreateGlobalStringPtr(msg + " [f64] : %.12f\n");
            } else if (val->getType() == Type::getInt8PtrTy(_context, 0)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i8*] : [%p] %." + std::to_string(max_str_print) + "s\n");
            } else if(val->getType()->isPointerTy()) {
                // get internal name
                auto name = getLLVMTypeName(val->getType());
                casted_val = builder.CreatePointerCast(val, llvm::Type::getInt8PtrTy(_context, 0));
                sconst = builder.CreateGlobalStringPtr(msg + " [" + name + "] : [%p]\n");
            } else {
                sconst = builder.CreateGlobalStringPtr(getLLVMTypeName(val->getType()) + "[??] : [%p]\n");
                casted_val = i8nullptr();
            }

            assert(sconst);

            auto fmt = builder.CreatePointerCast(sconst, llvm::Type::getInt8PtrTy(_context, 0));
            if (val->getType() != Type::getInt8PtrTy(_context, 0))
                builder.CreateCall(printf_F, {fmt, casted_val});
            else
                builder.CreateCall(printf_F, {fmt, casted_val, casted_val});
        }

        void LLVMEnvironment::printHexValue(llvm::IRBuilder<> &builder, llvm::Value *val, std::string msg) {
            using namespace llvm;

            auto printf_F = printf_prototype(_context, _module.get());
            llvm::Value *sconst = builder.CreateGlobalStringPtr("unknown type: ??");

            llvm::Value *casted_val = val;
            // check type of value
            if (val->getType() == Type::getInt1Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i1] : 0x%" PRIx8 "\n");
                casted_val = builder.CreateSExt(val, i8Type());
            } else if (val->getType() == Type::getInt8Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i8] : 0x%" PRIx8 "\n");
            } else if (val->getType() == Type::getInt32Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i32] : 0x%" PRIx32 "\n");
            } else if (val->getType() == Type::getInt64Ty(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [i64] : 0x%" PRIx64 "\n");
            } else if (val->getType() == Type::getDoubleTy(_context)) {
                sconst = builder.CreateGlobalStringPtr(msg + " [f64] : 0x%" PRIx64 "\n");
                casted_val = builder.CreateBitCast(val, i64Type());
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
                t == python::Type::PYOBJECT)
                return Type::getInt8PtrTy(_context);

            if(t.isDictionaryType()) {
                if(t.isStructuredDictionaryType()) {
                    return getOrCreateStructuredDictType(t);
                } else {
                    return Type::getInt8PtrTy(_context);
                }
            }

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
                return getOrCreateListType(t);

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
                    std::vector<llvm::Type*> member_types;
                    member_types.push_back(Type::getInt64Ty(_context));
                    member_types.push_back(Type::getInt1Ty(_context));
                    llvm::ArrayRef<llvm::Type *> members(member_types);
                    return llvm::StructType::create(_context, members, "bool_opt", packed);
                }

                if (rt == python::Type::I64) {
                    std::vector<llvm::Type*> member_types;
                    member_types.push_back(Type::getInt64Ty(_context));
                    member_types.push_back(Type::getInt1Ty(_context));
                    llvm::ArrayRef<llvm::Type *> members(member_types);
                    return llvm::StructType::create(_context, members, "i64_opt", packed);
                }

                if (rt == python::Type::F64) {
                    std::vector<llvm::Type*> member_types;
                    member_types.push_back(Type::getDoubleTy(_context));
                    member_types.push_back(Type::getInt1Ty(_context));
                    llvm::ArrayRef<llvm::Type *> members(member_types);
                    return llvm::StructType::create(_context, members, "f64_opt", packed);
                }

                if (rt == python::Type::STRING || rt == python::Type::GENERICDICT || rt.isDictionaryType()) {

                    // this here results in an error.
                    // => fix this!
                    std::vector<llvm::Type*> member_types;
                    member_types.push_back(Type::getInt8PtrTy(_context));
                    member_types.push_back(Type::getInt1Ty(_context));
                    llvm::ArrayRef<llvm::Type *> members(member_types);

                    // could theoretically also use nullptr?
                    return llvm::StructType::create(_context, members, "str_opt", packed);
                }

                if (rt.isListType()) {

                    // same issue here
                    std::vector<llvm::Type*> member_types;
                    member_types.push_back(getOrCreateListType(rt));
                    member_types.push_back(Type::getInt1Ty(_context));
                    llvm::ArrayRef<llvm::Type *> members(member_types);
                    return llvm::StructType::create(_context, members, "list_opt", packed);
                }
            }

            // optimized type? -> deoptimize!
            if(t.isOptimizedType())
                return pythonToLLVMType(t.underlying());

            // special case: exception type -> map to i64
            if(t.isExceptionType())
                return Type::getInt64Ty(_context); // <-- simply hold exception code.

            throw std::runtime_error("could not convert type '" + t.desc() + "' to LLVM compliant type");
            return nullptr;
        }

        std::string LLVMEnvironment::printAggregateType(llvm::Type* agg_type, bool print_python_type) {

            std::stringstream ss;

            // is it an aggregate type? if not, print.
            if(agg_type->isStructTy()) {
                // print name
                ss<<getLLVMTypeName(agg_type)<<"\n";
                if(print_python_type) {
                    // look up in all the types
                    auto py_types = lookupPythonTypes(agg_type);
                    ss<<"found "<<pluralize(py_types.size(), "possible python type")<<":\n";
                    for(unsigned i = 0; i < py_types.size(); ++i) {
                        ss<<"-- "<<py_types[i].desc()<<"\n";
                    }
                }
                for(unsigned i = 0; i < agg_type->getStructNumElements(); ++i) {
                    auto el_type = agg_type->getStructElementType(i);
                    ss<<"  "<<getLLVMTypeName(el_type)<<"  gep 0 "<<i<<"\n";
                }
            } else {
                ss<<getLLVMTypeName(agg_type);
            }

            return ss.str();
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
           return ::tuplex::codegen::stringCompare(builder, ptr, str, include_zero);
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
            auto hmap_get_func = getModule()->getOrInsertFunction("hashmap_get", hmap_func_type);
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
            auto hmap_get_func = getModule()->getOrInsertFunction("int64_hashmap_get", hmap_func_type);
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


            return list_length(*this, builder, val, listType);

            throw std::runtime_error("deprecated! replace!");

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
            builder.CreateStore(env.i64Const(0), i64_val); // <-- store before parse.
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
            auto cf = createUnaryIntrinsic(builder, llvm::Intrinsic::ID::ceil, value);
            auto fabs_value = createUnaryIntrinsic(builder, llvm::Intrinsic::ID::fabs, builder.CreateFSub(cf, value));

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

        // "Call parameter type does not match function signature!\n  %4 = alloca %struct.tuple\n %struct.tuple.1*  %216 = call i64 @fill_in_delays(%struct.tuple.0* %152, %struct.tuple* %4)\n"
        std::string LLVMEnvironment::decodeFunctionParameterError(const std::string& err_message) {
            using namespace std;
            stringstream ss;
            if(strStartsWith(err_message, "Call parameter type does not match function signature!")) {

                // lookup function type
                if(_module) {

                    // find func name in error message!
                    smatch match;
                    regex r("@[a-zA-Z_]+[a-zA-Z0-9_]*\\(");
                    string subject = err_message;
                    string func_name;
                    while(regex_search(subject, match, r)) {
                        func_name = match.str(0);
                        func_name = func_name.substr(1, func_name.length() - 2);
                        subject = match.suffix().str();
                    }

                    llvm::Function* func = _module->getFunction(func_name);
                    if(func) {
                        // function has two parameters input, output
                        std::vector<llvm::Argument *> args;
                        for (auto &arg : func->args()) {
                           args.emplace_back(&arg);
                        }

                        // when it's a udf, should be two tuples
                        if(args.size() == 2 && args[0]->getType()->isPointerTy() && args[1]->getType()->isPointerTy()) {
                            assert(args[0]->getType()->isPointerTy());
                            assert(args[1]->getType()->isPointerTy());

                            // fetch input/output type
                            auto out_llvm_type = args[0]->getType()->getPointerElementType();
                            auto in_llvm_type = args[1]->getType()->getPointerElementType();

                            // look python types up
                            auto in_llvm_name = getLLVMTypeName(in_llvm_type);
                            auto out_llvm_name = getLLVMTypeName(out_llvm_type);
                            ss<<"function @" +func_name<<": "<<in_llvm_name<<" -> "<<out_llvm_name<<"\n";

                            // get name from stored tuples
                            auto in_type = lookupPythonType(in_llvm_name);
                            auto out_type = lookupPythonType(out_llvm_name);
                            ss<<"corresponds to: "<<in_type.desc()<<" -> "<<out_type.desc()<<"\n";
                        }
                    } else {
                        ss<<"function @" + func_name + " not found in LLVM module\n";
                    }
                }


                // fetch all the tuple structs and look up the types...
                // regex is: %struct.tuple(\.?\d+)?
                smatch match;
                regex r("struct.tuple(\\.?\\d+)?");
                set<string> S;
                string subject = err_message;
                while(regex_search(subject, match, r)) {
                    S.insert(match.str(0));
                    subject = match.suffix().str();
                }

                for(const auto& el : S) {
                    // find in structs
                    auto type = lookupPythonType(el);
                    if(python::Type::UNKNOWN != type) {
                        ss<<el<<": "<<type.desc()<<"\n";
                    }
                }
            }
            return ss.str();
        }

        SerializableValue LLVMEnvironment::dummyValue(llvm::IRBuilder<> &builder, const python::Type &type) {
            // dummy value needs to be created for llvm to combine stuff.
            SerializableValue retVal;
            if (python::Type::BOOLEAN == type || python::Type::I64 == type) {
                retVal.val = i64Const(0);
                retVal.size = i64Const(sizeof(int64_t));
                retVal.is_null = i1Const(false);
            } else if (python::Type::F64 == type) {
                retVal.val = f64Const(0.0);
                retVal.size = i64Const(sizeof(double));
                retVal.is_null = i1Const(false);
            } else if (python::Type::STRING == type || type.isDictionaryType()) {
                // use empty string, DO NOT use nullptr.
                retVal.val = strConst(builder, "");
                retVal.size = i64Const(1);
                retVal.is_null = i1Const(false);
            } else {
                Logger::instance().logger("codegen").warn("Requested dummy for type " + type.desc() + " but not yet implemented");
            }
            return retVal;
        }

        SerializableValue LLVMEnvironment::upcastValue(llvm::IRBuilder<>& builder, const SerializableValue &val,
                                                      const python::Type &type,
                                                      const python::Type &targetType) {
            if(!canUpcastType(type, targetType))
                throw std::runtime_error("types " + type.desc() + " and " + targetType.desc() + " not compatible for upcasting");

            if(type == targetType)
                return val;

            // the types are different

            // constant -> underlying?
            if(type.isConstantValued() && canUpcastType(type.underlying(), targetType)) {
                // get the constant and upcast then!
                auto const_val = constantValuedTypeToLLVM(builder, type);
                return upcastValue(builder, const_val, type.underlying(), targetType);
            }

            // primitives
            // bool -> int -> f64
            if(type == python::Type::BOOLEAN) {
                if(targetType == python::Type::I64) {
                    // widen to i64
                    return SerializableValue(upCast(builder, val.val, i64Type()), i64Const(sizeof(int64_t)));
                }
                if(targetType == python::Type::F64)
                    // widen to f64
                    return SerializableValue(upCast(builder, val.val, doubleType()), i64Const(sizeof(int64_t)));
            }
            // int -> f64
            if(type == python::Type::I64 && targetType == python::Type::F64)
                return SerializableValue(upCast(builder, val.val, doubleType()), i64Const(sizeof(int64_t)));

            // null to Option[Any]
            if(type == python::Type::NULLVALUE && targetType.isOptionType()) {

                // need to create dummy value so LLVM works...
                auto baseType = targetType.getReturnType();
                auto tmp = dummyValue(builder, baseType);
                return SerializableValue(tmp.val, tmp.size, i1Const(true));
            }

            // primitive to option?
            if(!type.isOptionType() && targetType.isOptionType()) {
                auto tmp = upcastValue(builder, val, type, targetType.withoutOption());
                return SerializableValue(tmp.val, tmp.size, i1Const(false));
            }

            // option[a] to option[b]?
            if(type.isOptionType() && targetType.isOptionType()) {
                auto tmp = upcastValue(builder, val, type.withoutOption(), targetType.withoutOption());
                return SerializableValue(tmp.val, tmp.size, val.is_null);
            }

            if(type.isTupleType() && targetType.isTupleType()) {
                if(type.parameters().size() != targetType.parameters().size()) {
                    throw std::runtime_error("upcasting tuple type " + type.desc() + " to tuple type " + targetType.desc()
                          + " not possible, because tuples contain different amount of parameters!");
                }
                // upcast tuples
                // Load as FlattenedTuple
                FlattenedTuple val_tuple = FlattenedTuple::fromLLVMStructVal(this, builder, val.val, type);
                FlattenedTuple target_tuple(this);
                target_tuple.init(targetType);

                auto num_elements = type.parameters().size();

                // put to flattenedtuple (incl. assigning tuples!)
                for (int i = 0; i < num_elements; ++i) {
                    // retrieve from tuple itself and then upcast!
                    auto el_type = val_tuple.fieldType(i);
                    auto el_target_type = target_tuple.fieldType(i);
                    SerializableValue el(val_tuple.get(i), val_tuple.getSize(i), val_tuple.getIsNull(i));
                    auto el_target = upcastValue(builder, el, el_type, el_target_type);
                    target_tuple.setElement(builder, i, el_target.val, el_target.size, el_target.is_null);
                }

                // get loadable struct type
                auto ret = target_tuple.getLoad(builder);
                assert(ret->getType()->isStructTy());
                auto size = target_tuple.getSize(builder);
                return SerializableValue(ret, size);
            }

            // @TODO: Issue https://github.com/LeonhardFS/Tuplex/issues/214
            // @TODO: List[a] to List[b] if a,b are compatible?
            // @TODO: Dict[a, b] to Dict[c, d] if upcast a to c works, and upcast b to d works?
            if(type.isListType() && targetType.isListType()) {
                // @TODO:
                throw std::runtime_error("upcasting list type " + type.desc() + " to list type " + targetType.desc() + " not yet implemented");
            }

            if(type.isDictionaryType() && targetType.isDictionaryType()) {

                // are both struct dicts? -> that case can be solved
                if((type == python::Type::EMPTYDICT || type.isStructuredDictionaryType()) && targetType.isStructuredDictionaryType())
                    return struct_dict_upcast(*this, builder, val, type, targetType);

                throw std::runtime_error("upcasting dict type " + type.desc() + " to dict type " + targetType.desc() + " not yet implemented");
            }

            if(type.isConstantValued()) {
                auto u = constantValuedTypeToLLVM(builder, type);
                return upcastValue(builder, u, type.underlying(), targetType);
            }

            throw std::runtime_error("can not generate code to upcast " + type.desc() + " to " + targetType.desc());
            return val;
        }

        // mutex to make codegen thread-safe

        llvm::Type *
        LLVMEnvironment::getOrCreateStructuredDictType(const python::Type &structType, const std::string &twine) {

            // fix for empty dict
            if(python::Type::EMPTYDICT == structType)
                return getOrCreateEmptyDictType();

            assert(structType.isStructuredDictionaryType());

            // call helper function from StructDictHelper.h
            auto it = _generatedStructDictTypes.find(structType);
            if(it != _generatedStructDictTypes.end())
                return it->second;
            auto type = generate_structured_dict_type(*this, twine, structType);
            _generatedStructDictTypes[structType] = type;
            addType(type, structType);
            return type;
        }

        llvm::Type *LLVMEnvironment::getOrCreateEmptyDictType() {

            // check if it exists in cache
            auto it = _generatedStructDictTypes.find(python::Type::EMPTYDICT);

            if(it != _generatedStructDictTypes.end())
                return it->second;

            // generate type
            llvm::ArrayRef<llvm::Type *> members;
            bool packed = false;
            llvm::Type *structType = llvm::StructType::create(_module->getContext(), members, "emptydict", packed);
            _generatedStructDictTypes[python::Type::EMPTYDICT] = structType;
            addType(structType, python::Type::EMPTYDICT);
            return structType;
        }

        SerializableValue CreateDummyValue(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& type) {
            using namespace llvm;

            // dummy value needs to be created for llvm to combine stuff.
            SerializableValue retVal;

            // special case, option type:
            if(type.isOptionType()) {
                // recurse
                retVal = CreateDummyValue(env, builder, type.getReturnType());
                retVal.is_null = env.i1Const(true);
            } else if (python::Type::BOOLEAN == type || python::Type::I64 == type) {
                retVal.val = env.i64Const(0);
                retVal.size = env.i64Const(sizeof(int64_t));
            } else if (python::Type::F64 == type) {
                retVal.val = env.f64Const(0.0);
                retVal.size = env.i64Const(sizeof(double));
            } else if (python::Type::STRING == type || type.isDictionaryType()) {

                // special case: struct dict!
                if(python::Type::EMPTYDICT == type) {
                    retVal.val = env.CreateFirstBlockAlloca(builder, env.getOrCreateEmptyDictType());
                } else if(type.isStructuredDictionaryType()) {
                    retVal.val = env.CreateFirstBlockAlloca(builder, env.getOrCreateStructuredDictType(type));
                    struct_dict_mem_zero(env, builder, retVal.val, type);
                } else {
                    // generic dict...
                    retVal.val = env.i8ptrConst(nullptr);
                    retVal.size = env.i64Const(0);
                }
            } else if(python::Type::NULLVALUE == type) {
                retVal.is_null = env.i1Const(true);
            } else {
                throw std::runtime_error("not support to create dummy value for " + type.desc() + " yet.");
            }
            return retVal;
        }
    }
}