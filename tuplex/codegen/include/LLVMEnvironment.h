//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LLVMENVIRONMENT_H
#define TUPLEX_LLVMENVIRONMENT_H

#include "llvm/Support/MemoryBuffer.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/IRBuilder.h"

#include <ASTNodes.h>
#include <CodegenHelper.h>

#include <memory>
#include <TypeSystem.h>
#include <Row.h>
#include <unordered_map>
#include <tuple>
#include <cfloat>

#include "InstructionCountPass.h"

namespace tuplex {
    namespace codegen {
        /*!
 * helper class to generate LLVM Code into one module. Captures all globals necessary for LLVM based
 * code generation. Also provides helper functions to create individual LLVM code pieces.
 */

        /*!
         * get index for value, size and bitmapPosition
         * @param tupleType
         * @param index which tuple element
         * @return tuple of val idx, size idx, bitmap idx
         */
        extern std::tuple<size_t, size_t, size_t> getTupleIndices(const python::Type& tupleType, size_t index);

        /*!
         * return number of 64bit bitmap elements...
         * @param tupleType
         * @return
         */
        extern size_t calcBitmapElementCount(const python::Type& tupleType);

        class LLVMEnvironment {
        private:
            llvm::LLVMContext _context;
            std::unique_ptr<llvm::Module> _module;
            std::map<python::Type, llvm::Type *> _generatedTupleTypes;
            std::map<python::Type, llvm::Type *> _generatedListTypes;
            // use llvm struct member types for map key since iterators with the same yieldType may have different llvm structs
            std::map<std::vector<llvm::Type *>, llvm::Type *> _generatedIteratorTypes;
            // string: function name; BlockAddress*: BlockAddress* to be filled in an iterator struct
            std::map<std::string, llvm::BlockAddress *> _generatedIteratorUpdateIndexFunctions;
            std::map<llvm::Type *, python::Type> _typeMapping;
            llvm::Type *createTupleStructType(const python::Type &type, const std::string &twine = "tuple");

            void init(const std::string &moduleName = "tuplex");

            void release();

            // flag to check whether freeAll should generate code
            bool _memoryRequested;
            std::unordered_map<std::string, llvm::Function*> _generatedFunctionCache; // store generated helper functions...

            // variables to store metadata about globals

            // [_global_counters] is a map from twine to count, so that the names of each type of global variable are unique (e.g. if there are many re_search calls, their globals will each get a unique name)
            std::map<std::string, int> _global_counters;
            // [_global_regex_cache] is a map from a regex pattern string to the global compiled pcre2 pattern.
            std::map<std::string, llvm::Value *> _global_regex_cache;


            // The way that the initGlobal(), releaseGlobal() functions work: they start out with an entry block that allocates
            // an int64 variable with the return code (initialized to 0 and saved in _initGlobalRetValue/_releaseGlobalRetValue).
            // The entry block branches to a return block that returns this value.
            // Then, when getInitGlobalBuilder()/getReleaseGlobalBuilder() are called, a new block is inserted right after the
            // entry block, with the terminator properly set up to return early if _initGlobalRetValue/_releaseGlobalRetValue is
            // set to a nonzero value during the block's execution. The caller just has to insert the actual global initialization/
            // release code and set the value of the return variable appropriately.
            llvm::BasicBlock* _initGlobalRetBlock;
            llvm::BasicBlock* _initGlobalEntryBlock;
            llvm::Value* _initGlobalRetValue;

            llvm::BasicBlock* _releaseGlobalRetBlock;
            llvm::BasicBlock* _releaseGlobalEntryBlock;
            llvm::Value* _releaseGlobalRetValue;
            // Returns a builder into which global variable release can be inserted.
            llvm::IRBuilder<> getReleaseGlobalBuilder(const std::string &block_name);

            std::unique_ptr<llvm::legacy::FunctionPassManager> _fpm; // lazy initialized function pass manager for quick optimization of function

        public:
            LLVMEnvironment(const std::string& moduleName="tuplex") : _module(nullptr),
                                                                      _memoryRequested(false) {
                init(moduleName);
            }

            ~LLVMEnvironment() {
                release();
            }

            llvm::LLVMContext &getContext() { return _context; }

            std::unique_ptr<llvm::Module> &getModule() { return _module; }

            // Returns a builder into which global variable initialization can be inserted.
            llvm::IRBuilder<> getInitGlobalBuilder(const std::string &block_name);

//            void preOptimize(llvm::Function* func) {
// run https://github.com/llvm-mirror/llvm/blob/master/lib/Transforms/IPO/PassManagerBuilder.cpp then whatever is in populateFunctionPassManager.
// if func is nullptr, then simply optimize on all functions. Else, just on a specific one => this should help to make optimization faster...
//            }


            // generate default null value depending on type
            llvm::Constant* nullConstant(llvm::Type* type) {
                if(type->isIntegerTy()) {
                    return llvm::Constant::getIntegerValue(type, llvm::APInt(type->getIntegerBitWidth(), 0));
                } else if(type->isDoubleTy()) {
                    return llvm::ConstantFP::get(_context, llvm::APFloat(0.0));
                } else if(type->isPointerTy()) {
                    return llvm::ConstantPointerNull::get(llvm::cast<llvm::PointerType>(type));
                } else {
                    throw std::runtime_error("unknown type to null found");
                }
            }

            /*!
             * returns LLVM IR Code as string
             * @return
             */
            std::string getIR() const {
                std::string ir_string;
                llvm::raw_string_ostream os{ir_string};
                assert(_module);
                _module->print(os, nullptr, false);
                os.flush();
                return ir_string;
            }


//            /*!
//             * returns LLVM bitcode as buffer & size
//             * @return buffer
//             */
//             uint8_t* bu

            // see https://github.com/cmu-db/peloton/blob/1de89798f271804f8be38a71219a20e761a1b4b6/src/codegen/code_context.cpp on how to implement
            std::string getAssembly() const;

            /*!
             * creates (or returns already created) LLVM type for a tuple type
             * @param tupleType must be a tuple type
             * @param twine optional name for the type
             * @return pointer to LLVM Type struct, nullptr if errors occured.
             */
            inline llvm::Type *getOrCreateTupleType(const python::Type &tupleType,
                                                    const std::string &twine = "tuple") {
                assert(tupleType.isTupleType());

                // check if already generated
                auto it = _generatedTupleTypes.find(tupleType);
                if (_generatedTupleTypes.end() != it)
                    return it->second;
                else {
                    llvm::Type *t = createTupleStructType(tupleType, twine);
                    std::string name = t->getStructName();
                    _generatedTupleTypes[tupleType] = t;
                    return t;
                }
            }

            llvm::Type *getOrCreateTuplePtrType(const python::Type &tupleType, const std::string &twine = "tuple") {
                return llvm::PointerType::get(getOrCreateTupleType(tupleType, twine), 0);
            }

            /*!
             * return the type that is used to represent the list internally
             * if holding null/emptytuple/emptydict/emptylist, just returns i64
             * otherwise, represents a resizable array through a struct with size, array (for strings, use two parallel arrays for size, char*)
             * @param listType the Tuplex type of the list
             * @param twine an identifier for the codegen
             * @return llvm Type to be used as the given listType
             */
            llvm::Type *getListType(const python::Type &listType, const std::string &twine = "list");

            /*!
             * return LLVM type that is used to represent a iterator internally
             * @param iteratorInfo iterator-specific annotation of the target iterator
             * @return llvm type corresponding to the iterator with iteratorInfo
             */
            llvm::Type *createOrGetIteratorType(const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * return LLVM type that is used to represent a iterator generated from iter() call internally
             * @param iterableType argument type of the iter() call
             * @param twine
             * @return llvm type corresponding to the iterator generated from iterableType
             */
            llvm::Type *createOrGetIterIteratorType(const python::Type &iterableType, const std::string &twine = "iterator");

            /*!
             * return LLVM type that is used to represent a reverseiterator generated from reversed() call internally
             * @param argType argument type of the reversed() call, currently can be list, tuple, string, range
             * @param twine
             * @return llvm type corresponding to the reverseiterator generated from argType
             */
            llvm::Type *createOrGetReversedIteratorType(const python::Type &argType, const std::string &twine = "reverseiterator");

            /*!
             * return LLVM type that is used to represent a iterator generated from zip() call internally
             * @param argsType type of arguments of the zip() call
             * @param argsIteratorInfo iterator-specific annotations of arguments of the zip() call
             * @param twine
             * @return llvm type corresponding to the iterator with iteratorInfo
             */
            llvm::Type *createOrGetZipIteratorType(const python::Type &argsType, const std::vector<std::shared_ptr<IteratorInfo>> &argsIteratorInfo, const std::string &twine = "zip_iterator");

            /*!
             * return LLVM type that is used to represent a iterator generated from enumerate() call internally
             * @param argType type of the first argument of the enumerate() call
             * @param argIteratorInfo iterator-specific annotation of the first argument of the enumerate() call
             * @param twine
             * @return llvm type corresponding to the iterator with iteratorInfo
             */
            llvm::Type *createOrGetEnumerateIteratorType(const python::Type &argType, const std::shared_ptr<IteratorInfo> &argIteratorInfo, const std::string &twine = "enumerate_iterator");

            /*!
             * retrieve tuple element from pointer
             * @param builder
             * @param tupleType
             * @param tuplePtr pointer to llvm struct type
             * @param index
             * @return
             */
            SerializableValue getTupleElement(llvm::IRBuilder<>& builder, const python::Type& tupleType, llvm::Value* tuplePtr, unsigned int index);

            /*!
             * same as getTupleElement, but for a struct val. I.e. for a val where CreateLoad was done on a tuple ptr.
             * @param builder
             * @param tupleType
             * @param tuplePtr
             * @param index
             * @return
             */
            SerializableValue extractTupleElement(llvm::IRBuilder<>& builder, const python::Type& tupleType, llvm::Value* tupleVal, unsigned int index);

            void setTupleElement(llvm::IRBuilder<> &builder, const python::Type &tupleType, llvm::Value *tuplePtr,
                                 unsigned int index, const SerializableValue &value);

            llvm::Value* CreateMaximum(llvm::IRBuilder<>& builder, llvm::Value* rhs, llvm::Value* lhs);

            /*!
             * convert constant data to LLVM value represenation
             * @param builder
             * @param f
             * @return LLVM representation of constant data
             */
            SerializableValue primitiveFieldToLLVM(llvm::IRBuilder<>& builder, const Field& f);

            /*!
             * returns whatever is used to represent a boolean type. Should be i8. Why? Because byte is the smallest addressable unit
             * makes no sense to use i1 - though available in LLVM
             * @return llvm Type to be used as boolean
             */
            inline llvm::Type *getBooleanType() {
                return llvm::IntegerType::get(_context, 8);
            }

            inline llvm::Type *getBooleanPointerType() {
                return llvm::PointerType::get(getBooleanType(), 0);
            }

            inline llvm::Type *i8Type() {
                return llvm::Type::getInt8Ty(_context);
            }

            inline llvm::Type *i8ptrType() {
                return llvm::Type::getInt8PtrTy(_context, 0);
            }

            inline llvm::Type *i32Type() {
                return llvm::Type::getInt32Ty(_context);
            }

            inline llvm::Type *i64Type() {
                return llvm::Type::getInt64Ty(_context);
            }

            inline llvm::Type* i1Type() {
                return llvm::Type::getInt1Ty(_context);
            }

            inline llvm::Type *i64ptrType() {
                return llvm::Type::getInt64PtrTy(_context, 0);
            }
            inline llvm::Type *i32ptrType() {
                return llvm::Type::getInt32PtrTy(_context, 0);
            }

            inline llvm::Type *doubleType() {
                return llvm::Type::getDoubleTy(_context);
            }

            inline llvm::Type *doublePointerType() {
                return llvm::Type::getDoublePtrTy(_context, 0);
            }

            /*!
             * Represents the [matchObject] struct in Runtime.h. This struct is used to hold a pcre2 ovector (e.g. the
             * indices of match groups) and the underlying subject string that the match was run over.
             * @return  matchObject struct pointer llvm::Type
             */
            inline llvm::Type *getMatchObjectPtrType() {
                return llvm::PointerType::get(llvm::StructType::get(_context, {llvm::Type::getInt64PtrTy(_context, 0),
                                                        llvm::Type::getInt8PtrTy(_context, 0),
                                                        llvm::Type::getInt64Ty(_context)}), 0);
            }

            /*!
             * Represents a [range] object. It holds the start, stop, step parameters of a range sequence.
             * @return  range struct llvm::Type
             */
            inline llvm::Type *getRangeObjectType() {
                return llvm::StructType::get(_context, {i64Type(), i64Type(), i64Type()});
            }

            /*!
             * internally cmp returns an llvm i1 object. want to upcast to boolean type
             * @param val
             * @return upcasted val
             */
            inline llvm::Value *upcastToBoolean(llvm::IRBuilder<> &builder, llvm::Value *val) {
                if (val->getType()->getIntegerBitWidth() != getBooleanType()->getIntegerBitWidth())
                    return builder.CreateZExt(val, getBooleanType());
                else
                    return val;
            }

            inline llvm::Value *upCast(llvm::IRBuilder<> &builder, llvm::Value *val, llvm::Type *type) {
                // check if types are the same, then just return val
                if (val->getType() == type)
                    return val;
                else {
                    // is boolean?
                    if (val->getType() == getBooleanType()) {
                        // goal can be either i64 or double
                        if (type == i64Type())
                            return builder.CreateZExt(val, i64Type());
                        else if (type ==doubleType())
                            return builder.CreateSIToFP(val, doubleType());
                        else {
                            throw std::runtime_error("fatal error: could not upcast type");
                            return nullptr;
                        }
                    } else if (val->getType() == i64Type()) {
                        // only upcast to double possible
                        if (type == doubleType())
                            return builder.CreateSIToFP(val, doubleType());
                        else {
                            throw std::runtime_error("upcast only to double possible!");
                            return nullptr;
                        }
                    } else if (val->getType() == doubleType()) {
                        // if already double, all ok
                        if (type == doubleType())
                            return val;
                        else {
                            throw std::runtime_error("can't upcast double further");
                            return nullptr;
                        }
                    } else {
                        std::stringstream ss;
                        ss << "no upcast from " << getLLVMTypeName(val->getType()) << " to "
                        << getLLVMTypeName(type) << " possible. Wrong parameters?";
                        throw std::runtime_error(ss.str());
                        return nullptr;
                    }
                }
            }

            /*!
             * creates llvm code for boolean value corresponding to this environment
             * @param builder belonging to basic block where to insert instruction
             * @param b value
             * @return llvm::Value*
             */
            llvm::Value *boolConst(const bool b) {
                auto t = getBooleanType();
                return llvm::Constant::getIntegerValue(t, llvm::APInt(t->getIntegerBitWidth(), b));
            }

            /*!
             * create integer constant
             * @param val
             * @return
             */
            inline llvm::Constant *i32Const(const int32_t val) {
                return llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(_context), llvm::APInt(32, val));
            }

            inline llvm::Constant *i64Const(const int64_t val) {
                return llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(_context), llvm::APInt(64, val));
            }

            inline llvm::Constant* f64Const(const double val) {
                return llvm::ConstantFP::get(_context, llvm::APFloat(val));
            }

            inline llvm::Constant *i8Const(const char val) {
                return llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(_context), llvm::APInt(8, val));
            }


            inline llvm::Constant *i1Const(const bool value) {
                return llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(_context), llvm::APInt(1, value));
            }

            inline llvm::Constant* i8ptrConst(const uint8_t* ptr) {
                return llvm::Constant::getIntegerValue(llvm::Type::getInt8PtrTy(_context, 0), llvm::APInt(64, reinterpret_cast<uint64_t>(ptr)));
            }

            inline llvm::Constant* i64nullptr() const {
                return llvm::ConstantPointerNull::get(llvm::Type::getInt64PtrTy(const_cast<LLVMEnvironment*>(this)->getContext(), 0));
            }

            inline llvm::Constant* i8nullptr() const {
                return llvm::ConstantPointerNull::get(llvm::Type::getInt8PtrTy(const_cast<LLVMEnvironment*>(this)->getContext(), 0));
            }

            inline llvm::Value* strConst(llvm::IRBuilder<>& builder, const std::string& s) {
                assert(builder.GetInsertBlock()->getParent()); // make sure block has a parent, else pretty bad bugs could happen...

                auto sconst = builder.CreateGlobalStringPtr(s);
                return builder.CreatePointerCast(sconst, llvm::Type::getInt8PtrTy(_context, 0));
            }

            /*!
             * requests memory for runtime objects
             * @param builder LLVM builder where to insert GC at
             * @param size number of bytes requested
             * @return i8* pointer to memory region with size bytes
             */
            llvm::Value *malloc(llvm::IRBuilder<> &builder, llvm::Value *size);

            /*!
             * call C's malloc function (need to generate free code as well!)
             * @param builder
             * @param size
             * @return
             */
            llvm::Value* cmalloc(llvm::IRBuilder<>& builder, llvm::Value *size);

            /*!
             * call C's free function (need to make sure it works with malloc)
             * @param builder
             * @param ptr
             * @return
             */
            llvm::Value* cfree(llvm::IRBuilder<>& builder, llvm::Value* ptr);

            /*!
             * frees all previously allocated memory regions through the runtime (memory management implemented in Runtime.c)
             * if no mallocs have been performed, generates no code
             */
            void freeAll(llvm::IRBuilder<> &builder);

            /*!
             * helper function for debug purposes to print out llvm types
             * @param t
             * @return
             */
            static std::string getLLVMTypeName(llvm::Type *t);


            /*!
             * retrieves this environments struct type/stub for the empty tuple type
             * @return valid llvm type representing an empty tuple
             */
            llvm::Type *getEmptyTupleType();

            /*!
             * creates i1 cmp condition for checking the index
             * @param builder
             * @param val
             * @param numElements
             * @return value holding the result whether 0 <= val < numElements
             */
            llvm::Value* indexCheck(llvm::IRBuilder<>& builder, llvm::Value* val, llvm::Value* numElements);

            inline llvm::Value* indexCheck(llvm::IRBuilder<>& builder, llvm::Value* val, int64_t numElements) {
                return indexCheck(builder, val, i64Const(numElements));
            }

            inline llvm::Value* createNullInitializedGlobal(const std::string& name, llvm::Type* type) {
                _module->getOrInsertGlobal(name, type);
                llvm::GlobalVariable *gVar = _module->getNamedGlobal(name);
                gVar->setInitializer(nullConstant(type));
                return gVar;
            }

            /*!
             * logical negation (DO NOT USE CreateNeg!)
             * @return i1 logically negated. I.e. 0 => 1 amd 1 => 0
             */
            inline llvm::Value* i1neg(llvm::IRBuilder<>& builder, llvm::Value *val) {
                assert(val->getType() == llvm::Type::getInt1Ty(_context));
                return builder.CreateSub(i1Const(true), val);
            }

            void debugPrint(llvm::IRBuilder<>& builder, const std::string& message, llvm::Value* value=nullptr);

            void debugCellPrint(llvm::IRBuilder<>& builder, llvm::Value* cellStart, llvm::Value* cellEnd);


            llvm::Value* booleanToCondition(llvm::IRBuilder<>& builder, llvm::Value* val) {
                assert(val->getType() == getBooleanType());
                return builder.CreateTrunc(val, llvm::Type::getInt1Ty(_context));
            }

            /*!
             * debug print any llvm value
             * @param builder
             */
            void printValue(llvm::IRBuilder<>& builder, llvm::Value*, std::string msg="");

            llvm::Type* pythonToLLVMType(const python::Type &t);

            /*!
             * get nth bit as i1 back
             * @param builder
             * @param value value from where to read nth bit
             * @param idx n
             * @return i1 containing true/false
             */
            llvm::Value* extractNthBit(llvm::IRBuilder<>& builder, llvm::Value* value, llvm::Value* idx);

            /*!
             * generates code to perform Python3 compliant integer floor division, i.e. //
             * @param left must be i64 signed integer
             * @param right must be i64 signed integer
             * @return i64 signed integer holding the result
             */
            llvm::Value* floorDivision(llvm::IRBuilder<>& builder, llvm::Value* left, llvm::Value* right);

            /*!
             * generates code to perform Python3 compliant floor division. Note, both operands must have the same type
             * @param builder builder
             * @param left either i64 or double
             * @param right either i64 or double
             * @return result.
             */
            llvm::Value* floorModulo(llvm::IRBuilder<>& builder, llvm::Value* left, llvm::Value* right);


            /*!
             * creates basic block logic to store val in ptr if ptr is not a nullptr.
             * @param builder builder, will be changed to new block
             * @param val value to store
             * @param ptr where to store val when ptr is not null
             */
            void storeIfNotNull(llvm::IRBuilder<>& builder, llvm::Value* val, llvm::Value* ptr);


            /*!
             * check if string is zero terminated, if not copy contents & terminate with zero
             * @param builder
             * @param str
             * @param size
             * @param copy whether to copy to a new str with rtmalloc or simply zero terminate if necessary
             * @return
             */
            llvm::Value* zeroTerminateString(llvm::IRBuilder<>& builder, llvm::Value* str, llvm::Value* size, bool copy=true);

            /*!
             * compares memory at ptr to string.
             * @param builder
             * @param ptr
             * @param str
             * @param include_zero whether to check for zero at end too.
             * @return
             */
            llvm::Value* fixedSizeStringCompare(llvm::IRBuilder<>& builder, llvm::Value* ptr, const std::string& str, bool include_zero=false);


            /*!
             * checks whether value is an integer (i.e. for float there's no number after .)
             * @param builder
             * @param value some value to check for
             * @param eps epsilon value to use for floats, per default DBL_EPSILON from float.h (also what CPython uses)
             * @return i1 indicating true/false
             */
            llvm::Value* isInteger(llvm::IRBuilder<>& builder, llvm::Value* value, llvm::Value* eps=nullptr);

            /*!
             * create alloca instruction in first block of function. Helpful for variables within loops
             * @param builder
             * @param llvmType
             * @return allocated result
             */
            static inline llvm::Value* CreateFirstBlockAlloca(llvm::IRBuilder<>& builder,
                                                              llvm::Type* llvmType,
                                                              const std::string& name="") {
                auto ctorBuilder = getFirstBlockBuilder(builder);

                auto res = ctorBuilder.CreateAlloca(llvmType, 0, nullptr, name);
                assert(res);
                return res;
            }

            inline llvm::Constant* defaultEpsilon() {
                return f64Const(DBL_EPSILON);
            }

            llvm::Value* double_eq(llvm::IRBuilder<>& builder, llvm::Value* value, llvm::Value* eps=nullptr) {
                assert(value && value->getType() == doubleType());

                if(!eps)
                    eps = defaultEpsilon();
                return builder.CreateFCmpOLT(builder.CreateUnaryIntrinsic(llvm::Intrinsic::ID::fabs, value), eps);
            }

            /*!
             * creates a new var based on value's type.
             * @param builder
             * @param initialValue
             * @param name
             * @return pointer to new var
             */
            inline llvm::Value* CreateFirstBlockVariable(llvm::IRBuilder<>& builder,
                                                              llvm::Constant* initialValue,
                                                              const std::string& name="") {
                assert(initialValue);

                auto ctorBuilder = getFirstBlockBuilder(builder);
                auto llvmType = initialValue->getType();
                auto res = ctorBuilder.CreateAlloca(llvmType, 0, nullptr, name);
                ctorBuilder.CreateStore(initialValue, res);
                assert(res);
                return res;
            }

            /*!
             * set a variable to NULL (i.e. 0 for integers, 0.0 for floats, nullptr for pointers)
             * @param builder
             * @param ptr pointer variable
             */
            inline void storeNULL(llvm::IRBuilder<>& builder, llvm::Value* ptr) {
                assert(ptr->getType()->isPointerTy());

                // set respective nullptr or null value
                auto elType = ptr->getType()->getPointerElementType();
                builder.CreateStore(nullConstant(elType), ptr);
            }

            /*!
             * generate comparisons against an array of strings
             * @param builder
             * @param ptr
             * @param null_values
             * @param ptrIsZeroTerminated if true, then check also the 0 char. If false, the check becomes a prefix check.
             * @return
             */
            inline llvm::Value* compareToNullValues(llvm::IRBuilder<>& builder,
                                                    llvm::Value* ptr,
                                                    const std::vector<std::string>& null_values,
                                                    bool ptrIsZeroTerminated=false) {
                if(null_values.empty())
                    return i1Const(false);

                // Todo: optimize further, i.e. sometimes better to do check incl. null or depending on string size.

                // for "" include 0 in comparison!
                llvm::Value* isnull = fixedSizeStringCompare(builder,
                                                             ptr,
                                                             null_values.front(),
                                                             null_values.front().empty() | ptrIsZeroTerminated);
                for(int i = 1; i < null_values.size(); ++i)
                    isnull = builder.CreateOr(isnull, fixedSizeStringCompare(builder,
                                                                             ptr,
                                                                             null_values[i],
                                                                             null_values[i].empty() | ptrIsZeroTerminated));
                return isnull;
            }

            std::string stats(bool include_detailed_counts=false) const {
                char name[] = "inst count";
                InstructionCounts inst_count(*name);
                inst_count.runOnModule(*_module);
                return inst_count.formattedStats(include_detailed_counts);
            }

            /*!
             * python truth value testing according to https://docs.python.org/3/library/stdtypes.html
             * @param val
             * @param type
             * @return
             */
            llvm::Value* truthValueTest(llvm::IRBuilder<>& builder, const SerializableValue& val, const python::Type& type);


            /*!
             * converts double to runtime allocated string
             * @param builder
             * @param value must be doubletype
             * @return runtime allocated string together with size
             */
            SerializableValue f64ToString(llvm::IRBuilder<>& builder, llvm::Value* value);

            /*!
             * converts int to runtime allocated string
             * @param builder
             * @param value must be doubletype
             * @return runtime allocated string together with size
             */
            SerializableValue i64ToString(llvm::IRBuilder<>& builder, llvm::Value* value);

            static inline llvm::Value* CreateStructGEP(llvm::IRBuilder<>& builder, llvm::Value* ptr, unsigned int idx, const llvm::Twine& Name="") {
#if LLVM_VERSION_MAJOR < 9
                // compatibility
                return builder.CreateConstInBoundsGEP2_32(nullptr, ptr, 0, idx, Name);
#else
                return builder.CreateStructGEP(ptr, idx);
#endif
            }

            /*!
             * creates logic for cond ? <trueblock> : <elseblock>
             * @param condition
             * @param trueBlock
             * @param elseBlock
             * @return value of result of conditionally executing ifBlock or elseBlock!
             */
            llvm::Value* CreateTernaryLogic(llvm::IRBuilder<> &builder, llvm::Value *condition,
                                                             std::function<llvm::Value *(
                                                                     llvm::IRBuilder<> &)> ifBlock,
                                                             std::function<llvm::Value *(
                                                                     llvm::IRBuilder<> &)> elseBlock);

            /*!
             * return the length/size of a list.
             * @param builder
             * @param val
             * @param listType
             * @return i64 containing the size of the list.
             */
            llvm::Value* getListSize(llvm::IRBuilder<>& builder, llvm::Value* val, const python::Type& listType);

            /*!
             * Creates a global pcre2 jit compiled regex pattern using the given [regexPattern]. Uses [twine] as a
             * name for the global variable. Caches the pattern objects based on the pattern string to avoid duplicating
             * work.
             * @param twine
             * @param regexPattern
             * @return the created global jit-compiled regex pattern
             */
            llvm::Value *addGlobalRegexPattern(const std::string& twine, const std::string &regexPattern);

            /*!
             * Adds global pcre2 contexts that hold the runtime malloc and free functions so that pcre2 functions can be
             * used at runtime. Only creates these global values once.
             * @return A tuple with llvm::Value* for the pcre2 general context, match context, and compile context, in
             * that order.
             */
            std::tuple<llvm::Value*, llvm::Value*, llvm::Value*> addGlobalPCRE2RuntimeContexts();

            llvm::Value* callGlobalsInit(llvm::IRBuilder<>& builder);
            llvm::Value* callGlobalsRelease(llvm::IRBuilder<>& builder);

            llvm::Value* callBytesHashmapGet(llvm::IRBuilder<>& builder, llvm::Value* hashmap, llvm::Value* key, llvm::Value* key_size, llvm::Value* returned_bucket);

            /*!
             * Call get on an int64 hashmap (utils/int_hashmap.h) with an int64 key; load value into returned_bucket argument
             * @return i1 condition if the key was found or not
             */
            llvm::Value *callIntHashmapGet(llvm::IRBuilder<>& builder, llvm::Value *hashmap, llvm::Value *key, llvm::Value *returned_bucket);
            /*!
             * generate i1 condition for whether codeValue is of ExceptionCode ec incl. base classes etc.
             * @param builder
             * @param codeValue
             * @param ec
             * @return codegenerated i1 true/false
             */
            llvm::Value* matchExceptionHierarchy(llvm::IRBuilder<>& builder, llvm::Value* codeValue, const ExceptionCode& ec);

            /*!
             * Create or get a llvm function with signature i1(struct.iterator) that does the following:
             * Increments (or decrements if reverse==true) index field of the input struct.iterator,
             * then returns true if the iterator is exhausted, and false otherwise.
             * Explaination about the BlockAddress: It's one of the fields of list/tuple/... iterator structs.
             * Normally it's the address of the block that updates and checks an iterator's index, but as soon as the iterator is exhausted,
             * this field will be set to the address of a block that always returns true. When next() gets called on an exhausted iterator,
             * it can then tell whether the iterator is exhausted without having to check if index+1 > iterableObjectLength.
             * (It's not the only way to achieve this given the current implementation,
             * and can be replaced by an additional field "i1 iteratorExhausted" in iterator struct.)
             * @param builder
             * @param iterableType
             * @param reverse should only be used for reverseiterator
             * @return llvm::BlockAddress* to be stored in an iterator struct later
             */
            llvm::BlockAddress *createOrGetUpdateIteratorIndexFunctionDefaultBlockAddress(llvm::IRBuilder<> &builder,
                                                                                          const python::Type &iterableType,
                                                                                          bool reverse=false);
        };

// i.e. there should be a function
// print(const char* format, ...)
// ... can be llvm::Values, real string values, llvm::Type, ...
// function should handle it
// ==> use special %t for type e.g.
// need to validate format str with rest of values!!!


        static inline llvm::Function *printf_prototype(llvm::LLVMContext &ctx, llvm::Module *mod) {
            using namespace tuplex::codegen;
            using namespace llvm;

            FunctionType *printf_type = FunctionType::get(ctypeToLLVM<int>(ctx), {ctypeToLLVM<char*>(ctx)}, true);

#if LLVM_VERSION_MAJOR < 9
            Function *func = cast<Function>(mod->getOrInsertFunction(
            "printf", printf_type));
#else
            Function *func = cast<Function>(mod->getOrInsertFunction(
                    "printf", printf_type).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* snprintf_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            // cf. https://stackoverflow.com/questions/28168815/adding-a-function-call-in-my-ir-code-in-llvm
            using namespace llvm;
            using namespace tuplex::codegen;

            FunctionType *snprintf_type = FunctionType::get(ctypeToLLVM<int>(ctx), {ctypeToLLVM<char*>(ctx)}, true);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("snprintf", snprintf_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("snprintf", snprintf_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* memcmp_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto int64_type = llvm::Type::getInt64Ty(ctx);

            FunctionType *memcmp_type = llvm::FunctionType::get(int64_type, {char_ptr_type, char_ptr_type, int64_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("memcmp", memcmp_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("memcmp", memcmp_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* ststr_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *strstr_type = llvm::FunctionType::get(char_ptr_type, {char_ptr_type, char_ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strstr", strstr_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strstr", strstr_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* strlen_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *strlen_type = llvm::FunctionType::get(llvm::Type::getInt64Ty(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)},
                                                                false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strlen", strlen_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strlen", strlen_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* fastatod_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *fastatod_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx),
                                                                  {ptr_type, ptr_type, llvm::Type::getDoublePtrTy(ctx, 0)},
                                                                  false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("fast_atod", fastatod_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("fast_atod", fastatod_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* fastatob_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *fastatod_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx),
                                                                  {ptr_type, ptr_type, ptr_type},
                                                                  false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("fast_atob", fastatod_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("fast_atob", fastatod_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* fastatoi_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *fastatoi_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx),
                                                                  {ptr_type, ptr_type, llvm::Type::getInt64PtrTy(ctx, 0)},
                                                                  false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("fast_atoi64", fastatoi_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("fast_atoi64", fastatoi_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* count_prototype(llvm::LLVMContext &ctx, llvm::Module *mod) {
            using namespace llvm;

            auto int64_type = llvm::Type::getInt64Ty(ctx);
            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *count_proto = llvm::FunctionType::get(int64_type, {char_ptr_type, char_ptr_type, int64_type, int64_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strCount", count_proto));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strCount", count_proto).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* rfind_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *strstr_type = llvm::FunctionType::get(llvm::Type::getInt64Ty(ctx), {char_ptr_type, char_ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strRfind", strstr_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strRfind", strstr_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* isdecimal_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto int8_type = llvm::Type::getInt8Ty(ctx);

            FunctionType *isdecimal_type = llvm::FunctionType::get(int8_type, {char_ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsDecimal", isdecimal_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsDecimal", isdecimal_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* isdigit_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto int8_type = llvm::Type::getInt8Ty(ctx);

            FunctionType *isdigit_type = llvm::FunctionType::get(int8_type, {char_ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsDigit", isdigit_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsDigit", isdigit_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* isalpha_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto int8_type = llvm::Type::getInt8Ty(ctx);

            FunctionType *isalpha_type = llvm::FunctionType::get(int8_type, {char_ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsAlpha", isalpha_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsAlpha", isalpha_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* isalnum_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto int8_type = llvm::Type::getInt8Ty(ctx);

            FunctionType *isalnum_type = llvm::FunctionType::get(int8_type, {char_ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsAlNum", isalnum_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strIsAlNum", isalnum_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* strip_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *function_type = llvm::FunctionType::get(char_ptr_type, {char_ptr_type, char_ptr_type,
                                                                                  llvm::Type::getInt64PtrTy(ctx, 0)}, false);
#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strStrip", function_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strStrip", function_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* rstrip_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *function_type = llvm::FunctionType::get(char_ptr_type, {char_ptr_type, char_ptr_type,
                                                                                  llvm::Type::getInt64PtrTy(ctx, 0)}, false);
#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strRStrip", function_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strRStrip", function_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* lstrip_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *function_type = llvm::FunctionType::get(char_ptr_type, {char_ptr_type, char_ptr_type,
                                                                                  llvm::Type::getInt64PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strLStrip", function_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strLStrip", function_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* replace_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto char_ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            FunctionType *strstr_type = llvm::FunctionType::get(char_ptr_type, {char_ptr_type, char_ptr_type, char_ptr_type, llvm::Type::getInt64Ty(ctx)->getPointerTo(0)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strReplace", strstr_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strReplace", strstr_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* malloc_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            static_assert(sizeof(size_t) == sizeof(int64_t), "size_t should be same size as a 64bit integer");

            FunctionType *malloc_type = llvm::FunctionType::get(llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt64Ty(ctx), false);

#if LLVM_VERSION_MAJOR < 9
            Function *func = cast<Function>(mod->getOrInsertFunction("malloc", malloc_type));
#else
            Function *func = cast<Function>(mod->getOrInsertFunction("malloc", malloc_type).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* strFormat_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            using namespace tuplex::codegen;

            FunctionType *strformat_type = FunctionType::get(ctypeToLLVM<char*>(ctx), {ctypeToLLVM<char*>(ctx),
                                                                                       ctypeToLLVM<int64_t*>(ctx), ctypeToLLVM<char*>(ctx)}, true);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strFormat", strformat_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strFormat", strformat_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* strJoin_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            using namespace tuplex::codegen;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), // result string
                    {
                            llvm::Type::getInt8PtrTy(ctx, 0), // base string
                            llvm::Type::getInt64Ty(ctx), // length of base string
                            llvm::Type::getInt64Ty(ctx), // length of list
                            llvm::PointerType::get(llvm::Type::getInt8PtrTy(ctx, 0), 0), // list strings
                            llvm::Type::getInt64PtrTy(ctx, 0), // list string lengths
                            llvm::Type::getInt64PtrTy(ctx, 0), // result length
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strJoin", functionType));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strJoin", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* strSplit_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            using namespace tuplex::codegen;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt64Ty(ctx), // serialized size of resulting list
                    {
                            llvm::Type::getInt8PtrTy(ctx, 0), // base string
                            llvm::Type::getInt64Ty(ctx), // length of base string
                            llvm::Type::getInt8PtrTy(ctx, 0), // delim string
                            llvm::Type::getInt64Ty(ctx), // length of delim string
                            llvm::PointerType::get(llvm::PointerType::get(llvm::Type::getInt8PtrTy(ctx, 0), 0), 0), // result list strings
                            llvm::PointerType::get(llvm::Type::getInt64PtrTy(ctx, 0), 0), // result list string lengths
                            llvm::Type::getInt64PtrTy(ctx, 0), // result list length
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("strSplit", functionType));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("strSplit", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* floatToStr_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            using namespace tuplex::codegen;

            FunctionType *floatToStr_type = FunctionType::get(ctypeToLLVM<char*>(ctx), {ctypeToLLVM<double>(ctx),
                                                                                        ctypeToLLVM<int64_t*>(ctx)}, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("floatToStr", floatToStr_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("floatToStr", floatToStr_type).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* cJSONCreateObject_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(llvm::Type::getInt8PtrTy(ctx, 0), false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateObject", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateObject", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONDetachItemViaPointer_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {ptr_type, ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_DetachItemViaPointer", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_DetachItemViaPointer", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONGetArraySize_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto int_type = llvm::Type::getInt64Ty(ctx);
            FunctionType *functionType = llvm::FunctionType::get(int_type, {ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_GetArraySize", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_GetArraySize", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONPrintUnformatted_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_PrintUnformatted", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_PrintUnformatted", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONParse_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_Parse", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_Parse", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONCreateNumber_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            auto double_type = llvm::Type::getDoubleTy(ctx);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {double_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateNumber", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateNumber", functionType).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* cJSONCreateString_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = Type::getInt8PtrTy(ctx, 0);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateString", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateString", functionType).getCallee());
#endif

            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONCreateBool_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);

            // the function type here is wrong.
            // a bool is in C++ not necessarily a byte physically
            // => the compiler chooses the representation
            // ==> use char for explicit bool representation
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {Type::getInt64Ty(ctx)}, false);
            // check https://llvm.org/devmtg/2017-02-04/Restrict-Qualified-Pointers-in-LLVM.pdf why noalias doesn't work here.

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateBool", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_CreateBool", functionType).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* cJSONAddItemToObject_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {ptr_type, ptr_type, ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_AddItemToObject", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_AddItemToObject", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONGetObjectItem_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            auto ptr_type = llvm::Type::getInt8PtrTy(ctx, 0);
            FunctionType *functionType = llvm::FunctionType::get(ptr_type, {ptr_type, ptr_type}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_GetObjectItemCaseSensitive", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_GetObjectItemCaseSensitive", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* cJSONIsTrue_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            FunctionType *functionType = llvm::FunctionType::get(llvm::Type::getInt64Ty(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)},
                                                                 false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_IsTrue", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("cJSON_IsTrue", functionType).getCallee());
#endif

            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2Compile_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            // function signature of pcre2_compile_8 is
            //  pcre2_real_code_8 *pcre2_compile_8(PCRE2_SPTR8, size_t, uint32_t, int *, size_t *, pcre2_real_compile_context_8 *)

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), // compiled regex
                    {
                        llvm::Type::getInt8PtrTy(ctx, 0), // pattern
                        llvm::Type::getInt64Ty(ctx), // length
                        llvm::Type::getInt32Ty(ctx), // options
                        llvm::Type::getInt32PtrTy(ctx, 0), // errorcode
                        llvm::Type::getInt64PtrTy(ctx, 0), // erroroffset
                        llvm::Type::getInt8PtrTy(ctx, 0) // compile context
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_compile_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_compile_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2CodeFree_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_code_free_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_code_free_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2MatchDataCreateFromPattern_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(llvm::Type::getInt8PtrTy(ctx, 0),
                                                                 {llvm::Type::getInt8PtrTy(ctx, 0),
                                                                  llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_data_create_from_pattern_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_data_create_from_pattern_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2Substitute_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            // https://www.pcre.org/current/doc/html/pcre2_substitute.html
            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt32Ty(ctx), // number of matches/error
                    {
                            llvm::Type::getInt8PtrTy(ctx, 0), // code
                            llvm::Type::getInt8PtrTy(ctx, 0), // subject
                            llvm::Type::getInt64Ty(ctx), // subject length
                            llvm::Type::getInt64Ty(ctx), // start offset
                            llvm::Type::getInt32Ty(ctx), // options
                            llvm::Type::getInt8PtrTy(ctx, 0), // match data ptr
                            llvm::Type::getInt8PtrTy(ctx, 0), // match context ptr
                            llvm::Type::getInt8PtrTy(ctx, 0), // replacement
                            llvm::Type::getInt64Ty(ctx), // replacement length
                            llvm::Type::getInt8PtrTy(ctx, 0), // output buffer
                            llvm::Type::getInt64PtrTy(ctx, 0), // output buffer length
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_substitute_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_substitute_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2Match_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt32Ty(ctx), // number of matches/error
                    {
                            llvm::Type::getInt8PtrTy(ctx, 0), // code
                            llvm::Type::getInt8PtrTy(ctx, 0), // subject
                            llvm::Type::getInt64Ty(ctx), // subject length
                            llvm::Type::getInt64Ty(ctx), // start offset
                            llvm::Type::getInt32Ty(ctx), // options
                            llvm::Type::getInt8PtrTy(ctx, 0), // match data ptr
                            llvm::Type::getInt8PtrTy(ctx, 0), // match context ptr
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2JITMatch_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt32Ty(ctx), // number of matches/error
                    {
                            llvm::Type::getInt8PtrTy(ctx, 0), // code
                            llvm::Type::getInt8PtrTy(ctx, 0), // subject
                            llvm::Type::getInt64Ty(ctx), // subject length
                            llvm::Type::getInt64Ty(ctx), // start offset
                            llvm::Type::getInt32Ty(ctx), // options
                            llvm::Type::getInt8PtrTy(ctx, 0), // match data ptr
                            llvm::Type::getInt8PtrTy(ctx, 0), // match context ptr
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_jit_match_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_jit_match_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2JITCompile_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt32Ty(ctx), // number of matches/error
                    {
                            llvm::Type::getInt8PtrTy(ctx, 0), // code
                            llvm::Type::getInt32Ty(ctx), // options
                    },
                    false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_jit_compile_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_jit_compile_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* wrapPCRE2MatchObject_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::PointerType::get(llvm::StructType::get(ctx, {llvm::Type::getInt64PtrTy(ctx, 0),
                                                                            llvm::Type::getInt8PtrTy(ctx, 0),
                                                                            llvm::Type::getInt64Ty(ctx)}), 0),
                    {llvm::Type::getInt8PtrTy(ctx, 0),
                     llvm::Type::getInt8PtrTy(ctx, 0), llvm::Type::getInt64Ty(ctx)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("wrapPCRE2MatchObject", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("wrapPCRE2MatchObject", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2GetLocalGeneralContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), {}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetLocalGeneralContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetLocalGeneralContext", functionType).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2GetGlobalGeneralContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), {}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetGlobalGeneralContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetGlobalGeneralContext", functionType).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2MatchContextCreate_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_context_create_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_context_create_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2GetGlobalMatchContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), {}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetGlobalMatchContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetGlobalMatchContext", functionType).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2CompileContextCreate_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_compile_context_create_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_compile_context_create_8", functionType).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2GetGlobalCompileContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt8PtrTy(ctx, 0), {}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetGlobalCompileContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2GetGlobalCompileContext", functionType).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2GeneralContextFree_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_general_context_free_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_general_context_free_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2ReleaseGlobalGeneralContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2ReleaseGlobalGeneralContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2ReleaseGlobalGeneralContext", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2MatchContextFree_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_context_free_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_match_context_free_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2ReleaseGlobalMatchContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2ReleaseGlobalMatchContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2ReleaseGlobalMatchContext", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2CompileContextFree_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_compile_context_free_8", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2_compile_context_free_8", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* pcre2ReleaseGlobalCompileContext_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getVoidTy(ctx), {llvm::Type::getInt8PtrTy(ctx, 0)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2ReleaseGlobalCompileContext", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("pcre2ReleaseGlobalCompileContext", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* uniformInt_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;

            FunctionType *functionType = llvm::FunctionType::get(
                    llvm::Type::getInt64Ty(ctx), {llvm::Type::getInt64Ty(ctx), llvm::Type::getInt64Ty(ctx)}, false);

#if LLVM_VERSION_MAJOR < 9
            auto func = cast<Function>(mod->getOrInsertFunction("uniform_int", functionType));
#else
            auto func = cast<Function>(mod->getOrInsertFunction("uniform_int", functionType).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* quoteForCSV_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            using namespace tuplex::codegen;
            // extern char* quoteForCSV(const char *str, int64_t size, int64_t* new_size, char separator, char quotechar);
            FunctionType *strformat_type = FunctionType::get(ctypeToLLVM<char*>(ctx),
                                                             {ctypeToLLVM<char*>(ctx),
                                                              ctypeToLLVM<int64_t>(ctx),
                                                              ctypeToLLVM<int64_t*>(ctx),
                                                              ctypeToLLVM<char>(ctx),
                                                              ctypeToLLVM<char>(ctx)},
                                                             false);

#if LLVM_VERSION_MAJOR < 9
            Function *func = cast<Function>(mod->getOrInsertFunction("quoteForCSV", strformat_type));
#else
            Function *func = cast<Function>(mod->getOrInsertFunction("quoteForCSV", strformat_type).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* free_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            static_assert(sizeof(size_t) == sizeof(int64_t), "size_t should be same size as a 64bit integer");

            FunctionType *free_type = llvm::FunctionType::get(llvm::Type::getVoidTy(ctx), llvm::Type::getInt8PtrTy(ctx, 0), false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("free", free_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("free", free_type).getCallee());
#endif
            // func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* i64toa_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            // int i64toa_sse2(int64_t value, char* buffer)
            FunctionType *i64toa_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx),
                                                                {llvm::Type::getInt64Ty(ctx),
                                                                 llvm::Type::getInt8PtrTy(ctx, 0)}, false);
#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("i64toa_sse2", i64toa_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("i64toa_sse2", i64toa_type).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* d2fixed_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            // int d2fixed_buffered_n(double d, uint32_t precision, char* result);
            FunctionType *d2fixed_type = llvm::FunctionType::get(llvm::Type::getInt32Ty(ctx),
                                                                {llvm::Type::getDoubleTy(ctx),
                                                                 llvm::Type::getInt32Ty(ctx),
                                                                 llvm::Type::getInt8PtrTy(ctx, 0)}, false);
#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("d2fixed_buffered_n", d2fixed_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("d2fixed_buffered_n", d2fixed_type).getCallee());
#endif
            //func->addAttribute(1U, Attribute::NoAlias);
            return func;
        }

        static inline llvm::Function* powi64_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            //extern int64_t pow_i64(int64_t base, int64_t exp);
            FunctionType *func_type = llvm::FunctionType::get(llvm::Type::getInt64Ty(ctx),
                                                                 {llvm::Type::getInt64Ty(ctx),
                                                                  llvm::Type::getInt64Ty(ctx)}, false);
#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("pow_i64", func_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("pow_i64", func_type).getCallee());
#endif
            return func;
        }

        static inline llvm::Function* powf64_prototype(llvm::LLVMContext& ctx, llvm::Module* mod) {
            using namespace llvm;
            //extern double pow_f64(double base, int64_t exp);
            FunctionType *func_type = llvm::FunctionType::get(llvm::Type::getDoubleTy(ctx),
                                                              {llvm::Type::getDoubleTy(ctx),
                                                               llvm::Type::getInt64Ty(ctx)}, false);
#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction("pow_f64", func_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction("pow_f64", func_type).getCallee());
#endif
            return func;
        }


        // parse functions for individual cells
        extern  SerializableValue parseBoolean(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::BasicBlock *bbFailed,
                                               llvm::Value *str, llvm::Value *strSize,
                                               llvm::Value *isnull);
        extern SerializableValue parseF64(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::BasicBlock *bbFailed,
                                          llvm::Value *str, llvm::Value *strSize,
                                          llvm::Value *isnull);
        extern SerializableValue parseI64(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::BasicBlock *bbFailed,
                                          llvm::Value *str, llvm::Value *strSize,
                                          llvm::Value *isnull);


    }
}

#endif //TUPLEX_LLVMBUILDER_H