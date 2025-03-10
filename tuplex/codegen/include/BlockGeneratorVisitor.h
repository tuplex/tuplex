//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_BLOCKGENERATORVISITOR_H
#define TUPLEX_BLOCKGENERATORVISITOR_H


#include "IVisitor.h"
#include <IFailable.h>

#include <llvm/ADT/APFloat.h>
#include <llvm/ADT/STLExtras.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include "ClosureEnvironment.h"

#include <deque>
#include <ApatheticVisitor.h>
#include <LLVMEnvironment.h>
#include <Token.h>
#include <LambdaFunction.h>
#include <FunctionRegistry.h>
#include <stack>
#include <IteratorContextProxy.h>

#include <CodegenHelper.h>

namespace tuplex {

namespace codegen {

    /*!
     * helper function to get declared variables below any AST node.
     * Useful for pre-allocating variables on stack for a function or lambda
     * @param root
     * @return map of variables + list of all types attained.
     */
    extern std::map<std::string, std::vector<python::Type>> getDeclaredVariables(ASTNode* root);

    /*!
     * helper function to get the named comprehension target variables within a lambda.
     * Used to preallocate these variables on the stack for a lambda
     * @param root
     * @return list of variables + types
     */
    extern std::vector<std::tuple<std::string, python::Type>> getComprehensionVariables(NLambda* root);


    // visitor to generate LLVM IR
    class BlockGeneratorVisitor : public ApatheticVisitor, public IFailable {
    private:

        void declareVariables(ASTNode* func);

        struct Variable {
            llvm::Value *ptr;
            llvm::Value *sizePtr;
            llvm::Value *nullPtr;
            llvm::Type* llvm_type;
            python::Type type;
            std::string name;

            LLVMEnvironment* env;

            Variable() : ptr(nullptr), sizePtr(nullptr), nullPtr(nullptr), llvm_type(nullptr), name("undefined"), env(nullptr) {}

            Variable(LLVMEnvironment& env, const codegen::IRBuilder& builder, const python::Type& t, const std::string& name);

            static Variable asGlobal(LLVMEnvironment& env, const codegen::IRBuilder& builder,
                            const python::Type& t,
                            const std::string& name, const SerializableValue& value);

            inline void endLife(codegen::IRBuilder&builder) {
                if(ptr)
                    builder.CreateLifetimeEnd(ptr);
                if(sizePtr)
                    builder.CreateLifetimeEnd(sizePtr);
                if(nullPtr)
                    builder.CreateLifetimeEnd(nullPtr);
                ptr = nullptr;
                sizePtr = nullptr;
                nullPtr = nullptr;
            }

            // simplify interfaces a bit
            inline codegen::SerializableValue load(codegen::IRBuilder& builder) const {
                assert(ptr && sizePtr);

                // GlobalValue is a constant...
                // // hack to make certain things faster, i.e. for global constants don't do the loading instructions...
                // if(llvm::isa<llvm::Constant>(ptr)) {
                //     // return directly
                //     assert(llvm::isa<llvm::Constant>(sizePtr));
                //     if(nullPtr)
                //         assert(llvm::isa<llvm::Constant>(nullPtr));
                // }

                assert(type != python::Type::UNKNOWN && llvm_type);

                // special case empty types, use dummy
                if(type.isSingleValued()) {
                    if(python::Type::EMPTYITERATOR == type) // <-- for now only support iterator, check for empty list & Co.
                        return {}; // <-- nullptr
                }

                // special case iterator: Load here a pointer (because it points to a concrete iter and not a value, i.e. implement here pass-by-ref sermantics.)
                // TODO: need to do the same for lists and other objects
                // only load immutable elements directly -> TODO: extend this here! -> maybe refactor better to capture object properties?
                llvm::Value* value = nullptr;
                if(passByValue()) {
                    // load value
                    value = builder.CreateLoad(llvm_type, ptr);

                } else {
                    assert(!llvm_type->isPointerTy());
                    // load reference
                    value = builder.CreateLoad(llvm_type->getPointerTo(), ptr);
                }

                // iterator slot may not have ptr yet
                return codegen::SerializableValue(value, builder.CreateLoad(builder.getInt64Ty(), sizePtr),
                        nullPtr ? builder.CreateLoad(builder.getInt1Ty(), nullPtr) : nullptr);
            }

            inline void store(const codegen::IRBuilder& builder, const codegen::SerializableValue& val) {
                assert(ptr && sizePtr);

                if(val.val) {

                    // new: -> simply store to pointer.

                    // LLVM9 pointer type check
                    if(passByValue()) {
#ifndef NDEBUG
                        if(val.val->getType()->getPointerTo() != ptr->getType()) {
                            std::stringstream err;
                            err<<"attempting to store value of LLVM type "<<env->getLLVMTypeName(val.val->getType())<<" to slot expecting LLVM type "<<env->getLLVMTypeName(ptr->getType());
                            Logger::instance().logger("codegen").error(err.str());
                        }
#endif
                        assert(val.val->getType()->getPointerTo() == ptr->getType());
                    } else {

                        // debug checks
#ifndef NDEBUG
                        if(val.val->getType()->getPointerTo() != ptr->getType()) {
                            std::stringstream err;
                            err<<"attempting to store value of LLVM type "<<env->getLLVMTypeName(val.val->getType())<<" to slot expecting LLVM type "<<env->getLLVMTypeName(ptr->getType());
                            Logger::instance().logger("codegen").error(err.str());
                        }
#endif

                        assert(val.val->getType()->isPointerTy());
                        assert(val.val->getType()->getPointerTo() == ptr->getType());
                    }

                    builder.CreateStore(val.val, ptr, false);
                }

                if(val.size) {
                    assert(val.size->getType() == llvm::Type::getInt64Ty(builder.getContext()));
                    builder.CreateStore(val.size, sizePtr);
                }

                if(val.is_null) {
                    assert(val.is_null->getType() == llvm::Type::getInt1Ty(builder.getContext()));

                    // interesting here, allocate if necessary new block!
                    if(!nullPtr) {
                        // allocate new pointer at start of func
                        // special case: no instructions yet present?
                        auto& firstBlock = builder.GetInsertBlock()->getParent()->getEntryBlock();
                        llvm::IRBuilder<> ctorBuilder(&firstBlock);

                        // when first block is not empty, go to first instruction
                        if(!firstBlock.empty()) {
                            llvm::Instruction& inst = *firstBlock.getFirstInsertionPt();
                            ctorBuilder.SetInsertPoint(&inst);
                        }

                        auto llvmType = llvm::Type::getInt1Ty(builder.getContext());
                        nullPtr = ctorBuilder.CreateAlloca(llvmType, 0, nullptr, name + "_isnull");
                    }

                    assert(nullPtr);
                    builder.CreateStore(val.is_null, nullPtr);
                }
            }

            static bool passByValue(const python::Type& t) {
                assert(t != python::Type::UNKNOWN);

                // for option, decide based on underlying type
                if(t.isOptionType())
                    return passByValue(t.getReturnType());

                if(t.isIteratorType())
                    return false;

                // dictionary type right now mapped to i8* already, so mapping is mutable.
                return t.isImmutable() || t.isDictionaryType();
            }

        private:

            llvm::Type* deriveLLVMType() const {
                assert(env);

                // get rid off option!

                // only string, bool, int, f64 so far supported!
                auto t_without_option = type.isOptionType() ? type.getReturnType() : type;
                return env->pythonToLLVMType(t_without_option);
            }

            inline bool passByValue() const {
               return passByValue(type);
            }
        };


        // new: slots for variables
        struct VariableSlot {
            python::Type type;
            Variable var;
            llvm::Value* definedPtr;

            VariableSlot():type(python::Type::UNKNOWN), definedPtr(nullptr) {}

            void generateUnboundLocalCheck(LambdaFunctionBuilder& lfb, codegen::IRBuilder& builder) {
                assert(definedPtr);
                auto val = builder.CreateLoad(builder.getInt1Ty(), definedPtr);
                auto c_val = llvm::dyn_cast<llvm::ConstantInt>(val);
                if(c_val && c_val->getValue().getBoolValue()) {
                    // nothing todo, just remove the load instruction
                    val->eraseFromParent();
                } else {
                    // need to do dynamic check
                    auto i1_type = llvm::Type::getInt1Ty(val->getContext());
                    assert(val->getType() == i1_type);
                    // need to flip condition.
                    auto neg_val = builder.CreateSub(llvm::Constant::getIntegerValue(i1_type, llvm::APInt(1, true)), val);
                    lfb.addException(builder, ExceptionCode::UNBOUNDLOCALERROR, neg_val);
                }
            }

            bool isDefined(codegen::IRBuilder& builder) const {
                // unknown type?
                if(type == python::Type::UNKNOWN)
                    return false;

                // check whether variable has been defined yet.
                if(!definedPtr)
                    return false;

                auto val = builder.CreateLoad(builder.getInt1Ty(), definedPtr);
                auto c_val = llvm::dyn_cast<llvm::ConstantInt>(val);
                if(c_val) {
                    val->eraseFromParent();
                    return c_val->getValue().getBoolValue();
                }

                // remove load instruction, not necessary anymore.
                val->eraseFromParent();

                // else, can't define
                return true;
            }

        };

        struct VariableRealization {
            std::string name;
            python::Type type;
            codegen::SerializableValue val;
            codegen::SerializableValue original_ptr;
            llvm::Value* defined;
            llvm::Value* original_defined_ptr;

            static VariableRealization fromSlot(codegen::IRBuilder&builder, const std::string& name, const VariableSlot& slot) {
                VariableRealization r;
                r.name = name;
                r.type = slot.type;
                r.defined = builder.CreateLoad(builder.getInt1Ty(), slot.definedPtr);
                r.val = slot.var.load(builder);

                r.original_ptr = SerializableValue(slot.var.ptr, slot.var.sizePtr, slot.var.nullPtr);
                r.original_defined_ptr = slot.definedPtr;
                return std::move(r);
            }
        };

        inline std::unordered_map<std::string, VariableRealization> snapshotVariableValues(codegen::IRBuilder&builder) {
            std::unordered_map<std::string, VariableRealization> var_realizations;
            for(auto p : _variableSlots) {
                auto r = VariableRealization::fromSlot(builder, p.first, p.second);
                var_realizations[r.name] = r;
            }
            return var_realizations;
        }

        inline void restoreVariableSlots(codegen::IRBuilder& builder, const std::unordered_map<std::string, VariableRealization>& var_realizations, bool delete_others=false) {
            using namespace std;
            // when delete is specified, delete all slots which are not used anymore!
            // TODO: potentially add lifetime end!
            if(delete_others) {
                vector<string> remove_list;
                for(auto kv : _variableSlots)
                    if(var_realizations.find(kv.first) == var_realizations.end())
                        remove_list.push_back(kv.first);
                for(auto name : remove_list)
                    _variableSlots.erase(_variableSlots.find(name));
            }

            for(auto keyval : var_realizations) {
                // restore in slot!
                auto name = keyval.first;
                auto it = _variableSlots.find(name); assert(it != _variableSlots.end());

                // type different? alloc new variable!
                // => do not change definedPtr!
                if(keyval.second.type != _variableSlots[name].type) {
                    _variableSlots[name].type = keyval.second.type;
                    _variableSlots[name].var = Variable(*_env, builder, _variableSlots[name].type, name);
                }

                // load realization to var
                _variableSlots[name].var.store(builder, keyval.second.val);
            }
        }

        // current name table (symbol table) of variables.
        std::unordered_map<std::string, VariableSlot> _variableSlots;

        // simple assign, i.e. x = 2
        void assignToSingleVariable(NIdentifier* target, const python::Type& valueType);

        // assign from sequence type to multiple variables, i.e. tuple unpacking
        void assignToMultipleVariables(NTuple* lhs, ASTNode* rhs);

        inline VariableSlot* getSlot(const std::string& name) {
            auto jt = _variableSlots.find(name);
            if(jt != _variableSlots.end())
                return &jt->second;
            return nullptr;
        }

        std::unordered_map<std::string, std::tuple<python::Type, Variable>> _globals; //! global variables

        MessageHandler& _logger;

        LLVMEnvironment *_env;

        LambdaFunctionBuilder* _lfb;
        const codegen::CompilePolicy& _policy;

        // currently the codegen is restricted to single lambda functions (no nested lambdas!)
        // this variable is used to store that.
        int _numLambdaFunctionsEncountered;

        std::stack<std::string> _funcNames;
        std::map<std::string, python::Type> _nameTypes;

        std::unique_ptr<FunctionRegistry> _functionRegistry;

        // needs to be static in case multiple modules / functions are compiled
        static int _lambdaCounter;

        // lambda functions need to get a unique name in the LLVM IR module
        // following is a rule to define these names
        std::string lambdaName() {
            return std::string("lam") + std::to_string(_lambdaCounter);
        }

        std::string getNextLambdaName() {

            while (_nameTypes.find(lambdaName()) != _nameTypes.end())
                _lambdaCounter++;
            std::string name = lambdaName();
            _lambdaCounter++;
            return name;
        }

        llvm::Value *logErrorV(const std::string &message) {
            error(message);
            return nullptr;
        }

        // save all the blocks generated from the various visiting calls on this stack
        // visitors may pop values
        // in the end only one value should be left (i.e. the start block)
        std::deque<SerializableValue> _blockStack;

        // store current iteration ending block and loop ending block for for and while loops
        std::deque<llvm::BasicBlock*> _loopBlockStack;

        // store variable nodes in first iteration unrolled loop body.
        // update their types to stabilized types after first iteration
        // only references to existing NIdentifiers are stored here
        std::deque<std::set<NIdentifier *>> _loopBodyIdentifiersStack;

        std::shared_ptr<IteratorContextProxy> _iteratorContextProxy;

        void init() {

            if (!_blockStack.empty()) {
                // make sure to release all memory of the stack that has been allocated previously
                _blockStack = std::deque<SerializableValue>();
            }

            _funcNames = std::stack<std::string>();
            _numLambdaFunctionsEncountered = 0;
            _iteratorContextProxy = std::make_shared<IteratorContextProxy>(_env);
        }

        /*!
         * helper function to add an instruction to the stack (i.e. as return value of the visitor)
         * @param val value to be added
         * @param size size of the value in bytes (can be inferred for primitives)
         */
        inline void addInstruction(llvm::Value *val, llvm::Value *size = nullptr, llvm::Value *isnull = nullptr) {

            if(!val) {
                // add dummy to stack
                _blockStack.push_back(SerializableValue(val, size, isnull));
                return;
            }

            //assert(val);

            if (!size) {
                // only for double & int values this works
                auto type = val->getType();
                llvm::Value *inferred_size = nullptr;
                if (type->isIntegerTy()) {
                    // only 32 & 64 bit allowed + bool size
                    auto bitWidth = type->getIntegerBitWidth();
                    assert(bitWidth == _env->getBooleanType()->getIntegerBitWidth() || bitWidth == 32 ||
                           bitWidth == 64);

                    inferred_size = _env->i64Const(bitWidth / 8);

                    // special case: Boolean, use 8 bytes!
                    if (type == _env->getBooleanType())
                        inferred_size = _env->i64Const(sizeof(int64_t));

                } else if (type->isDoubleTy()) {
                    // 8 bytes
                    inferred_size = _env->i64Const(8);
                }

//                if (!inferred_size && !val->getType()->isFunctionTy()) {
//                    error("could not infer valid size from llvm Type");
//                    return;
//                }

                _blockStack.push_back(SerializableValue(val, inferred_size, isnull));

            } else {
                _blockStack.push_back(SerializableValue(val, size, isnull));
            }
        }

        // upcast return type
        SerializableValue upCastReturnType(const codegen::IRBuilder& builder, const SerializableValue& val, const python::Type& type, const python::Type& targetType);

        SerializableValue CreateDummyValue(const codegen::IRBuilder& builder, const python::Type& type);
        SerializableValue popWithNullCheck(const codegen::IRBuilder& builder, ExceptionCode ec, const std::string& message="");

        SerializableValue additionInst(const SerializableValue &L, NBinaryOp *op, const SerializableValue &R);

        llvm::Value *subtractionInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        SerializableValue logicalAndInst(NBinaryOp *op);
        SerializableValue logicalOrInst(NBinaryOp *op);

        SerializableValue multiplicationInst(const SerializableValue& L, NBinaryOp *op, const SerializableValue& R);

        llvm::Value* divisionInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        llvm::Value* integerDivisionInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        llvm::Value* moduloInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        llvm::Value* powerInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        llvm::Value* oneSidedNullComparison(const codegen::IRBuilder& builder, const python::Type& type, const TokenType& tt, llvm::Value* isnull);

        llvm::Value *compareInst(const codegen::IRBuilder& builder,
                                llvm::Value *L,
                                 llvm::Value *L_isnull,
                                 const python::Type &leftType,
                                 const TokenType &tt,
                                 llvm::Value *R,
                                 llvm::Value *R_isnull,
                                 const python::Type &rightType);

        llvm::Value *compareInst(const codegen::IRBuilder& builder,
                                 llvm::Value *L,
                                 const python::Type &leftType,
                                 const TokenType &tt,
                                 llvm::Value *R,
                                 const python::Type &rightType);

        llvm::Value* listInclusionCheck(const codegen::IRBuilder& builder, llvm::Value *L, const python::Type &leftType,
                                llvm::Value *R, const python::Type &rightType);

        llvm::Value *numericCompareInst(const codegen::IRBuilder& builder, llvm::Value *L,
                                 const python::Type &leftType,
                                 const TokenType &tt,
                                 llvm::Value *R,
                                 const python::Type &rightType);

        llvm::Value *stringCompareInst(const codegen::IRBuilder& builder, llvm::Value *L,
                                       const python::Type &leftType,
                                       const TokenType &tt,
                                       llvm::Value *R,
                                       const python::Type &rightType);

        llvm::Value *leftShiftInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        llvm::Value *rightShiftInst(llvm::Value *L, NBinaryOp *op, llvm::Value *R);

        SerializableValue stringSliceInst(const SerializableValue& value, llvm::Value *start, llvm::Value *end, llvm::Value *stride);

        llvm::Value *processSliceIndex(const codegen::IRBuilder& builder, llvm::Value *index, llvm::Value *len, llvm::Value *stride);

        SerializableValue tupleStaticSliceInst(ASTNode *tuple_node, ASTNode *start_node, ASTNode *end_node,
                ASTNode *stride_node, const SerializableValue& tuple, llvm::Value *start, llvm::Value *end,
                llvm::Value *stride);

        SerializableValue indexTupleWithStaticExpression(ASTNode *index_node, ASTNode *value_node, SerializableValue index, SerializableValue value);

        SerializableValue formatStr(const SerializableValue& fmtString, NBinaryOp *op, const SerializableValue& arg);

        /*!
         * brings val to the type (i.e. internal i8 is upcast to i64, i64 to double)
         * @param val value to be upcast
         * @param type desired type
         * @return
         */
        llvm::Value *upCast(const codegen::IRBuilder &builder, llvm::Value *val, llvm::Type *type);

        llvm::Value *i32Const(const int32_t val) {
            return llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(_env->getContext()), llvm::APInt(32, val));
        }

        /*!
         * build a cJSON representation of a dictionary
         * @param keys
         * @param vals
         * @return
         */
        SerializableValue createCJSONFromDict(NDictionary *dict, const std::vector<SerializableValue> &keys, const std::vector<SerializableValue> &vals);

        /*!
         * create a subscription of a cJSON dictionary
         * @param sub
         * @param index
         * @param value
         * @return
         */
        SerializableValue subscriptCJSONDictionary(NSubscription *sub, SerializableValue index, const python::Type& index_type, SerializableValue value);

        // helper function to generate if/else statements
        void generateIfElseExpression(NIfElse* ifelse, bool short_circuit = false);

        /*!
         * speculative version of if-else expression, i.e. only one side is visited...
         * @param ifelse
         * @param visit_if true when only if branch should be visited, else false.
         * @param short_circuit
         */
        void generatePartialIfElseExpression(NIfElse* ifelse, bool visit_if, bool short_circuit = false);

        void generateIfElseStatement(NIfElse* ifelse, bool exceptOnThen=false, bool exceptOnElse=false);

        void generateIfElse(NIfElse* ifelse, bool exceptOnThen=false, bool exceptOnElse=false);

        // deal with the case when for loop expression is a tuple
        void visitUnrolledLoopSuite(NSuite*);

        /*!
         * helper function for visit(NFor *forStmt)
         * assign value at curr of exprAlloc to all loop variables in loopVal, then emit loop body
         * @param forStmt
         * @param targetType
         * @param exprType expression type
         * @param loopVal a vector of loop variables
         * @param exprAlloc SerializableValue of expression
         * @param curr current index of expression, except in range this represents the actual value
         */
        void assignForLoopVariablesAndGenerateLoopBody(NFor *forStmt, const python::Type &targetType,
                                                       const python::Type &exprType,
                                                       const std::vector<std::pair<NIdentifier *, python::Type>> &loopVal,
                                                       const SerializableValue &exprAlloc,
                                                       llvm::Value *curr);

        inline bool earlyExit() const {
            // expression early exit check
            if(_lfb && _lfb->hasExited())
                return true;
            if(failed())
                return true;
            return false;
        }

    protected:

        // override such it throws
        void error(const std::string& message, const std::string& logger="") override;
    public:

        BlockGeneratorVisitor() = delete;

        BlockGeneratorVisitor(LLVMEnvironment *env,
                              const std::map<std::string, python::Type> &nameTypes,
                              const codegen::CompilePolicy& policy) : IFailable(true), _nameTypes(nameTypes),
                                                              _policy(policy),
                                                              _functionRegistry(new FunctionRegistry(*env, _policy.sharedObjectPropagation)),
                                                              _logger(Logger::instance().logger("codegen")) {
            assert(env);
            _env = env;
            _lfb = nullptr;

            init();
        }

        ~BlockGeneratorVisitor() {
            if(_lfb)
                delete _lfb;
            _lfb = nullptr;
        }

        void addGlobals(ASTNode* root, const ClosureEnvironment& ce);

        std::string getIR() {
            std::string ir = "";
            llvm::raw_string_ostream os(ir);
            _env->getModule()->print(os, nullptr);
            return ir;
        }

        // ----
        // expressions: need to perform hasExited check always...
        void visit(NNone*) override;
        void visit(NBoolean *) override;
        void visit(NNumber *) override;
        void visit(NUnaryOp *) override;
        void visit(NBinaryOp *) override;
        void visit(NLambda *lambda) override;
        void visit(NIdentifier *) override;
        void visit(NTuple *) override;
        void visit(NDictionary *) override;
        void visit(NList *) override;
        void visit(NCompare *) override;
        void visit(NString *) override;
        void visit(NSubscription *) override;
        void visit(NCall*) override;
        void visit(NSlice*) override;
        void visit(NAttribute*) override;
        // list comprehension stuff
        void visit(NRange*) override;
        void visit(NComprehension*) override;
        void visit(NListComprehension*) override;


        // ----------
        // statements
        void visit(NSuite*) override;
        void visit(NFunction*) override;

        // function related stuff
        void visit(NIfElse*) override; // can be both expression OR statement.
        void visit(NFor*) override;
        void visit(NWhile*) override;
        void visit(NContinue*) override;
        void visit(NBreak*) override;
        void visit(NAssign*) override;
        void visit(NReturn*) override;

        void visit(NAssert*) override;
        void visit(NRaise*) override;

        llvm::Module *getLLVMModule() const { return _env->getModule().get(); }

        std::string getLastFuncName() {
            assert(_funcNames.size() > 0);
            return _funcNames.top();
        }

        llvm::Value *binaryInst(llvm::Value *R, NBinaryOp *op, llvm::Value *L);

        void updateSlotsBasedOnRealizations(const codegen::IRBuilder& builder,
                                            const std::unordered_map<std::string, VariableRealization>& var_realizations,
                                            const std::string &branch_name,
                                            bool allowNumericUpcasting);

        void updateSlotsWithSharedTypes(const codegen::IRBuilder& builder,
                                        const std::unordered_map<std::string, VariableRealization> &if_var_realizations,
                                        const std::unordered_map<std::string, VariableRealization> &else_var_realizations);

        llvm::Value *generateConstantIntegerPower(const codegen::IRBuilder& builder,
                                                  llvm::Value *base, int64_t exponent);

        /*!
         * should get called when targetType is iteratorType
         * use targetType and iteratorInfo annotation to get concrete LLVM type for iterator variable
         * allocate iterator struct and update slot ptr if the current slot ptr type is different from the concrete LLVM type
         * @param builder
         * @param slot
         * @param val
         * @param targetType
         * @param iteratorInfo
         */
        void updateIteratorVariableSlot(const codegen::IRBuilder &builder,
                                        VariableSlot *slot,
                                        const SerializableValue &val,
                                        const python::Type &targetType,
                                        const std::shared_ptr<IteratorInfo> &iteratorInfo);
    };
}
}

#endif //TUPLEX_BLOCKGENERATORVISITOR_H