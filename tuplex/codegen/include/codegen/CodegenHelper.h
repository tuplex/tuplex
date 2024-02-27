//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CODEGENHELPER_H
#define TUPLEX_CODEGENHELPER_H

#include <llvm/IR/Value.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/raw_ostream.h>
#include <string>
#include <TypeSystem.h>
#include <Field.h>

#include <parameters.h>

//#if LLVM_VERSION_MAJOR == 9
// LLVM9 fix
#include <llvm/Target/TargetMachine.h>
//#endif

// Note: would be great to use something like this to find bugs within LLVM IR generation
// https://reviews.llvm.org/D40778#change-y6HwnUloCr6I


// builder and codegen funcs
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Type.h>
#include <unordered_map>

#include <llvm/IR/CFG.h>

#include <Base.h>

#include <nlohmann/json.hpp>
#include <cstddef>

namespace tuplex {
    namespace codegen {

        // various switches to influence compiler behavior
        struct CompilePolicy {
            bool allowUndefinedBehavior;
            bool allowNumericTypeUnification; // whether bool/i64 get autoupcasted and merged when type conflicts exist within if-branches.
            bool sharedObjectPropagation;
            double normalCaseThreshold;

            CompilePolicy() : allowUndefinedBehavior(false),
            allowNumericTypeUnification(false),
            sharedObjectPropagation(false),
            normalCaseThreshold(0.9) {}

            bool operator == (const CompilePolicy& other) const {
                if(allowUndefinedBehavior != other.allowUndefinedBehavior)
                    return false;
                if(allowNumericTypeUnification != other.allowNumericTypeUnification)
                    return false;
                if(sharedObjectPropagation != other.sharedObjectPropagation)
                    return false;
                if(std::abs(normalCaseThreshold - other.normalCaseThreshold) > 0.001)
                    return false;
                return true;
            }

#ifdef BUILD_WITH_CEREAL
            // cereal serialization functions
            template<class Archive> void serialize(Archive &ar) {
                ar(allowUndefinedBehavior, allowNumericTypeUnification, sharedObjectPropagation, normalCaseThreshold);
            }
#endif
        };

        static CompilePolicy DEFAULT_COMPILE_POLICY;

        // when using specialization, additional checks/classifications need to be performed on each column
        // following is a helper struct to describe such a check
        enum class CheckType {
            CHECK_UNKNOWN=0,
            CHECK_NULL=1,
            CHECK_NOTNULL,
            CHECK_DELAYEDPARSING,
            CHECK_INTEGER_RANGE,
            CHECK_CONSTANT,
            CHECK_FILTER // promoted filter, special case.
        };

        struct NormalCaseCheck {

            std::vector<size_t> colNos; ///! multiple columns
            CheckType type;

            ///! the column number to check for
            inline size_t colNo() const {
                assert(isSingleColCheck());
                return colNos.front();
            }

            inline bool isSingleColCheck() const {
                return colNos.size() == 1;
            }

            NormalCaseCheck() : type(CheckType::CHECK_UNKNOWN),
            _constantType(python::Type::UNKNOWN),
            _iMin(std::numeric_limits<int64_t>::min()),
            _iMax(std::numeric_limits<int64_t>::max()) {}

            NormalCaseCheck(const NormalCaseCheck& other) : colNos(other.colNos), type(other.type),
            _constantType(other._constantType), _iMin(other._iMin), _iMax(other._iMax), _serializedCheck(other._serializedCheck) {}

            NormalCaseCheck& operator = (const NormalCaseCheck& other) {
                colNos = other.colNos;
                type = other.type;
                // private members
                _constantType = other._constantType;
                _iMin = other._iMin;
                _iMax = other._iMax;
                _serializedCheck = other._serializedCheck;
                return *this;
            }

            static NormalCaseCheck NullCheck(size_t colNo) {
                NormalCaseCheck c;
                c.colNos.push_back(colNo);
                c.type = CheckType::CHECK_NULL;
                return c;
            }

            static NormalCaseCheck NotNullCheck(size_t colNo) {
                NormalCaseCheck c;
                c.colNos.push_back(colNo);
                c.type = CheckType::CHECK_NOTNULL;
                return c;
            }

            static NormalCaseCheck FilterCheck(const std::vector<size_t>& accessed_cols, const std::string& serialized_filter) {
                NormalCaseCheck c;
                c.colNos = accessed_cols;
                c.type = CheckType::CHECK_FILTER;
                c._serializedCheck = serialized_filter;
                return c;
            }

            static NormalCaseCheck ConstantCheck(size_t colNo, const python::Type& constType) {
                assert(constType.isConstantValued());

                NormalCaseCheck c;
                c.colNos.push_back(colNo);
                c.type = CheckType::CHECK_CONSTANT;
                c._constantType = constType; // required because of different formattings of types...
                return c;
            }

            /*!
             * generates a condition yielding true if check was passed, false else
             * @param builder
             * @param cell_value the cell as str
             * @param cell_size the cell size in bytes
             * @return true if check was passed (i.e. cell conforms to check!), false otherwise
             */
            llvm::Value* codegenForCell(llvm::IRBuilder<>& builder, llvm::Value* cell_value, llvm::Value* cell_size);

            inline python::Type constant_type() const {
                assert(type == CheckType::CHECK_CONSTANT);
                return _constantType;
            }

#ifdef BUILD_WITH_CEREAL
            template<class Archive> void serialize(Archive & ar) {
                    ar(colNos, type, _constantType, _iMin, _iMax, _serializedCheck);
            }
#endif

        // JSON serialization
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["colNos"] = colNos;
            j["type"] = static_cast<int>(type);
            j["constantType"] = _constantType.desc();
            j["iMin"] = _iMin;
            j["iMax"] = _iMax;
            j["check"] = _serializedCheck;
            return j;
        }

        static NormalCaseCheck from_json(nlohmann::json j) {
            NormalCaseCheck c;
            c.colNos = j["colNo"].get<std::vector<size_t>>();
            c.type = static_cast<CheckType>(j["colNo"].get<int>());
            c._constantType = python::decodeType(j["constantType"].get<std::string>());
            c._iMin = j["iMin"].get<int64_t>();
            c._iMax = j["iMax"].get<int64_t>();
            c._serializedCheck = j["check"].get<std::string>();
            return c;
        }

        std::string data() const { return _serializedCheck; }

        private:
            python::Type _constantType;
            int64_t _iMin;
            int64_t _iMax;

            std::string _serializedCheck;
        };

        // helper function to determine number of predecessors
        inline size_t successorCount(llvm::BasicBlock* block) {
            assert(block);

            size_t count = 0;
            for(llvm::BasicBlock* pred : llvm::successors(block))
                count++;
            return count;
        }

        inline bool hasSuccessor(llvm::BasicBlock* block) {
            return successorCount(block) != 0;
        }

        // helper function to determine number of predecessors
        inline size_t predecessorCount(llvm::BasicBlock* block) {
            assert(block);

            size_t count = 0;
            for(llvm::BasicBlock* pred : llvm::predecessors(block))
                count++;
            return count;
        }

        inline bool hasPredecessor(llvm::BasicBlock* block) {
            return predecessorCount(block) != 0;
        }

        /*!
         * print the ir code of a single LLVM function.
         * @param func pointer to LLVM function
         * @param withLineNumbers include line numbers in print or not
         * @return formatted string holding llvm ir code
         */
        inline std::string printFunction(llvm::Function* func, bool withLineNumbers=false) {
            std::string ir_string;
            llvm::raw_string_ostream os{ir_string};
            assert(func);
            func->print(os, nullptr, false);
            os.flush();
            if(withLineNumbers)
                return core::withLineNumbers(ir_string);
            else
                return ir_string;
        }

        /*!
         * get a builder for the first block in a function. The first block may be linked already.
         * @param builder
         * @return
         */
        inline llvm::IRBuilder<> getFirstBlockBuilder(llvm::IRBuilder<>& builder) {
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());

            // function shouldn't be empty when this function here is called!
            assert(!builder.GetInsertBlock()->getParent()->empty());

            // special case: no instructions yet present?
            auto& firstBlock = builder.GetInsertBlock()->getParent()->getEntryBlock();
            llvm::IRBuilder<> ctorBuilder(&firstBlock);

            // when first block is not empty, go to first instruction
            if(!firstBlock.empty()) {
                llvm::Instruction& inst = *firstBlock.getFirstInsertionPt();
                ctorBuilder.SetInsertPoint(&inst);
            }
            return ctorBuilder;
        }

        // in order to serialize/deserialize data properly and deal with
        // varlen variables too, the size needs to be known
        // this is a helper structure to make sure no sizing information is lost
        // during the process
        struct SerializableValue {
            llvm::Value *val;
            llvm::Value *size;
            llvm::Value *is_null; // should be i1, optional

            SerializableValue() : val(nullptr), size(nullptr), is_null(nullptr)   {}
            SerializableValue(llvm::Value *v, llvm::Value* s) : val(v), size(s), is_null(nullptr) {
#ifndef NDEBUG
                if(s) {
                   auto stype = s->getType();
                   if(stype->isPointerTy())
                       stype = stype->getPointerElementType();
                    assert(stype == llvm::Type::getInt64Ty(s->getContext()));
                }
#endif
            }
            SerializableValue(llvm::Value *v, llvm::Value* s, llvm::Value* n) : val(v), size(s), is_null(n) {
#ifndef NDEBUG
                if(s) {
                    auto stype = s->getType();
                    if(stype->isPointerTy())
                        stype = stype->getPointerElementType();
                    assert(stype == llvm::Type::getInt64Ty(s->getContext()));
                }
#endif
            }

            SerializableValue(const SerializableValue& other) : val(other.val), size(other.size), is_null(other.is_null) {}
            SerializableValue(SerializableValue&& other) : val(other.val), size(other.size), is_null(other.is_null) {}

            SerializableValue& operator = (const SerializableValue& other) {
                val = other.val;
                size = other.size;
                is_null = other.is_null;

                return *this;
            }

            /*!
             * gives the value back representing None/NULL
             * @param builder
             * @return None
             */
            static SerializableValue None(llvm::IRBuilder<>& builder) {
                auto is_null = llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(builder.getContext()), llvm::APInt(1, true));
                return SerializableValue(nullptr, nullptr, is_null);
            }
        };


        // for variable length fields => offset and size packing!
        inline llvm::Value* pack_offset_and_size(llvm::IRBuilder<>& builder, llvm::Value* offset, llvm::Value* size) {
            auto& ctx = builder.GetInsertBlock()->getContext();

            // truncate or ext both offset and size to 32bit
            offset = builder.CreateZExtOrTrunc(offset, llvm::Type::getInt32Ty(ctx));
            size = builder.CreateZExtOrTrunc(size, llvm::Type::getInt32Ty(ctx));

            llvm::Value *info = builder.CreateOr(builder.CreateZExt(offset, llvm::Type::getInt64Ty(ctx)), builder.CreateShl(builder.CreateZExt(size, llvm::Type::getInt64Ty(ctx)), 32));
            return info;
        }

        inline std::tuple<llvm::Value*, llvm::Value*> unpack_offset_and_size(llvm::IRBuilder<>& builder, llvm::Value* info) {
            using namespace llvm;

            // truncation yields lower 32 bit (= offset)
            Value *offset = builder.CreateTrunc(info, Type::getInt32Ty(builder.getContext()));
            // right shift by 32 yields size
            Value *size = builder.CreateLShr(info, 32, "varsize");

            // extend to 64bit
            offset = builder.CreateZExtOrTrunc(offset, Type::getInt64Ty(builder.getContext()));
            size = builder.CreateZExtOrTrunc(size, Type::getInt64Ty(builder.getContext()));
            return std::make_tuple(offset, size);
        }

        /*!
         * generates code to get a compatible underlying value from an optimized value.
         * @param builder LLVM IR Builder
         * @param value codegen value representing the optimized value
         * @param optType type the codegen value has
         * @param underlyingType pointer, if not null will output the deoptmizedType to that var. Same as if deoptimizedType was called on optType.
         * @return codegen value representing deoptimized value, i.e. having type underlyingType.
         */
        extern SerializableValue deoptimizeValue(llvm::IRBuilder<>& builder,
                                                 const SerializableValue& value,
                                                 const python::Type& optType,
                                                 python::Type* underlyingType=nullptr);

        /*!
         * retrieves IR stored in LLVM module as string
         * @param mod llvm Module
         * @return string
         */
        inline std::string moduleToString(const llvm::Module& module) {
            std::string ir = "";
            llvm::raw_string_ostream os(ir);
            module.print(os, nullptr);
            os.flush();
            return ir;
        }

        /*!
         * converts llvm IR string to module
         * @param llvmIR
         * @return LLVM module
         */
        extern std::unique_ptr<llvm::Module> stringToModule(llvm::LLVMContext& context, const std::string& llvmIR);

        extern uint8_t* moduleToBitCode(const llvm::Module& module, size_t* bufSize);
        extern std::string moduleToBitCodeString(const llvm::Module& module);
        extern std::unique_ptr<llvm::Module> bitCodeToModule(llvm::LLVMContext& context, void* buf, size_t bufSize);
        inline std::unique_ptr<llvm::Module> bitCodeToModule(llvm::LLVMContext& context, const std::string& bc) {
            return bitCodeToModule(context, (void*)bc.c_str(), bc.size());
        }


        /*!x
         * compute code stats over LLVM IR code
         * @param llvmIR
         * @param include_detailed_counts
         * @return formatted string from InstructionCount Pass
         */
        extern std::string moduleStats(const std::string& llvmIR, bool include_detailed_counts=false);

        /*!
         * retrieves assembly for the Tuplex target machine as string (with comments)
         * @param module Module to lower to Assembly
         * @return string with x86 assembly code
         */
        extern std::string moduleToAssembly(std::shared_ptr<llvm::Module> module);

        /*!
         * get Tuplex specific target machine (i.e. with sse4.2 features or so)
         * @return
         */
        extern llvm::TargetMachine* getOrCreateTargetMachine();

        /*!
         * get features of CPU as llvm feature string
         */
        extern std::string getLLVMFeatureStr();

        /*!
         * helper function to initialize LLVM targets for this platform
         */
        extern void initLLVM();

        /*!
         * shutdown llvm
         */
        extern void shutdownLLVM();

        /*
         * cast val to destType (i.e. integer expansion or int to float conversion)
         * @param builder
         * @param val
         * @param destType
         * @return casted llvm Value
         */
        extern llvm::Value* upCast(llvm::IRBuilder<> &builder, llvm::Value *val, llvm::Type *destType);

        extern llvm::Value *
        dictionaryKey(llvm::LLVMContext &ctx, llvm::Module *mod, llvm::IRBuilder<> &builder, llvm::Value *val,
                      python::Type keyType, python::Type valType);

        extern SerializableValue
        dictionaryKeyCast(llvm::LLVMContext &ctx, llvm::Module* mod,
                          llvm::IRBuilder<> &builder, llvm::Value *val, python::Type keyType);

        /*!
         * for debug purposes convert llvm type to string
         * @param type llvm type, if nullptr "null" is returned
         * @return string describing the type. In debug mode, extended description.
         */
        inline std::string llvmTypeToStr(llvm::Type* type) {
            if(!type)
                return "null";
            std::string s = "";
            llvm::raw_string_ostream os(s);
#ifndef NDEBUG
            type->print(os, true);
#else
            type->print(os);
#endif
            os.flush();
            return s;
        }

        /*!
         * verifies module and then each function itself
         * @param mod
         * @param out
         * @return whether module is ok or not.
         */
        extern bool verifyModule(llvm::Module& mod, std::string* out=nullptr);

        /*!
         * verifies function and optionally yields error message
         * @param func
         * @param out
         * @return if function is ok to compile true else false
         */
        extern bool verifyFunction(llvm::Function* func, std::string* out=nullptr);


        /*!
         * counts how many successor blocks a basic block has
         * @param block
         * @return number of sucessor blocks, 0 when nullptr
         */
        extern size_t successorBlockCount(llvm::BasicBlock* block);

        static inline llvm::Function* exception_handler_prototype(llvm::LLVMContext& ctx, llvm::Module* mod, const std::string& name) {
            using namespace llvm;
            std::vector<Type*> eh_argtypes{Type::getInt8PtrTy(ctx, 0),
                                           Type::getInt64Ty(ctx),
                                           Type::getInt64Ty(ctx),
                                           Type::getInt64Ty(ctx),
                                           Type::getInt8PtrTy(ctx, 0),
                                           Type::getInt64Ty(ctx),};
            FunctionType *eh_type = FunctionType::get(Type::getVoidTy(ctx), eh_argtypes, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction(name, eh_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction(name, eh_type).getCallee());
#endif
            return func;
        }

        template<typename T> inline llvm::Type* ctypeToLLVM(llvm::LLVMContext& ctx) {
#ifndef NDEBUG
            throw std::runtime_error(std::string("unknown type ") + typeid(T).name() + " encountered");
#endif
            return nullptr;
        }

        template<> inline llvm::Type* ctypeToLLVM<int>(llvm::LLVMContext& ctx) {
            switch(sizeof(int)) {
                case 4:
                    return llvm::Type::getInt32Ty(ctx);
                case 8:
                    return llvm::Type::getInt16Ty(ctx);
                default:
                    throw std::runtime_error("unknown integer with");
            }
        }

        template<> inline llvm::Type* ctypeToLLVM<bool>(llvm::LLVMContext& ctx) {
            switch(sizeof(bool)) {
                case 1:
                    return llvm::Type::getInt8Ty(ctx);
                case 4:
                    return llvm::Type::getInt32Ty(ctx);
                case 8:
                    return llvm::Type::getInt16Ty(ctx);
                default:
                    throw std::runtime_error("unknown boolean with");
            }
        }

        template<> inline llvm::Type* ctypeToLLVM<char>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(char) == 1, "char must be 1 byte");
            return llvm::Type::getInt8Ty(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<int8_t>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(int8_t) == 1, "int8_t must be 1 byte");
            return llvm::Type::getInt8Ty(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<int64_t>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(int64_t) == 8, "int64_t must be 8 bytes");
            return llvm::Type::getInt64Ty(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<char*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(char*) == 8, "char* must be 8 byte");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<int64_t*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(int64_t) == 8, "int64_t must be 64bit");
            return llvm::Type::getInt64Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<void*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(void*) == 8, "void* must be 64bit");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<uint8_t*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(uint8_t*) == 8, "uint8_t* must be 64bit");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<double>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(double) == 8, "double should be 64bit");
            return llvm::Type::getDoubleTy(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<uint8_t**>(llvm::LLVMContext& ctx) {
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0)->getPointerTo();
        }

        /*!
         * returns the underlying string of a global variable, created e.g. via env->strConst.
         * May throw exception if value is not a constantexpr
         * @param value
         * @return string or empty string if extraction failed.
         */
        extern std::string globalVariableToString(llvm::Value* value);


        /*!
         * compare string stored in ptr to constant str
         * @param builder
         * @param ptr
         * @param str
         * @param include_zero
         * @return i1 true if strings match, else i1 false
         */
        extern llvm::Value* stringCompare(llvm::IRBuilder<> &builder, llvm::Value *ptr, const std::string &str,
                                                   bool include_zero);

        /*!
         * renames function args and returns them as hashmap for easy access. Order of names in vector corresponds to order of args
         */
        inline std::unordered_map<std::string, llvm::Value*> mapLLVMFunctionArgs(llvm::Function* func, const std::vector<std::string>& names) {
            std::unordered_map<std::string, llvm::Value*> m;

            std::vector<llvm::Argument *> args;
            int counter = 0;
            for (auto &arg : func->args()) {
                if(counter >= names.size()) {
                    throw std::runtime_error("too few names given");
                }
                m[names[counter]] = &arg;
                counter++;
            }

            return m;
        }

        inline int hashtableKeyWidth(const python::Type &t) {
            if (t.withoutOption() == python::Type::I64 ||
                (t.isTupleType() &&
                 t.parameters().size() == 1 &&
                        t.parameters()[0].withoutOption() == python::Type::I64)) {
                static_assert(sizeof(int64_t) == 8, "int64_t must be 8 bytes");
                return 8; // single int is hashed in an int hashtable
            }
            // constant or tuple of constants?
            if(t.isConstantValued() || python::isTupleOfConstants(t))
                return 0;

            return 0xFFFFFFFF; // strings are strings and anything besides int is just serialized to string right now
        }


        inline std::vector<std::string> extractFunctionNames(llvm::Module* mod) {
            using namespace std;
            if(!mod)
                return {};
            vector<string> v;
            for(const auto& func : mod->functions()) {
                v.push_back(func.getName().str());
            }
            return v;
        }

        /*!
         * helper function to annotate module such that each IR instruction is printed when executed. Helpful for debugging
         * @param mod
         */
        extern void annotateModuleWithInstructionPrint(llvm::Module& mod);

        /*!
         * check whether row type of normal-case can be upcast to general case type
         * @param normal_case_type
         * @param general_case_type
         * @param mapping optional mapping
         * @return true/false
         */
        inline bool checkCaseCompatibility(const python::Type& normal_case_type, const python::Type& general_case_type,
                                           const std::map<int, int>& mapping) {
            auto& logger = Logger::instance().logger("codegen");


            // special case: both are row type
            if(PARAM_USE_ROW_TYPE && normal_case_type.isRowType() && general_case_type.isRowType()) {
                // check that each pair can be upcast
                std::vector<std::pair<std::string, python::Type>> normal_case_pairs;
                std::unordered_map<std::string, python::Type> general_case_map;
                auto normal_case_columns = normal_case_type.get_column_names();
                auto normal_case_column_types = normal_case_type.get_column_types();
                for(unsigned i = 0; i < normal_case_columns.size(); ++i)
                    normal_case_pairs.push_back(std::make_pair(normal_case_columns[i], normal_case_column_types[i]));

                auto general_case_columns = general_case_type.get_column_names();
                auto general_case_column_types = general_case_type.get_column_types();
                for(unsigned i = 0; i < general_case_columns.size(); ++i)
                    general_case_map[general_case_columns[i]] = general_case_column_types[i];

                // now check each pair
                for(const auto& p : normal_case_pairs) {
                    auto it = general_case_map.find(p.first);
                    // missing key/column
                    if(general_case_map.end() == it)
                        return false;

                    // check if type can be upcast
                    if(!python::canUpcastType(p.second, it->second))
                        return false;
                }
                return true;
            }

            if(!normal_case_type.isTupleType() && !normal_case_type.isRowType())
                throw std::runtime_error("normal case type " + normal_case_type.desc() + " is not a row type.");
            if(!general_case_type.isTupleType() && !general_case_type.isRowType())
                throw std::runtime_error("general case type " + general_case_type.desc() + " is not a row type.");

            auto num_normal_columns = extract_columns_from_type(normal_case_type);
            auto num_general_columns = extract_columns_from_type(general_case_type);

            if(mapping.empty()) {
                // no mapping, column count must match and upcast be possible!
                if(num_normal_columns != num_general_columns) {
                    logger.debug("mapping is empty but number of columns normal(" +
                    std::to_string(num_general_columns) + ")/general(" + std::to_string(num_general_columns)
                    + ") not matching.");
                    return false;
                }

                auto tmp_normal_case_type = normal_case_type;
                auto tmp_general_case_type = general_case_type;
                if(normal_case_type.isRowType())
                    tmp_normal_case_type = normal_case_type.get_columns_as_tuple_type();
                if(general_case_type.isRowType())
                    tmp_general_case_type = general_case_type.get_columns_as_tuple_type();

                return python::canUpcastToRowType(tmp_normal_case_type, tmp_general_case_type);
            } else {
                // check that for each column in normal case a mapping to general case exists and is valid!
                for(unsigned i = 0; i < num_normal_columns; ++i) {
                    // check that entry exists in mapping!
                    auto it = mapping.find(i);
                    if(it == mapping.end()) {
                        logger.debug("no mapping entry found for index " + std::to_string(i));
                        return false;
                    }
                    if(it->second < 0 || it->second >= num_general_columns) {
                        logger.debug("invalid index mapping " + std::to_string(i) + " -> " + std::to_string(it->second));
                    }

                    // mapping found, check that upcating is possible
                    auto nt = normal_case_type.isRowType() ? normal_case_type.get_column_type(i) : normal_case_type.parameters()[i];
                    auto gt = general_case_type.isRowType() ? general_case_type.get_column_type(mapping.at(i)) : general_case_type.parameters()[mapping.at(i)];
                    if(!python::canUpcastType(nt, gt)) {
                        logger.debug("can not upcast " + nt.desc() + " -> " + gt.desc());
                        return false;
                    }
                }
            }
            return true;
        }

        inline bool blockContainsRet(llvm::BasicBlock *bb) {
            assert(bb);
            if(bb->empty())
                return false;
            return llvm::isa<llvm::ReturnInst>(bb->back());
        }

        // for both condbr or br
        inline bool blockContainsBr(llvm::BasicBlock *bb) {
            assert(bb);
            if(bb->empty())
                return false;
            return llvm::isa<llvm::BranchInst>(bb->back());
        }

        // block is open when there is no ret nor br instruction
        inline bool blockOpen(llvm::BasicBlock *bb) {
            if (!bb)
                return false;
            if(bb->empty())
                return true;
            return !blockContainsRet(bb) && !blockContainsBr(bb);
        }

        /*!
         * create an object file from an existing module
         * @param mod
         * @return buffer of bytes holding result (writing to disk will create a .o file).
         */
        extern std::vector<uint8_t> compileToObjectFile(llvm::Module& mod,
                                                        const std::string& target_triple=llvm::sys::getDefaultTargetTriple(),
                                                        const std::string& cpu="generic");

        extern nlohmann::json compileEnvironmentAsJson();

        /*!
         * return information about compiling for a target machine as JSON.
         */
        extern std::string compileEnvironmentAsJsonString();

        // helper to enable llvm6 and llvm9 compatibility // --> force onto llvm9+ for now.
        inline llvm::CallInst *createCallHelper(llvm::Function *Callee, llvm::ArrayRef<llvm::Value*> Ops,
                                          llvm::IRBuilder<>& builder,
                                          const llvm::Twine &Name = "",
                                                llvm::Instruction *FMFSource = nullptr) {
            llvm::CallInst *CI = llvm::CallInst::Create(Callee, Ops, Name);
            if (FMFSource)
                CI->copyFastMathFlags(FMFSource);
#if (LLVM_VERSION_MAJOR <= 15)
            builder.GetInsertBlock()->getInstList().insert(builder.GetInsertPoint(), CI);
#else
            CI->insertInto(builder.GetInsertBlock(), builder.GetInsertBlock()->begin());
#endif
            builder.SetInstDebugLocation(CI);
            return CI;
        }

        inline llvm::Value* getOrInsertCallable(llvm::Module& mod, const std::string& name, llvm::FunctionType* FT) {
#if LLVM_VERSION_MAJOR < 9
            return mod.getOrInsertFunction(name, FT);
#else
            return mod.getOrInsertFunction(name, FT).getCallee();
#endif
        }

        inline llvm::Value* getOrInsertCallable(llvm::Module* mod, const std::string& name, llvm::FunctionType* FT) {
            assert(mod);
            if(!mod)
                return nullptr;
            return getOrInsertCallable(*mod, name, FT);
        }


        inline llvm::Function* getOrInsertFunction(llvm::Module& mod, const std::string& name, llvm::FunctionType* FT) {
#if LLVM_VERSION_MAJOR < 9
            llvm::Function* func = cast<Function>(mod.getOrInsertFunction(name, FT));
#else
            llvm::Function *func = llvm::cast<llvm::Function>(mod.getOrInsertFunction(name, FT).getCallee());
#endif
            return func;
        }

        inline llvm::Function* getOrInsertFunction(llvm::Module* mod, const std::string& name, llvm::FunctionType* FT) {
            if(!mod)
                return nullptr;

#if LLVM_VERSION_MAJOR < 9
            llvm::Function* func = cast<Function>(mod->getOrInsertFunction(name, FT));
#else
            llvm::Function *func = llvm::cast<llvm::Function>(mod->getOrInsertFunction(name, FT).getCallee());
#endif
            return func;
        }

        template <typename... ArgsTy>
        llvm::Function* getOrInsertFunction(llvm::Module* mod, const std::string& Name, llvm::Type *RetTy,
                                      ArgsTy... Args) {
            if(!mod)
                return nullptr;
            llvm::SmallVector<llvm::Type*, sizeof...(ArgsTy)> ArgTys{Args...};
            return getOrInsertFunction(mod, Name, llvm::FunctionType::get(RetTy, ArgTys, false));
        }

        // cJSON helper functions (for easier access)
        extern llvm::Value* call_cjson_getitem(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern llvm::Value* call_cjson_isnumber(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern llvm::Value* call_cjson_isnull(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern llvm::Value* call_cjson_isstring(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern llvm::Value* call_cjson_isobject(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern llvm::Value* get_cjson_as_integer(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern llvm::Value* get_cjson_as_float(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
        extern SerializableValue get_cjson_as_string_value(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);

        extern llvm::Value* call_cjson_create_empty(llvm::IRBuilder<>& builder);

        extern llvm::Value* call_simdjson_to_cjson_object(llvm::IRBuilder<>& builder, llvm::Value* json_item);

        [[maybe_unused]] extern SerializableValue serialize_cjson_as_runtime_str(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);

        // extended cjson function to check homogeneity of list
        [[maybe_unused]] extern llvm::Value* call_cjson_is_list_of_generic_dicts(llvm::IRBuilder<>& builder, llvm::Value* cjson_obj);
    }
}

#endif //TUPLEX_CODEGENHELPER_H
