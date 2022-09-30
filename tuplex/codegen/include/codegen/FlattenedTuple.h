//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FLATTENEDTUPLE_H
#define TUPLEX_FLATTENEDTUPLE_H

#include <codegen/CodegenHelper.h>
#include <codegen/LLVMEnvironment.h>
#include <Logger.h>
#include <TupleTree.h>

namespace tuplex {

    /*!
     * in tuplex all rows are represented as tuples. To get the corresponding row type for a python primitive,
     * use this function here
     * @return
     */
    inline python::Type pyTypeToRowType(python::Type type) {
        auto original_type = type;
        type = deoptimizedType(type); // deoptimize first...

        if (type.isPrimitiveType() || python::Type::EMPTYTUPLE == type || type.isDictionaryType() ||
            python::Type::NULLVALUE == type || python::Type::GENERICTUPLE == type ||
            python::Type::PYOBJECT == type ||
            python::Type::GENERICDICT == type || type.isOptionType() || type.isListType())
            return python::Type::makeTupleType({original_type});
        else if (type.isTupleType()) {
            if (1 == type.parameters().size())
                return python::Type::makeTupleType({original_type});
        }
        return original_type;
    }

    namespace codegen {

        /*!
         * helper class representing a flattened tuple structure. Used to generate both python functions as well
         * as passing nested parameters around. Is faster than C calling convention because optimization is left to
         * LLVM
         *
         * @Todo: There is still optimization potential because as for now, also scalars get a size field. However, the
         * LLVM optimizer usually optimizes them away, so nothing to worry about.
         */
        class FlattenedTuple {
        private:
            LLVMEnvironment *_env;
            bool _forceZeroTerminatedStrings;

            // new: data is represented by tree structure
            TupleTree<tuplex::codegen::SerializableValue> _tree;
            python::Type _flattenedTupleType; // cache this for fast speed

            inline python::Type tupleType() const {
                return _tree.tupleType();
            }

            // the flattened tuple may represent an empty tuple
            // this is a special case
            bool isEmptyTuple() const { return _tree.numElements() == 0; }

            bool containsVarLenField() const;

            // encode i1 arrays as 64bit bitmaps to easily store!
            std::vector<llvm::Value*> getBitmap(llvm::IRBuilder<> &builder) const;
        public:
            FlattenedTuple(LLVMEnvironment *env) : _env(env), _forceZeroTerminatedStrings(false) {}

            FlattenedTuple(const FlattenedTuple &other) : _env(other._env),
                                                          _tree(other._tree),
                                                          _flattenedTupleType(other._flattenedTupleType),
                                                          _forceZeroTerminatedStrings(other._forceZeroTerminatedStrings) {}

            FlattenedTuple &operator=(const FlattenedTuple &other) {
                _env = other._env;
                _tree = other._tree;
                _flattenedTupleType = other._flattenedTupleType;
                _forceZeroTerminatedStrings = other._forceZeroTerminatedStrings;
                return *this;
            }


            /*!
             * sometimes pointers for strings (e.g. when coming from CSV parser) are not zero terminated because this
             * would be only possible with additional memcpy. When calling this function, an internal flag is enabled
             * such that the last byte written for the string is always '\0'.
             */
            void enableForcedZeroTerminatedStrings() { _forceZeroTerminatedStrings = true; }

            /*!
             * returns the actual (possibly) nested type of the wrapper
             * @return python::Type of the tuple stored within this wrapper
             */
            python::Type getTupleType() const { return tupleType(); }

            /*!
             * init flattened tuple according to tuple type. Provides nullptrs for elements/sizes.
             * @param type
             */
            void init(const python::Type &type);


            /*!
             * upcast this flattened Tuple to a new flattened tuple.
             * @param builder IR Builder
             * @param target_type the target type, needs to be compatible with FlattenedTuple
             * @param allow_simple_tuple_wrap because row-types per definition are
             * @return new FlattenedTuple.
             */
            FlattenedTuple upcastTo(llvm::IRBuilder<>& builder, const python::Type& target_type, bool allow_simple_tuple_wrap=false) const;

            /*!
             * retrieves an element based on the index. I.e. to access the
             * @param index
             * @return
             */
            llvm::Value *get(std::vector<int> index);

            /*!
             * gets the size of the element in bytes
             * @param index
             * @return
             */
            llvm::Value *getSize(std::vector<int> index) const;

            /*!
             * returns the number of elements the tuple possesses
             * @return
             */
            size_t numElements() const { return _tree.numElements(); }

            llvm::Value *get(const int i) const { return _tree.get(i).val; }

            llvm::Value *getSize(const int i) const { return _tree.get(i).size; }

            llvm::Value *getIsNull(const int i) const { return _tree.get(i).is_null; }

            std::vector<python::Type> getFieldTypes() const;

            python::Type fieldType(const std::vector<int>& index) const;

            inline python::Type fieldType(int index) const { return getFieldTypes()[index]; }

#ifndef NDEBUG
            void print(llvm::IRBuilder<>& builder);
#endif

            /*!
             * set using tuple index an element to a value.
             * @param index
             * @param value
             * @param size size of the element. Must be i64
             * @param isnull nullptr or i1 element
             * @return
             */
            void set(llvm::IRBuilder<> &builder, const std::vector<int>& index, llvm::Value *value, llvm::Value *size, llvm::Value *is_null);

            /*!
             * assign subtree of this flattenedtuple via another flattenedtuple
             * @param builder
             * @param index
             * @param t
             */
            void set(llvm::IRBuilder<>& builder, const std::vector<int>& index, const FlattenedTuple& t);

            /*!
             * sets a dummy value at the current index position. I.e., a null-value for the corresponding type
             * @param builder
             * @param index
             */
            void setDummy(llvm::IRBuilder<>& builder, const std::vector<int>& index);

            /*!
             * deserializes i8* pointer
             * @param builder
             * @param input memory addr from where to start deserialization
             */
            void deserializationCode(llvm::IRBuilder<> &builder, llvm::Value *input);


            /*!
             * creates serialzation, sets builder to block if serialization was successful
             * @param builder
             * @param output
             * @param capacity
             * @param insufficientCapacityHandler basicblock where to jump to when there are not enough bytes left to store the data.
             * @return serialization size (how many bytes where written)
             */
            llvm::Value *serializationCode(llvm::IRBuilder<> &builder, llvm::Value *output,
                                           llvm::Value *capacity, llvm::BasicBlock *insufficientCapacityHandler) const;

            /*!
             * serialize to pointer (no capacity check!) only use if absolutely sure it works!
             * @param builder
             * @param ptr
             */
            void serialize(llvm::IRBuilder<> &builder, llvm::Value *ptr) const;

            /*!
             * allocates via internal enviornment new memory block and fits tuple in
             * @param builder
             * @return memory pointer and size of serialized tuple
             */
            codegen::SerializableValue serializeToMemory(llvm::IRBuilder<> &builder) const;


            std::vector<llvm::Type *> getTypes();

             /*!
              * construct FlattenedTuple wrapper from the result of a function call.
              * @param val must be a struct type that is compatible with the tupletype
              * @param env
              * @param builder
              * @param ptr
              * @param type
              * @return
              */
            static FlattenedTuple fromLLVMStructVal(LLVMEnvironment *env,
                                                    llvm::IRBuilder<> &builder,
                                                    llvm::Value *ptr,
                                                    const python::Type &type);

            /*!
             * create a flattenedtuple out of constants
             * @param env
             * @param row
             * @return
             */
            static FlattenedTuple fromRow(LLVMEnvironment* env,  llvm::IRBuilder<>& builder, const Row& row);

            /*!
             * returns the nesting level for the flattened elements according to internal nesting algorithm
             * @param rowType
             * @return
             */
            static std::vector<int> nestingLevels(const python::Type &rowType);

            /*!
             * returns as llvm Value the size of the tuple by summing sizes together. If the tuple contains a single
             * variable length (serialized) type, 8 bytes for the varlen field is added.
             * @return llvm::Value representing the total size of the tuple
             */
            llvm::Value *getSize(llvm::IRBuilder<> &builder) const;

            /*!
             * sets ith element to be value/size. Automatically decodes tuples, ...
             * no copying will be performed.
             * @param builder
             * @param iElement
             * @param val
             * @param size
             */
            void setElement(llvm::IRBuilder<> &builder,
                            const int iElement,
                            llvm::Value *val,
                            llvm::Value *size,
                            llvm::Value *is_null);


            /*!
             * assigns the ith position value and size. Value needs to be a primitive type! size i64
             * @param i
             * @param val       value
             * @param size      size of value in bytes, should be llvm::Type::I64
             * @param isnull    nullptr or i1 type to indicate whether entry is null. Note: val/size can be nullptr.
             */
            void assign(const int i, llvm::Value *val, llvm::Value *size, llvm::Value *isnull);

            /*!
             * returns the (flattened) tuple as value after alloc and filling in everything
             * @return
             */
            llvm::Value *getLoad(llvm::IRBuilder<> &builder) const;


            /*!
             * returns a ptr to an tuple structure filled with the values hold by this wrapper
             * @param builder
             * @return ptr to getLLVMType() filled with data elements
             */
            llvm::Value* loadToPtr(llvm::IRBuilder<>& builder, const std::string& twine="") const {
                auto ptr = alloc(builder, twine);
                storeTo(builder, ptr);
                return ptr;
            }

            /*!
             * creates alloc for llvm struct val representing this tuple
             * @param builder
             * @return alloc tuple
             */
            llvm::Value *alloc(llvm::IRBuilder<> &builder, const std::string& twine="") const;

            /*!
             * stores contents to llvm struct val ptr.
             * @param builder
             * @param ptr
             * @return
             */
            void storeTo(llvm::IRBuilder<> &builder, llvm::Value *ptr) const;

            /*!
             * returns the value at the given index. May be a tuple
             * @param builder
             * @param index
             * @return
             */
            codegen::SerializableValue getLoad(llvm::IRBuilder<> &builder, const std::vector<int> &index);

            /*!
             * returns internal LLVM type to represent this flattened tuple structure
             * @return
             */
            llvm::Type *getLLVMType() const;


            LLVMEnvironment *getEnv() const { return _env; }


            python::Type flattenedTupleType() const {
                assert(python::Type::UNKNOWN != _flattenedTupleType);
                return _flattenedTupleType;
            }
        };

        /*!
         * convert normal tuple to general tuple by placing dummies.
         * @param builder
         * @param normal_tuple
         * @param normal_case
         * @param general_case
         * @param mapping
         * @return flattened tuple upcast to general type.
         */
        inline FlattenedTuple normalToGeneralTuple(llvm::IRBuilder<>& builder,
                                                   const FlattenedTuple& normal_tuple,
                                                   const python::Type& normal_case,
                                                   const python::Type& general_case,
                                                   const std::map<int, int>& mapping) {
            auto& logger = Logger::instance().logger("codegen");
            assert(normal_tuple.getTupleType() == normal_case);

            // could be the case that fast path/normal case requires less columns than general case
            // (never the other way round!)
            if(normal_case.parameters().size() != general_case.parameters().size()) {
                assert(normal_case.parameters().size() <= general_case.parameters().size());
                std::stringstream ss;
                ss<<"normal cases uses "<<pluralize(normal_case.parameters().size(), "column")<<" vs. general case which uses "<<pluralize(general_case.parameters().size(), "column");
                logger.debug(ss.str());
                // fill in with dummys and then set correct tuple
                assert(!mapping.empty());
                // init FlattenedTuple with general case type
                auto ft = FlattenedTuple(normal_tuple.getEnv());
                ft.init(general_case);

                // upcast flattened tuple to restricted type
                std::vector<python::Type> params(normal_case.parameters().size(), python::Type::UNKNOWN);
                std::map<int, int> generalToNormalMapping;
                for(auto kv : mapping) {
                    // check indices
                    assert(kv.first < params.size());
                    assert(kv.second < general_case.parameters().size());

                    params[kv.first] = general_case.parameters()[kv.second];
                    generalToNormalMapping[kv.second] = kv.first;
                }
                auto restrictedRowType = python::Type::makeTupleType(params);
                auto ftRestricted = normal_tuple.upcastTo(builder, restrictedRowType);

                // go through mapping & indices
                auto generalcase_col_count = general_case.parameters().size();
                for(unsigned i = 0; i < generalcase_col_count; ++i) {
                    // need to perform reverse lookup...
                    auto it = generalToNormalMapping.find(i);
                    if(it != generalToNormalMapping.end()) {
                        // found a corresponding normal row entry
                        auto element = ftRestricted.getLoad(builder, {it->second});
                        ft.set(builder, {static_cast<int>(i)}, element.val, element.size, element.is_null);
                    } else {
                        // set a dummy entry
                        ft.setDummy(builder, {static_cast<int>(i)});
                    }
                }
                return ft;
            } else {
                // upcast flattened tuple!
                auto ft = normal_tuple.upcastTo(builder, general_case);
                return ft;
            }
        }

        inline FlattenedTuple normalToGeneralTupleWithNullCompatibility(llvm::IRBuilder<>& builder,
                                                               LLVMEnvironment* env,
                                                               const FlattenedTuple& normal_tuple,
                                                               const python::Type& normal_case,
                                                               const python::Type& general_case,
                                                               const std::map<int, int>& mapping,
                                                               llvm::BasicBlock* failureBlock,
                                                               bool autoUpcast) {
            using namespace llvm;

            if(normal_case == general_case)
                return normal_tuple;

            assert(failureBlock);
            assert(env);

            FlattenedTuple target_tuple(env);
            target_tuple.init(general_case);

            auto num_elements = normal_case.parameters().size();
            auto num_target_elements = general_case.parameters().size();
            std::set<int> indices_set;

            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());
            auto& ctx = builder.GetInsertBlock()->getContext();
            auto func = builder.GetInsertBlock()->getParent();

            // put to flattenedtuple (incl. assigning tuples!)
            for (int i = 0; i < num_elements; ++i) {
                assert(mapping.find(i) != mapping.end());
                auto general_idx = mapping.at(i);
                indices_set.insert(general_idx);

                // retrieve from tuple itself and then upcast!
                auto el_type = normal_tuple.fieldType(i);
                auto el_target_type = target_tuple.fieldType(general_idx);
                SerializableValue el(normal_tuple.get(i), normal_tuple.getSize(i), normal_tuple.getIsNull(i));

                // check compatibility (todo: auto upcast?)
                SerializableValue el_target;
                if(el_type == python::Type::NULLVALUE && el_target_type.isOptionType()) {
                    // upcast simply
                    el_target = env->upcastValue(builder, el, el_type, el_target_type);
                } else if(el_type.isOptionType() && el_target_type == python::Type::NULLVALUE) {
                    // compare isnull, if is isnull == 0, then go to failure block
                    auto cond = builder.CreateICmpEQ(el.is_null, env->i1Const(false));
                    llvm::BasicBlock* bb = llvm::BasicBlock::Create(ctx, "null_check", func);
                    builder.CreateCondBr(cond, failureBlock, bb);
                    builder.SetInsertPoint(bb);

                    el_target = SerializableValue::None(builder);
                } else if(el_type.isOptionType() &&
                          el_target_type.isOptionType() &&
                          el_type.elementType() != el_target_type.elementType() ) {
                    // element types are different. Hence, can only return when
                    if(autoUpcast && python::canUpcastType(el_type.elementType(), el_target_type.elementType())) {
                        el_target = env->upcastValue(builder, el, el_type, el_target_type);
                    } else {
                        // can always upcast None to any option! --> emit check to see whether element is null.
                        auto cond = builder.CreateICmpEQ(el.is_null, env->i1Const(false));
                        llvm::BasicBlock* bb = llvm::BasicBlock::Create(ctx, "null_check", func);
                        builder.CreateCondBr(cond, failureBlock, bb);
                        builder.SetInsertPoint(bb);

                        el_target = SerializableValue::None(builder);
                    }
                } else if(el_type == el_target_type) {
                    el_target = el;
                } else {
                    // error? should not occur
                   throw std::runtime_error("INTERNAL ERROR, this branch should never be visited");
                }

                // set in general-case tuple the value.
                target_tuple.setElement(builder, general_idx, el_target.val, el_target.size, el_target.is_null);
            }

            // fill rest with dummy values
            for(unsigned i = 0; i < num_target_elements; ++i) {
                // index set?
                if(indices_set.find(i) == indices_set.end()) {
                    // set dummy
                    auto dummy_type = general_case.parameters()[i];
                    auto dummy = env->dummyValue(builder, dummy_type);
                    target_tuple.setElement(builder, i, dummy.val, dummy.size, dummy.is_null);
                }
            }

            return target_tuple;
        }
    }
}


#endif //TUPLEX_FLATTENEDTUPLE_H