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
    inline python::Type pyTypeToRowType(const python::Type &type) {
        if (type.isPrimitiveType() || python::Type::EMPTYTUPLE == type || type.isDictionaryType() ||
            python::Type::NULLVALUE == type || python::Type::GENERICTUPLE == type ||
            python::Type::PYOBJECT == type ||
            python::Type::GENERICDICT == type || type.isOptionType() || type.isListType())
            return python::Type::makeTupleType({type});
        else if (type.isTupleType()) {
            if (1 == type.parameters().size())
                return python::Type::makeTupleType({type});
        }
        return type;
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
             * @return new FlattenedTuple.
             */
            FlattenedTuple upcastTo(llvm::IRBuilder<>& builder, const python::Type& target_type) const;

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

            python::Type fieldType(const std::vector<int>& index);

            inline python::Type fieldType(int index) { return getFieldTypes()[index]; }

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
    }
}


#endif //TUPLEX_FLATTENEDTUPLE_H