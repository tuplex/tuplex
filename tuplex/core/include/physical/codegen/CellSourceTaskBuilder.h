//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CELLSOURCETASKBUILDER_H
#define TUPLEX_CELLSOURCETASKBUILDER_H

#include "physical/codegen/BlockBasedTaskBuilder.h"

namespace tuplex {
    namespace codegen {

        /*!
         * This class produces a function taking (void* userData, char **cells, int64_t *cell_sizes)
         * and performs then necessary value conversions/null checks etc.
         */
        class CellSourceTaskBuilder : public BlockBasedTaskBuilder {
        private:
            int64_t _operatorID; // operatorID where to put null failures / value errors etc. out
            python::Type _fileInputRowType; /// original file input type, i.e. how many cells + what they should represent, unprojected.
            python::Type _fileInputRowTypeGeneralCase; /// original file input type general case, unprojected.
            std::vector<bool> _columnsToSerialize; /// which columns from the file input type to serialize, determines output type. Good for projection pushdown.
            std::vector<bool> _generalCaseColumnsToSerialize; // same for the general case
            std::map<int, int> _normalToGeneralMapping;
            std::string _functionName; /// name of the LLVM function

            std::vector<std::string> _nullValues; /// strings that should be interpreted as null values.
            size_t numCells() const { return _fileInputRowType.parameters().size(); }

            FlattenedTuple cellsToTuple(llvm::IRBuilder<>& builder,
                                        const std::vector<bool> columnsToSerialize,
                                        const python::Type& inputRowType,
                                        llvm::Value* cellsPtr,
                                        llvm::Value* sizesPtr);

            llvm::BasicBlock* _valueErrorBlock;
            llvm::BasicBlock* _nullErrorBlock;
            llvm::BasicBlock* valueErrorBlock(llvm::IRBuilder<>& builder); // create a value error(conversion failure block lazily)
            llvm::BasicBlock* nullErrorBlock(llvm::IRBuilder<>& builder); // create an (internal) nullerror (i.e. a non option type was expected, but actually there was a null! Only with active null value optimization...)

            inline llvm::Value* nullCheck(llvm::IRBuilder<>& builder, llvm::Value* ptr) {
                assert(ptr);
                // Note: maybe put this into separate function & emit call? ==> might be easier for llvm to optimize!
                return env().compareToNullValues(builder, ptr, _nullValues, true); // NOTE: ptr must be 0 terminated!
            }

            SerializableValue cachedParse(llvm::IRBuilder<>& builder, const python::Type& type, size_t colNo, llvm::Value* cellsPtr, llvm::Value* sizesPtr);
            std::unordered_map<std::tuple<size_t, python::Type>, SerializableValue> _parseCache; // certain columns may be cached to avoid calling expensive parsing multiple times.
            void generateChecks(llvm::IRBuilder<>& builder, llvm::Value* userData, llvm::Value* rowNumber, llvm::Value* cellsPtr, llvm::Value* sizesPtr);

            inline FlattenedTuple parseGeneralCaseRow(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {
                return cellsToTuple(builder, _generalCaseColumnsToSerialize, _fileInputRowTypeGeneralCase, cellsPtr, sizesPtr);
            }
            inline FlattenedTuple parseNormalCaseRow(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {
                return cellsToTuple(builder, _columnsToSerialize, _fileInputRowType, cellsPtr, sizesPtr);
            }

            SerializableValue serializeFullRowAsBadParseException(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr,
                                                                  llvm::Value* sizesPtr) const ;

            SerializableValue serializeGeneralColumnsAsBadParseException(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr,
                                                                  llvm::Value* sizesPtr) const ;

            SerializableValue serializeBadParseException(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr,
                                                         llvm::Value* sizesPtr, bool use_dummies,
                                                         bool use_only_projected_general_case_columns) const;

            SerializableValue serializeCellVector(llvm::IRBuilder<>& builder,
                                                  const std::vector<llvm::Value*>& cells,
                                                  const std::vector<llvm::Value*>& cell_sizes,
                                                  llvm::Value* empty_str=nullptr) const;

            inline void exitWith(llvm::IRBuilder<>& builder, llvm::Value* ecCode) {
                assert(ecCode);
                if(ecCode->getType() != env().i64Type())
                    ecCode = builder.CreateZExtOrTrunc(ecCode, env().i64Type());

                // to make compatible, use negative ecCode
                ecCode = builder.CreateMul(env().i64Const(-1), ecCode);
                builder.CreateRet(ecCode);
            }

            inline void exitWith(llvm::IRBuilder<>& builder, const ExceptionCode& ec) {
                exitWith(builder, env().i64Const(ecToI64(ec)));
            }

        public:
            CellSourceTaskBuilder() = delete;

            /*!
             * construct a new task which parses CSV input (given block wise)
             * @param env CodeEnv where to generate code into
             * @param normalCaseRowType the detected row Type of the file
             * @param restrictedGeneralCaseInputRowType the (restricted) detected general case row type of the file
             * @param normalCaseColumnsToSerialize if empty vector, all rows get serialized. If not, indicates which columns should be serialized. Length must match rowType.
             * @param name Name of the function to generate++
             * @param except_mode specify how exceptions should be serialized.
             * @param operatorID ID of the operator for exception handling.
             * @param null_values array of strings that should be interpreted as null values
             * @param checks array of checks to perform and else issue a normalcaseviolation
             */

            explicit CellSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                           const python::Type& normalCaseRowType,
                                           const std::vector<bool> &normalCaseColumnsToSerialize,
                                           const python::Type& generalCaseInputRowType,
                                           const std::vector<bool> &generalCaseColumnsToSerialize,
                                           const std::map<int, int>& normalToGeneralMapping,
                                           const std::string &name,
                                           const ExceptionSerializationMode& except_mode,
                                           int64_t operatorID,
                                           const std::vector<std::string>& null_values=std::vector<std::string>{""},
                                           const std::vector<NormalCaseCheck>& checks={}) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env,
                                                                                                              restrictRowType(
                                                                                                                      normalCaseColumnsToSerialize,
                                                                                                                      normalCaseRowType),
                                                                                                              restrictRowType(generalCaseColumnsToSerialize,
                                                                                                                     generalCaseInputRowType),
                                                                                                              normalToGeneralMapping,
                                                                                                              name,
                                                                                                              except_mode,
                                                                                                              checks),
                                                                 _operatorID(operatorID),
                                                                 _fileInputRowType(normalCaseRowType),
                                                                 _fileInputRowTypeGeneralCase(generalCaseInputRowType),
                                                                 _columnsToSerialize(normalCaseColumnsToSerialize),
                                                                 _generalCaseColumnsToSerialize(generalCaseColumnsToSerialize),
                                                                 _normalToGeneralMapping(normalToGeneralMapping),
                                                                 _functionName(name),
                                                                 _nullValues(null_values),
                                                                 _valueErrorBlock(nullptr),
                                                                 _nullErrorBlock(nullptr) {

                // both cases should have same amount of columns
                assert(extract_columns_from_type(_fileInputRowType) == extract_columns_from_type(_fileInputRowTypeGeneralCase));


                // make sure columnsToSerialize array is valid
                // if empty array given, add always trues
                if(_columnsToSerialize.empty()) {
                    for(int i = 0; i < extract_columns_from_type(_fileInputRowType); ++i)
                        _columnsToSerialize.emplace_back(true);
                }

                // check that counts hold up
                // i.e., assert(_columnsToSerialize.size() <= restrictedGeneralCaseInputRowType.parameters().size());
                int num_normal_cols = 0;
                int num_general_cols = 0;
                if(!_columnsToSerialize.empty() && ! _generalCaseColumnsToSerialize.empty()) {
                    assert(_columnsToSerialize.size() == _generalCaseColumnsToSerialize.size());
                    for(unsigned i = 0; i < std::min(_columnsToSerialize.size(), _generalCaseColumnsToSerialize.size()); ++i) {
                        num_normal_cols += _columnsToSerialize[i];
                        num_general_cols += _generalCaseColumnsToSerialize[i];
                    }
                }
                assert(num_normal_cols <= num_general_cols);

                assert(_columnsToSerialize.size() == extract_columns_from_type(_fileInputRowType));
            }

            llvm::Function *build(bool terminateEarlyOnLimitCode) override;

            std::tuple<python::Type, std::string>
            extract_type_and_value_from_constant_check(const NormalCaseCheck &check) const;
        };


        extern SerializableValue parse_string_cell(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::BasicBlock* bParseError,
                                            const python::Type& cell_type, const std::vector<std::string>& null_values,
                                            llvm::Value* cell_str, llvm::Value* cell_size);

        extern SerializableValue serialize_cell_vector(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                       const std::vector<llvm::Value *> &cells,
                                                       const std::vector<llvm::Value *> &cell_sizes,
                                                       llvm::Value *empty_str);
    }
}

#endif //TUPLEX_CELLSOURCETASKBUILDER_H