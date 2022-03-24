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
            python::Type _fileInputRowType; /// original file input type, i.e. how many cells + what they should represent
            std::vector<bool> _columnsToSerialize; /// which columns from the file input type to serialize, determines output type. Good for projection pushdown.
            std::vector<bool> _generalCaseColumnsToSerialize; // same for the general case
            std::string _functionName; /// name of the LLVM function

            std::vector<std::string> _nullValues; /// strings that should be interpreted as null values.
            std::vector<NormalCaseCheck> _checks; ///! normal case checks
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
                return cellsToTuple(builder, _generalCaseColumnsToSerialize, _inputRowTypeGeneralCase, cellsPtr, sizesPtr);
            }
            inline FlattenedTuple parseNormalCaseRow(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {
                return cellsToTuple(builder, _columnsToSerialize, _fileInputRowType, cellsPtr, sizesPtr);
            }
        public:
            CellSourceTaskBuilder() = delete;

            /*!
             * construct a new task which parses CSV input (given block wise)
             * @param env CodeEnv where to generate code into
             * @param fileInputRowType the detected row Type of the file
             * @param restrictedGeneralCaseInputRowType the (restricted) detected general case row type of the file
             * @param columnsToSerialize if empty vector, all rows get serialized. If not, indicates which columns should be serialized. Length must match rowType.
             * @param name Name of the function to generate
             * @param operatorID ID of the operator for exception handling.
             * @param null_values array of strings that should be interpreted as null values
             * @param checks array of checks to perform and else issue a normalcaseviolation
             */

            explicit CellSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                           const python::Type& fileInputRowType,
                                           const std::vector<bool> &columnsToSerialize,
                                           const python::Type& generalCaseInputRowType,
                                           const std::vector<bool> &generalCaseColumnsToSerialize,
                                           const std::map<int, int>& normalToGeneralMapping,
                                           const std::string &name,
                                           int64_t operatorID,
                                           const std::vector<std::string>& null_values=std::vector<std::string>{""},
                                           const std::vector<NormalCaseCheck>& checks={}) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env,
                                                                                                              restrictRowType(
                                                                                                                      columnsToSerialize,
                                                                                                                      fileInputRowType),
                                                                                                                      restrictRowType(generalCaseColumnsToSerialize,
                                                                                                                     generalCaseInputRowType),
                                                                                                              normalToGeneralMapping,
                                                                                                              name),
                                                                 _operatorID(operatorID),
                                                                 _fileInputRowType(fileInputRowType),
                                                                 _columnsToSerialize(columnsToSerialize),
                                                                 _functionName(name),
                                                                 _nullValues(null_values),
                                                                 _checks(checks),
                                                                 _valueErrorBlock(nullptr),
                                                                 _nullErrorBlock(nullptr) {

                // make sure columnsToSerilaize array is valid
                // if empty array given, add always trues
                if(_columnsToSerialize.empty()) {
                    for(int i = 0; i < _fileInputRowType.parameters().size(); ++i)
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

                assert(_columnsToSerialize.size() == _fileInputRowType.parameters().size());
            }

            llvm::Function *build(bool terminateEarlyOnLimitCode) override;
        };
    }
}

#endif //TUPLEX_CELLSOURCETASKBUILDER_H