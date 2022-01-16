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

#include "BlockBasedTaskBuilder.h"

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
            std::string _functionName; /// name of the LLVM function

            std::vector<std::string> _nullValues; /// strings that should be interpreted as null values.

            size_t numCells() const { return _fileInputRowType.parameters().size(); }

            FlattenedTuple cellsToTuple(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr);

            llvm::BasicBlock* _valueErrorBlock;
            llvm::BasicBlock* _nullErrorBlock;
            llvm::BasicBlock* valueErrorBlock(llvm::IRBuilder<>& builder); // create a value error(conversion failure block lazily)
            llvm::BasicBlock* nullErrorBlock(llvm::IRBuilder<>& builder); // create an (internal) nullerror (i.e. a non option type was expected, but actually there was a null! Only with active null value optimization...)

            inline llvm::Value* nullCheck(llvm::IRBuilder<>& builder, llvm::Value* ptr) {
                assert(ptr);
                // Note: maybe put this into separate function & emit call? ==> might be easier for llvm to optimize!
                return env().compareToNullValues(builder, ptr, _nullValues, true); // NOTE: ptr must be 0 terminated!
            }

        public:
            CellSourceTaskBuilder() = delete;

            /*!
             * construct a new task which parses CSV input (given block wise)
             * @param env CodeEnv where to generate code into
             * @param rowType the detected row Type of the file
             * @param columnsToSerialize if empty vector, all rows get serialized. If not, indicates which columns should be serialized. Length must match rowType.
             * @param name Name of the function to generate
             * @param operatorID ID of the operator for exception handling.
             * @param null_values array of strings that should be interpreted as null values
             */
            explicit CellSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                           const python::Type &rowType,
                                           const std::vector<bool> &columnsToSerialize,
                                           const std::string &name,
                                           int64_t operatorID,
                                           const std::vector<std::string>& null_values=std::vector<std::string>{""}) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env,
                                                                                                              restrictRowType(
                                                                                                                      columnsToSerialize,
                                                                                                                      rowType),
                                                                                                              name),
                                                                 _operatorID(operatorID),
                                                                 _fileInputRowType(rowType),
                                                                 _columnsToSerialize(columnsToSerialize),
                                                                 _functionName(name),
                                                                 _nullValues(null_values),
                                                                 _valueErrorBlock(nullptr),
                                                                 _nullErrorBlock(nullptr) {

                // make sure columnsToSerilaize array is valid
                // if empty array given, add always trues
                if(_columnsToSerialize.empty()) {
                    for(int i = 0; i < _fileInputRowType.parameters().size(); ++i)
                        _columnsToSerialize.emplace_back(true);
                }

                assert(_columnsToSerialize.size() == _fileInputRowType.parameters().size());
            }

            llvm::Function *build(bool terminateEarlyOnLimitCode) override;
        };
    }
}

#endif //TUPLEX_CELLSOURCETASKBUILDER_H