//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JITCSVSOURCETASKBUILDER_H
#define TUPLEX_JITCSVSOURCETASKBUILDER_H

#include "BlockBasedTaskBuilder.h"

namespace tuplex {
    namespace codegen {
        /*!
         * class to build a task function using a memory block by parsing it as CSV
         */
        class JITCSVSourceTaskBuilder : public BlockBasedTaskBuilder {
        private:
            std::unique_ptr<CSVParseRowGenerator> _parseRowGen;
            int64_t _operatorID; /// operatorID where to save
            python::Type _fileInputRowType; /// original file input type

            /*!
             * generates code to process a row depending on parse result...
             * @param builder
             * @param userData a value for userData (i.e. the class ptr of the task typically) to be parsed to callback functions
             * @param parseCode the parse result (i.e. overflow, underflow, ok)
             * @param parseResult the struct holding the result of a parse
             * @param normalRowCountVar where to store normal row counts
             * @param badRowCountVar where to store bad row counts
             * @param processRowFunc (optional) function to be called before output is written.
             *        Most likely this is not a nullptr, because users want to transform data.
             */
            void processRow(llvm::IRBuilder<> &builder,
                            llvm::Value *userData,
                            llvm::Value *parseCode,
                            llvm::Value *parseResult,
                            llvm::Value *normalRowCountVar,
                            llvm::Value *badRowCountVar,
                            llvm::Value *outputRowNumberVar,
                            llvm::Value *inputRowPtr,
                            llvm::Value *inputRowSize,
                            llvm::Function *processRowFunc = nullptr);

            void generateParser();

            // building vars for LLVM
            void createMainLoop(llvm::Function *read_block_func);

            FlattenedTuple createFlattenedTupleFromCSVParseResult(llvm::IRBuilder<> &builder, llvm::Value *parseResult,
                                                                  const python::Type &parseRowType);

            std::vector<bool> _columnsToSerialize;
        public:
            JITCSVSourceTaskBuilder() = delete;

            /*!
             * construct a new task which parses CSV input (given block wise)
             * @param env CodeEnv where to generate code into
             * @param rowType the detected row Type of the file
             * @param columnsToSerialize if empty vector, all rows get serialized. If not, indicates which columns should be serialized. Length must match rowType.
             * @param name Name of the function to generate
             * @param operatorID ID of the operator for exception handling.
             * @param delimiter CSV delimiter for which to produce a parser
             * @param quotechar CSV quotechar for which to produce a parser
             */
            explicit JITCSVSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                             const python::Type &rowType,
                                             const std::vector<bool> &columnsToSerialize,
                                             const std::string &name,
                                             int64_t operatorID,
                                             const std::vector<std::string> &null_values,
                                             char delimiter,
                                             char quotechar) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env,
                                                                                                            restrictRowType(
                                                                                                                    columnsToSerialize,
                                                                                                                    rowType),
                                                                                                            name),
                                                               _parseRowGen(
                                                                       new CSVParseRowGenerator(_env.get(), null_values,
                                                                                                quotechar, delimiter)),
                                                               _operatorID(operatorID),
                                                               _columnsToSerialize(columnsToSerialize),
                                                               _fileInputRowType(rowType) {
            }

            virtual llvm::Function *build() override;
        };
    }
}

#endif //TUPLEX_JITCSVSOURCETASKBUILDER_H