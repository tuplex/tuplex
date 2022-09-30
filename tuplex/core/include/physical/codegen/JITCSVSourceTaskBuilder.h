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

#include "physical/codegen/BlockBasedTaskBuilder.h"

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
                            bool terminateEarlyOnLimitCode,
                            llvm::Function *processRowFunc = nullptr);

            void generateParser();

            // building vars for LLVM
            void createMainLoop(llvm::Function *read_block_func, bool terminateEarlyOnLimitCode);

            FlattenedTuple createFlattenedTupleFromCSVParseResult(llvm::IRBuilder<> &builder, llvm::Value *parseResult,
                                                                  const python::Type &parseRowType);

            std::vector<bool> _columnsToSerialize;
        public:
            JITCSVSourceTaskBuilder() = delete;

#warning "whatever fixes are done in cellsource task builder should apply here too."

            /*!
             * construct a new task which parses CSV input (given block wise)
             * @param env CodeEnv where to generate code into
             * @param fileInputRowType the detected row Type of the file
             * @param fileGeneralCaseInputRowType the detected general case row type of the file
             * @param columnsToSerialize if empty vector, all rows get serialized. If not, indicates which columns should be serialized. Length must match rowType.
             * @param name Name of the function to generate
             * @param operatorID ID of the operator for exception handling.
             * @param delimiter CSV delimiter for which to produce a parser
             * @param quotechar CSV quotechar for which to produce a parser
             * @param checks normal case checks that are required to be satisfied upon reading in data. If they fail, a normalcaseviolation exception is produced.
             * @param serializeExceptionAsGeneralCase if true, upcasts exceptions to generalCaseInputRowType. If false, uses inputRowType.
             */
            explicit JITCSVSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                             const python::Type& fileInputRowType,
                                             const python::Type& fileGeneralCaseInputRowType,
                                             const std::vector<bool> &columnsToSerialize,
                                             const std::map<int, int>& normalToGeneralMapping,
                                             const std::string &name,
                                             bool serializeExceptionsAsGeneralCase,
                                             int64_t operatorID,
                                             const std::vector<std::string> &null_values,
                                             char delimiter,
                                             char quotechar,
                                             const std::vector<NormalCaseCheck>& checks={}
                                             ) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env,
                                                                                                            restrictRowType(
                                                                                                                    columnsToSerialize,
                                                                                                                    fileInputRowType),
                                                                                                            restrictRowType(
                                                                                                                    columnsToSerialize,
                                                                                                                    fileGeneralCaseInputRowType),
                                                                                                            normalToGeneralMapping,
                                                                                                            name,
                                                                                                            serializeExceptionsAsGeneralCase),
                                                               _parseRowGen(
                                                                       new CSVParseRowGenerator(_env.get(), null_values,
                                                                                                quotechar, delimiter)),
                                                               _operatorID(operatorID),
                                                               _columnsToSerialize(columnsToSerialize),
                                                               _fileInputRowType(fileInputRowType) {

                if(!checks.empty())
                    throw std::runtime_error("normal checks not yet supported for JITCSVSourceTaskBuilder");
            }

            virtual llvm::Function *build(bool terminateEarlyOnFailureCode) override;
        };
    }
}

#endif //TUPLEX_JITCSVSOURCETASKBUILDER_H