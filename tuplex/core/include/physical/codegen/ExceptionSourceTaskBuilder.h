//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_EXCEPTIONSOURCETASKBUILDER_H
#define TUPLEX_EXCEPTIONSOURCETASKBUILDER_H

#include "physical/codegen/BlockBasedTaskBuilder.h"

namespace tuplex {
    namespace codegen {
        /*!
         * Class used to process tuplex partitions through the pipeline when both input exceptions and filters are
         * present. This is necessary to update the row indices of any input exceptions if rows are filtered out.
         */
        class ExceptionSourceTaskBuilder : public BlockBasedTaskBuilder {
        private:
            void createMainLoop(llvm::Function* read_block_func, bool terminateEarlyOnLimitCode);

            /*!
            * generates code to process a row depending on parse result...
            * @param builder
            * @param userData a value for userData (i.e. the class ptr of the task typically) to be parsed to callback functions
            * @param tuple (flattened) tuple representation of current tuple (LLVM)
            * @param normalRowCountVar where to store normal row counts
            * @param badRowCountVar where to store bad row counts
            * @param processRowFunc (optional) function to be called before output is written.
            *        Most likely this is not a nullptr, because users want to transform data.
            */
            void processRow(llvm::IRBuilder<>& builder,
                            llvm::Value* userData,
                            const FlattenedTuple& tuple,
                            llvm::Value *normalRowCountVar,
                            llvm::Value *badRowCountVar,
                            llvm::Value *outputRowNumberVar,
                            llvm::Value *inputRowPtr,
                            llvm::Value *inputRowSize,
                            bool terminateEarlyOnLimitCode,
                            llvm::Function* processRowFunc=nullptr);

            void callProcessFuncWithHandler(llvm::IRBuilder<> &builder, llvm::Value *userData,
                                            const FlattenedTuple &tuple,
                                            llvm::Value *normalRowCountVar, llvm::Value *badRowCountVar, llvm::Value *rowNumberVar,
                                            llvm::Value *inputRowPtr, llvm::Value *inputRowSize,
                                            bool terminateEarlyOnLimitCode,
                                            llvm::Function *processRowFunc);
        public:
            ExceptionSourceTaskBuilder() = delete;

            /*!
             * creates a function to read rows from memory and process them via a pipeline.
             * Used when both input exceptions and filters are present within a pipeline
             * and exceptions need to be merged in order.
             * @param env LLVM codegen environment where to put everything
             * @param inputRowType the row type rows are stored in within the memory block
             * @param generalCaseInputRowType the row type exceptions should be stored in. inputRowType must be upcastable to generalCaseInputRowType.
             * @param name how to call the function to be generated.
             * @param serializeExceptionAsGeneralCase if true, upcasts exceptions to generalCaseInputRowType. If false, uses inputRowType.
             */
            explicit ExceptionSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment>& env,
                                                const python::Type& inputRowType,
                                                const python::Type& generalCaseInputRowType,
                                                const std::map<int, int>& normalToGeneralMapping,
                                                const std::string& name,
                                                bool serializeExceptionsAsGeneralCase) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env, inputRowType, generalCaseInputRowType, normalToGeneralMapping, name, serializeExceptionsAsGeneralCase) {}

            llvm::Function* build(bool terminateEarlyOnFailureCode) override;
        };
    }
}

#endif //TUPLEX_EXCEPTIONSOURCETASKBUILDER_H