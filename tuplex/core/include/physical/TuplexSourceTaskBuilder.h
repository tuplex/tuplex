//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TUPLEXSOURCETASKBUILDER_H
#define TUPLEX_TUPLEXSOURCETASKBUILDER_H

#include "BlockBasedTaskBuilder.h"

namespace tuplex {
    namespace codegen {
        class TuplexSourceTaskBuilder : public BlockBasedTaskBuilder {
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
                                            llvm::Value *normalRowCountVar, llvm::Value *rowNumberVar,
                                            llvm::Value *inputRowPtr, llvm::Value *inputRowSize,
                                            bool terminateEarlyOnLimitCode,
                                            llvm::Function *processRowFunc);
        public:
            TuplexSourceTaskBuilder() = delete;

            /*!
            * creates a function to read rows from memory and process them via a pipeline.
             * Memory format is Tuplex's internal storage format.
            * @param env LLVM codegen environment where to put everything
            * @param inputRowType the row type rows are stored in within the memory block
            * @param generalCaseInputRowType the row type exceptions should be stored in. inputRowType must be upcastable to generalCaseInputRowType.
            * @param name how to call the function to be generated.
            */
            explicit TuplexSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment>& env,
                                             const python::Type& inputRowType,
                                             const python::Type& generalCaseInputRowType,
                                             const std::map<int, int>& normalToGeneralMapping,
                                             const std::string& name) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env, inputRowType, generalCaseInputRowType, normalToGeneralMapping, name)   {}

            llvm::Function* build(bool terminateEarlyOnLimitCode) override;
        };
    }
}

#endif //TUPLEX_TUPLEXSOURCETASKBUILDER_H