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

#include "BlockBasedTaskBuilder.h"

namespace tuplex {
    namespace codegen {
        class ExceptionSourceTaskBuilder : public BlockBasedTaskBuilder {
        private:
            void createMainLoop(llvm::Function* read_block_func);

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
                            llvm::Function* processRowFunc=nullptr);

            void callProcessFuncWithHandler(llvm::IRBuilder<> &builder, llvm::Value *userData,
                                            const FlattenedTuple &tuple,
                                            llvm::Value *normalRowCountVar, llvm::Value *badRowCountVar, llvm::Value *rowNumberVar,
                                            llvm::Value *inputRowPtr, llvm::Value *inputRowSize,
                                            llvm::Function *processRowFunc);
        public:
            ExceptionSourceTaskBuilder() = delete;

            explicit ExceptionSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment>& env, const python::Type& rowType, const std::string& name) : BlockBasedTaskBuilder::BlockBasedTaskBuilder(env, rowType, name)   {}

            llvm::Function* build() override;
        };
    }
}

#endif //TUPLEX_EXCEPTIONSOURCETASKBUILDER_H