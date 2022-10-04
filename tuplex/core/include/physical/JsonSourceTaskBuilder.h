//
// Created by Leonhard Spiegelberg on 9/30/22.
//

#ifndef TUPLEX_JSONSOURCETASKBUILDER_H
#define TUPLEX_JSONSOURCETASKBUILDER_H

#include "BlockBasedTaskBuilder.h"

namespace tuplex {
    namespace codegen {
        class JsonSourceTaskBuilder : public BlockBasedTaskBuilder {
        public:
            JsonSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment>& env,
                                  const python::Type& normalCaseRowType,
                                  const python::Type& generalCaseRowType,
                                  const std::string& name);

            llvm::Function* build(bool terminateEarlyOnFailureCode) override;
        private:
            std::string _functionName; /// name of the LLVM function

            python::Type _normalCaseRowType;
            python::Type _generalCaseRowType;

            // codegen helper variables
            // blocks to hold start/end of frees --> called before going to next row.
            llvm::BasicBlock *_freeStart;
            llvm::BasicBlock *_freeEnd;

            // helper values
            llvm::Value *_rowNumberVar;
            llvm::Value *_normalRowCountVar;
            llvm::Value *_generalRowCountVar;
            llvm::Value *_fallbackRowCountVar;
            llvm::Value *_normalMemorySizeVar;
            llvm::Value *_generalMemorySizeVar;
            llvm::Value *_fallbackMemorySizeVar;
            llvm::Value* _parsedBytesVar;

            void initVars(llvm::IRBuilder<>& builder);

            /*!
             * generate parse loop and return number of parsed bytes.
             * @param builder
             * @param bufPtr
             * @param bufSize
             * @return
             */
            llvm::Value* generateParseLoop(llvm::IRBuilder<>& builder, llvm::Value* bufPtr, llvm::Value* bufSize);

            inline llvm::Value* incVar(llvm::IRBuilder<>& builder, llvm::Value* var, llvm::Value* what_to_add) {
                llvm::Value* val = builder.CreateLoad(var);
                val = builder.CreateAdd(val, what_to_add);
                builder.CreateStore(val, var);
                return val;
            }

            inline llvm::Value* incVar(llvm::IRBuilder<>& builder, llvm::Value* var, int64_t delta=1) {
                return incVar(builder, var, _env->i64Const(delta));
            }

            inline llvm::Value *rowNumber(llvm::IRBuilder<> &builder) {
                assert(_rowNumberVar);
                assert(_rowNumberVar->getType() == _env->i64ptrType());
                return builder.CreateLoad(_rowNumberVar);
            }

            llvm::Value* parsedBytes(llvm::IRBuilder<>& builder, llvm::Value* parser, llvm::Value* buf_size);

            llvm::Value *isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j);
            llvm::Value* parseRowAsStructuredDict(llvm::IRBuilder<> &builder, const python::Type& row_type, llvm::Value *j,
                                                  llvm::BasicBlock *bbSchemaMismatch);
            llvm::Value *initJsonParser(llvm::IRBuilder<> &builder);
            void freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j);
            llvm::Value *openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf, llvm::Value *buf_size);
            void exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition, llvm::Value *exitCode);
            llvm::Value *hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);
            void moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);
        };
    }
}

#endif //TUPLEX_JSONSOURCETASKBUILDER_H
