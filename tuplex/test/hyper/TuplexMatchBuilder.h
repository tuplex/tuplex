//
// Created by leonhard on 9/26/22.
//

#ifndef TUPLEX_TUPLEXMATCHBUILDER_H
#define TUPLEX_TUPLEXMATCHBUILDER_H

// bindings
#include <StringUtils.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>
#include <fstream>
#include <TypeHelper.h>
#include <Utils.h>
#include <compression.h>
#include <RuntimeInterface.h>

#include <AccessPathVisitor.h>

#include <llvm/IR/TypeFinder.h>

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <physical/experimental/JsonHelper.h>

#include "JSONParseRowGenerator.h"

namespace tuplex {
    namespace codegen {
        class TuplexMatchBuilder {
        public:
            TuplexMatchBuilder(LLVMEnvironment &env,
                                  const python::Type& normalCaseRowType,
                                  const python::Type& generalCaseRowType,
                                  const std::string &functionName = "parseJSON", bool unwrap_first_level = true) : _env(env),
                                  _normalCaseRowType(normalCaseRowType),
                                  _generalCaseRowType(generalCaseRowType),
                                                                                                                   _functionName(
                                                                                                                           functionName),
                                                                                                                   _unwrap_first_level(
                                                                                                                           unwrap_first_level),
                                                                                                                   _rowNumberVar(
                                                                                                                           nullptr),
                                                                                                                   _freeStart(
                                                                                                                           nullptr),
                                                                                                                   _freeEnd(
                                                                                                                           _freeStart) {

                _normalRowCountVar = nullptr;
                _generalRowCountVar = nullptr;
                _fallbackRowCountVar = nullptr;
                _normalMemorySizeVar = nullptr;
                _generalMemorySizeVar = nullptr;
                _fallbackMemorySizeVar = nullptr;
            }

            void build(bool hackyPromoteEventFilter=false,
                       const std::string& hackyEventName="");
        private:
            LLVMEnvironment &_env;
            python::Type _normalCaseRowType;
            python::Type _generalCaseRowType;
            std::string _functionName;
            bool _unwrap_first_level;

            // helper values
            llvm::Value *_rowNumberVar;
            llvm::Value *_normalRowCountVar;
            llvm::Value *_generalRowCountVar;
            llvm::Value *_fallbackRowCountVar;
            llvm::Value *_normalMemorySizeVar;
            llvm::Value *_generalMemorySizeVar;
            llvm::Value *_fallbackMemorySizeVar;

            void writeOutput(llvm::IRBuilder<>& builder, llvm::Value* var, llvm::Value* val);

            // blocks to hold start/end of frees --> called before going to next row.
            llvm::BasicBlock *_freeStart;
            llvm::BasicBlock *_freeEnd;

            // helper functions
            void generateParseLoop(llvm::IRBuilder<> &builder,
                                   llvm::Value *bufPtr,
                                   llvm::Value *bufSize,
                                   bool hackyPromoteEventFilter,
                                   const std::string& hackyEventName);

            llvm::Value *initJsonParser(llvm::IRBuilder<> &builder);

            void freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j);

            llvm::Value *
            openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf, llvm::Value *buf_size);

            void
            exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition, llvm::Value *exitCode);

            llvm::Value *hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);

            void moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);

            llvm::Value* emitHackyFilterPromo(llvm::IRBuilder<>& builder, llvm::Value* parser, const std::string& hackyEventName, llvm::BasicBlock* bbFailure);

            llvm::BasicBlock *
            emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *condition);

            inline llvm::Value *rowNumber(llvm::IRBuilder<> &builder) {
                assert(_rowNumberVar);
                assert(_rowNumberVar->getType() == _env.i64ptrType());
                return builder.CreateLoad(_rowNumberVar);
            }

            llvm::Value *isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j);

            llvm::Value* parseRowAsStructuredDict(llvm::IRBuilder<> &builder, const python::Type& row_type, llvm::Value *j,
                                          llvm::BasicBlock *bbSchemaMismatch);

            inline llvm::Value* incVar(llvm::IRBuilder<>& builder, llvm::Value* var, llvm::Value* what_to_add) {
                llvm::Value* val = builder.CreateLoad(var);
                val = builder.CreateAdd(val, what_to_add);
                builder.CreateStore(val, var);
                return val;
            }

            inline llvm::Value* incVar(llvm::IRBuilder<>& builder, llvm::Value* var, int64_t delta=1) {
                return incVar(builder, var, _env.i64Const(delta));
            }
        };
    }
}

#endif //TUPLEX_TUPLEXMATCHBUILDER_H
