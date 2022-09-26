//
// Created by leonhard on 9/26/22.
//

#ifndef TUPLEX_JSONSOURCETASKBUILDER_H
#define TUPLEX_JSONSOURCETASKBUILDER_H

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
        class JSONSourceTaskBuilder {
        public:
            JSONSourceTaskBuilder(LLVMEnvironment &env,
                                  const python::Type &rowType,
                                  const std::string &functionName = "parseJSON", bool unwrap_first_level = true) : _env(
                    env), _rowType(rowType),
                                                                                                                   _functionName(
                                                                                                                           functionName),
                                                                                                                   _unwrap_first_level(
                                                                                                                           unwrap_first_level),
                                                                                                                   _rowNumberVar(
                                                                                                                           nullptr),
                                                                                                                   _badParseCountVar(
                                                                                                                           nullptr),
                                                                                                                   _freeStart(
                                                                                                                           nullptr),
                                                                                                                   _freeEnd(
                                                                                                                           _freeStart) {}

            void build();

        private:
            LLVMEnvironment &_env;
            python::Type _rowType;
            std::string _functionName;
            bool _unwrap_first_level;

            // helper values
            llvm::Value *_rowNumberVar;
            llvm::Value *_badParseCountVar; // stores count of bad parse emits.

            // output vars
            llvm::Value *_outTotalRowsVar;
            llvm::Value *_outTotalBadRowsVar;
            llvm::Value *_outTotalSerializationSize;

            void writeOutput(llvm::IRBuilder<>& builder, llvm::Value* var, llvm::Value* val);


            // blocks to hold start/end of frees --> called before going to next row.
            llvm::BasicBlock *_freeStart;
            llvm::BasicBlock *_freeEnd;


            // helper functions

            void generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value *bufPtr, llvm::Value *bufSize);

            llvm::Value *initJsonParser(llvm::IRBuilder<> &builder);

            void freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j);

            llvm::Value *
            openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf, llvm::Value *buf_size);

            void
            exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition, llvm::Value *exitCode);

            llvm::Value *hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);

            void moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);


            llvm::BasicBlock *
            emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *condition);

            inline llvm::Value *rowNumber(llvm::IRBuilder<> &builder) {
                assert(_rowNumberVar);
                assert(_rowNumberVar->getType() == _env.i64ptrType());
                return builder.CreateLoad(_rowNumberVar);
            }

            llvm::Value *isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j);

            void parseAndPrintStructuredDictFromObject(llvm::IRBuilder<> &builder, llvm::Value *j,
                                                       llvm::BasicBlock *bbSchemaMismatch);

            void printValueInfo(llvm::IRBuilder<> &builder, const std::string &key, const python::Type &valueType,
                                llvm::Value *keyPresent, const SerializableValue &value);

            void checkRC(llvm::IRBuilder<> &builder, const std::string &key, llvm::Value *rc);
        };
    }
}


#endif //TUPLEX_JSONSOURCETASKBUILDER_H
