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
                                  int64_t input_operator_id,
                                  const python::Type& normalCaseRowType,
                                  const python::Type& generalCaseRowType,
                                  const std::vector<std::string>& normal_case_columns,
                                  const std::vector<std::string>& general_case_columns,
                                  bool unwrap_first_level,
                                  const std::map<int, int>& normalToGeneralMapping,
                                  const std::string &name,
                                  const ExceptionSerializationMode& except_mode,
                                  const std::vector<NormalCaseCheck>& checks = std::vector<NormalCaseCheck>());

            llvm::Function* build(bool terminateEarlyOnFailureCode) override;
        private:
            std::string _functionName; /// name of the LLVM function
            int64_t _inputOperatorID; // id of the input operator

            python::Type _normalCaseRowType;
            python::Type _generalCaseRowType;

            std::vector<std::string> _normal_case_columns;
            std::vector<std::string> _general_case_columns;
            bool _unwrap_first_level;

            // codegen helper variables
            // blocks to hold start/end of frees --> called before going to next row.
            llvm::BasicBlock *_freeStart;
            llvm::BasicBlock *_freeEnd;

            // helper values
            llvm::Value *_rowNumberVar;
            llvm::Value *_normalRowCountVar;
            llvm::Value *_generalRowCountVar;
            llvm::Value *_fallbackRowCountVar;

            llvm::Value* _badNormalRowCountVar;

            llvm::Value *_normalMemorySizeVar;
            llvm::Value *_generalMemorySizeVar;
            llvm::Value *_fallbackMemorySizeVar;
            llvm::Value* _parsedBytesVar;

            // this is row specific -> should be handled separately.
            llvm::Value* _row_object_var;

            void initVars(llvm::IRBuilder<>& builder);

            /*!
             * generate parse loop and return number of parsed bytes.
             * @param builder
             * @param bufPtr
             * @param bufSize
             * @return
             */
            llvm::Value* generateParseLoop(llvm::IRBuilder<>& builder, llvm::Value* bufPtr, llvm::Value* bufSize,
                                           llvm::Value *userData,
                                           const std::vector<std::string>& normal_case_columns,
                                           const std::vector<std::string>& general_case_columns,
                                           bool unwrap_first_level,
                                           bool terminateEarlyOnLimitCode);

            void generateChecks(llvm::IRBuilder<>& builder,
                                llvm::Value* userData,
                                llvm::Value* rowNumber,
                                llvm::Value* parser,
                                llvm::BasicBlock* bbSkipRow,
                                llvm::BasicBlock* bbBadRow);

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
            llvm::Value* parseRowAsStructuredDict(llvm::IRBuilder<> &builder, const python::Type& dict_type, llvm::Value *j,
                                                  llvm::BasicBlock *bbSchemaMismatch);

            FlattenedTuple parseRow(llvm::IRBuilder<>& builder, const python::Type& row_type,
                                    const std::vector<std::string>& columns,
                                    bool unwrap_first_level,
                                    llvm::Value* parser,
                                    llvm::BasicBlock *bbSchemaMismatch);

            llvm::Function* generateParseRowFunction(const std::string& name,
                                                     const python::Type& row_type,
                                                     const std::vector<std::string>& columns,
                                                     bool unwrap_first_level);

            FlattenedTuple generateAndCallParseRowFunction(llvm::IRBuilder<>& parent_builder,
                                                           const std::string& name,
                                                           const python::Type& row_type,
                                                           const std::vector<std::string>& columns,
                                                           bool unwrap_first_level,
                                                           llvm::Value* parser,
                                                           llvm::BasicBlock *bbSchemaMismatch);

            llvm::Value *initJsonParser(llvm::IRBuilder<> &builder);
            void freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j);
            llvm::Value *openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf, llvm::Value *buf_size);
            void exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition, llvm::Value *exitCode);
            llvm::Value *hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);
            void moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);

            // serialize bad parse exception
            void serializeBadParseException(llvm::IRBuilder<> &builder,
                                            llvm::Value* userData,
                                            int64_t operatorID,
                                            llvm::Value *row_no,
                                            llvm::Value *str,
                                            llvm::Value *str_size);

            void serializeAsNormalCaseException(llvm::IRBuilder<>& builder,
                                                llvm::Value* userData,
                                                int64_t operatorID,
                                                llvm::Value* row_no,
                                                const FlattenedTuple& general_case_row);
        };


        // several json related codegen functions that are useful
        /*!
         *
         * @param env
         * @param builder
         * @param row_type
         * @param columns
         * @param unwrap_first_level
         * @param fill_missing_first_level_with_null if a tuple is given as row type with options, and this flag is set then missing keys are replaced with nulls (for the first level).
         * @param parser
         * @param bbSchemaMismatch
         * @return
         */
        extern FlattenedTuple json_parseRow(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& row_type,
                                            const std::vector<std::string>& columns,
                                            bool unwrap_first_level,
                                            bool fill_missing_first_level_with_null,
                                            llvm::Value* parser,
                                            llvm::BasicBlock *bbSchemaMismatch);

        extern llvm::Function* json_generateParseRowFunction(LLVMEnvironment& env,
                                                             const std::string& name,
                                        const python::Type &row_type,
                                        const std::vector<std::string> &columns,
                                        bool unwrap_first_level);

        /*!
         * generates a llvm function that produces an llvm output according to flattenedtuple for row type and takes str pointer and size as input.
         * Returns badparsestring input as exception if parse was not successful.
         * @param env
         * @param name
         * @param row_type
         * @param columns
         * @param unwrap_first_level
         * @return function.
         */
        extern llvm::Function* json_generateParseStringFunction(LLVMEnvironment& env,
                                                                const std::string& name,
                                                                const python::Type &row_type,
                                                                const std::vector<std::string> &columns);


        inline bool row_type_compatible_with_columns(const python::Type& row_type, const std::vector<std::string>& columns) {
            if(row_type.isRowType()) {
                if(columns.size() != row_type.get_column_count())
                    return false;
                for(unsigned i = 0; i < columns.size(); ++i) {
                    if(columns[i] != row_type.get_column_names()[i])
                        return false;
                }
                return true;
            }

            if(row_type.isTupleType()) {
                return row_type.parameters().size() == columns.size();
            }
            return false;
        }
    }
}

#endif //TUPLEX_JSONSOURCETASKBUILDER_H
