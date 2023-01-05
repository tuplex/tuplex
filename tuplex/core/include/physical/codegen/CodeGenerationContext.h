//
// Created by leonhards on 3/23/22.
//

#ifndef TUPLEX_CODEGENERATIONCONTEXT_H
#define TUPLEX_CODEGENERATIONCONTEXT_H

#include <logical/Operators.h>
#include <nlohmann/json.hpp>
#include <google/protobuf/util/json_util.h>

namespace tuplex {
    namespace codegen {
        /*!
        * helper struct to give code-gen context for a single Stage
        */
        struct CodeGenerationContext {

            // @TODO: add updateInputExceptions...

            // Common variables, i.e. config settings for global code-gen
            bool                                            allowUndefinedBehavior;
            bool                                            sharedObjectPropagation;
            bool                                            nullValueOptimization;
            bool                                            constantFoldingOptimization;
            bool                                            isRootStage;
            bool                                            generateParser;
            double                                          normalCaseThreshold;

            // outputMode & format are shared between the different code-paths (normal & general & fallback)
            EndPointMode                                    outputMode;
            FileFormat                                      outputFileFormat;
            int64_t                                         outputNodeID;
            Schema                                          outputSchema; //! output schema of stage
            std::unordered_map<std::string, std::string>    fileOutputParameters; // parameters specific for a file output format
            size_t                                          outputLimit;

            std::vector<size_t>                             hashColKeys; // the column to use as hash key
            // python::Type                                    hashKeyType;
            // python::Type                                    hashBucketType;
            bool                                            hashSaveOthers; // whether to save other columns than the key or not. => TODO: UDAs, meanByKey etc. all will require similar things...
            bool                                            hashAggregate; // whether the hashtable is an aggregate

            // input mode and parameters are also shared between the different codepaths
            EndPointMode                                    inputMode;
            FileFormat                                      inputFileFormat;
            int64_t                                         inputNodeID;
            std::unordered_map<std::string, std::string>    fileInputParameters; // parameters specific for a file input format

            std::map<int, int> normalToGeneralMapping; // mapping of column indices of normal-case to general-case columns.

            struct CodePathContext {
                Schema readSchema;
                Schema inputSchema;
                Schema outputSchema;

                std::shared_ptr<LogicalOperator>              inputNode;
                std::vector<std::shared_ptr<LogicalOperator>> operators;
                std::vector<bool>                             columnsToRead;
                std::vector<NormalCaseCheck>                  checks;

                // columns to perform checks on (fastPathOnly)
                CodePathContext() : inputNode(nullptr) {}

                bool valid() const { return inputSchema.getRowType() != python::Type::UNKNOWN && inputNode; }

                nlohmann::json to_json() const;
                static CodePathContext from_json(nlohmann::json obj);

                inline size_t columnsToReadCount() const {
                    size_t count = 0;
                    for(const auto& b : columnsToRead)
                        count += b;
                    return count;
                }

                // columns (after pushdown) of input operator to read
                inline std::vector<std::string> columns() const {
                    assert(inputNode);
                    return inputNode->columns();
                }

                // columns (before pushdown) of input operator to read
                inline std::vector<std::string> inputColumns() const {
                    assert(inputNode);
                    return inputNode->inputColumns();
                }

                inline python::Type hashKeyType(const std::vector<size_t>& key_cols) const {
                    if(!operators.empty() && operators.back()->type() == LogicalOperatorType::AGGREGATE) {
                        auto aop = std::dynamic_pointer_cast<AggregateOperator>(operators.back());
                        return aop->keyType();
                    }
                    throw std::runtime_error("can't return hash key type");
                }

                inline python::Type hashBucketType(const std::vector<size_t>& key_cols) const {
                    if(!operators.empty() && operators.back()->type() == LogicalOperatorType::AGGREGATE) {
                        auto aop = std::dynamic_pointer_cast<AggregateOperator>(operators.back());
                        return aop->bucketType();
                    }
                    throw std::runtime_error("can't return hash key type");
                }

#ifdef BUILD_WITH_CEREAL
                template<class Archive> void serialize(Archive & ar) {
                    ar(readSchema, inputSchema, outputSchema, inputNode, operators, columnsToRead, checks);
                }
#endif
            };

            CodePathContext fastPathContext;
            CodePathContext slowPathContext;

            inline char csvOutputDelimiter() const {
                return fileOutputParameters.at("delimiter")[0];
            }
            inline char csvOutputQuotechar() const {
                return fileOutputParameters.at("quotechar")[0];
            }

            inline bool hasOutputLimit() const {
                return outputLimit < std::numeric_limits<size_t>::max();
            }

            // serialization, TODO
            std::string toJSON() const;
            static CodeGenerationContext fromJSON(const std::string& json_str);

#ifdef BUILD_WITH_CEREAL
            // serialize using Cereal
            template<class Archive> void serialize(Archive & ar) {
                ar(allowUndefinedBehavior,
                   sharedObjectPropagation,
                   nullValueOptimization,
                   constantFoldingOptimization,
                   isRootStage,
                   generateParser,
                   normalCaseThreshold,
                   outputMode,
                   outputFileFormat,
                   outputNodeID,
                   outputSchema,
                   fileOutputParameters,
                   outputLimit,
                   hashColKeys,
                   hashKeyType,
                   hashSaveOthers,
                   hashAggregate,
                   inputMode,
                   inputFileFormat,
                   inputNodeID,
                   fileInputParameters,
                   normalToGeneralMapping,
                   fastPathContext,
                   slowPathContext);
            }
#endif
        };
    }
}

#endif //TUPLEX_CODEGENERATIONCONTEXT_H
