//
// Created by leonhards on 3/23/22.
//

#ifndef TUPLEX_CODEGENERATIONCONTEXT_H
#define TUPLEX_CODEGENERATIONCONTEXT_H

#include <logical/Operators.h>

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
            python::Type                                    hashKeyType;
            bool                                            hashSaveOthers; // whether to save other columns than the key or not. => TODO: UDAs, meanByKey etc. all will require similar things...
            bool                                            hashAggregate; // whether the hashtable is an aggregate

            // input mode and parameters are also shared between the different codepaths
            EndPointMode                                    inputMode;
            FileFormat                                      inputFileFormat;
            int64_t                                         inputNodeID;
            std::unordered_map<std::string, std::string>    fileInputParameters; // parameters specific for a file input format

            std::map<int, int> normalToGeneralMapping; // mapping of column indices of normal-case to general-case columns.


//                // Resolve variables (they will be only present on slow path!)
//                std::vector<LogicalOperator*>                   resolveOperators;
//                // the input node of the general case path. => fast-path may specialize its own input Node!
//                LogicalOperator*                                inputNode;


            struct CodePathContext {
                Schema readSchema;
                Schema inputSchema;
                Schema outputSchema;

                std::shared_ptr<LogicalOperator>              inputNode;
                std::vector<std::shared_ptr<LogicalOperator>> operators;
                std::vector<bool>             columnsToRead;

                // columns to perform checks on (fastPathOnly)
                CodePathContext() : inputNode(nullptr) {}

                bool valid() const { return inputSchema.getRowType() != python::Type::UNKNOWN && inputNode; }

                nlohmann::json to_json() const;
                static CodePathContext from_json(nlohmann::json obj);

                template<class Archive> void serialize(Archive & ar) {
                    ar(readSchema, inputSchema, outputSchema, inputNode, operators, columnsToRead);
                }
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

//                Schema resolveReadSchema; //! schema for reading input
//                Schema resolveInputSchema; //! schema after applying projection pushdown to input source code
//                Schema resolveOutputSchema; //! output schema of stage
//
//                Schema normalCaseInputSchema; //! schema after applying normal case optimizations

            // python::Type hashBucketType;

//                // Fast Path
//                std::vector<LogicalOperator*> fastOperators;
//                python::Type fastReadSchema;
//                python::Type fastInSchema;
//                python::Type fastOutSchema;


            // serialize using Cereal
            template<class Archive> void serialize(Archive & ar) {
                ar(allowUndefinedBehavior,
                   sharedObjectPropagation,
                   nullValueOptimization,
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
        };
    }
}

#endif //TUPLEX_CODEGENERATIONCONTEXT_H
