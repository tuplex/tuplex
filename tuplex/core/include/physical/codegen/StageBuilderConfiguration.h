//
// Created by Leonhard Spiegelberg on 1/31/23.
//

#ifndef TUPLEX_STAGEBUILDERCONFIGURATION_H
#define TUPLEX_STAGEBUILDERCONFIGURATION_H

#include <ContextOptions.h>
#include <codegen/CodegenHelper.h>
#include <ExceptionCodes.h>
#include "CodeDefs.h"

namespace tuplex {
    namespace codegen {

        struct StageBuilderConfiguration {
            CompilePolicy policy; // compiler policy for stage and UDFs
            bool allowUndefinedBehavior;
            bool generateParser; // whether to generate a parser
            bool sharedObjectPropagation;
            bool nullValueOptimization; // whether to use null value optimization
            bool constantFoldingOptimization; // whether to apply constant folding or not
            bool updateInputExceptions; // whether input exceptions indices need to be updated (change for experimental incremental exception handling)
            bool generateSpecializedNormalCaseCodePath; // whether to emit specialized normal case code path or not
            bool filterPromotion; // whether to propagate filters to become checks, i.e. manipulate the sample to be only rows that pass the filter.

            bool pure_python_mode; // whether to generate only python code

            size_t sampling_size; // re-sampling sample size

            ExceptionSerializationMode exceptionSerializationMode;

            StageBuilderConfiguration() : policy(CompilePolicy()),
                                          allowUndefinedBehavior(false),
                                          generateParser(false),
                                          sharedObjectPropagation(true),
                                          nullValueOptimization(true),
                                          constantFoldingOptimization(true),
                                          updateInputExceptions(false),
                                          generateSpecializedNormalCaseCodePath(true),
                                          filterPromotion(false),
                                          pure_python_mode(false),
                                          exceptionSerializationMode(ExceptionSerializationMode::SERIALIZE_AS_GENERAL_CASE),
                                          sampling_size(0) {}

            // update with context option object
            inline void applyOptions(const ContextOptions& co) {
                policy = compilePolicyFromOptions(co);
                allowUndefinedBehavior = co.UNDEFINED_BEHAVIOR_FOR_OPERATORS();
                generateParser = co.OPT_GENERATE_PARSER();
                sharedObjectPropagation = co.OPT_SHARED_OBJECT_PROPAGATION();
                nullValueOptimization = co.OPT_NULLVALUE_OPTIMIZATION();
                constantFoldingOptimization = co.OPT_CONSTANTFOLDING_OPTIMIZATION();
                filterPromotion = co.OPT_FILTER_PROMOTION();
                pure_python_mode = co.PURE_PYTHON_MODE();
                sampling_size = co.SAMPLE_MAX_DETECTION_MEMORY();

                // from options, infer
                if(co.EXPERIMENTAL_FORCE_BAD_PARSE_EXCEPT_FORMAT())
                    exceptionSerializationMode = ExceptionSerializationMode::SERIALIZE_MISMATCH_ALWAYS_AS_BAD_PARSE;
                else
                    exceptionSerializationMode = ExceptionSerializationMode::SERIALIZE_AS_GENERAL_CASE;

                // updateInputExceptions = false; // this is a weird setting, has to be done manually?
                // generateSpecializedNormalCaseCodePath = true // also needs to be manually set.
            }

            StageBuilderConfiguration(const ContextOptions& co) : StageBuilderConfiguration::StageBuilderConfiguration() {
                applyOptions(co);
            }
        };
    }
}
#endif //TUPLEX_STAGEBUILDERCONFIGURATION_H
