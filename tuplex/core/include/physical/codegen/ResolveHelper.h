//
// Created by leonhard on 10/12/22.
//

#ifndef TUPLEX_RESOLVEHELPER_H
#define TUPLEX_RESOLVEHELPER_H

#include "PipelineBuilder.h"

namespace tuplex {
    namespace codegen {

        /*!
         * creates llvm function to process an exception row
         * (int64_t)(*)(void* userData, int64_t rowNumber, int64_t ExceptionCode, uint8_t* inputBuffer, int64_t inputBufferSize) to call pipeline over single function.
         * @param pip
         * @param name
         * @return
         */
        extern llvm::Function *createProcessExceptionRowWrapper(PipelineBuilder& pip,
                                                                const std::string& name,
                                                                const python::Type& normalCaseType,
                                                                const std::map<int, int>& normalToGeneralMapping,
                                                                const std::vector<std::string>& null_values,
                                                                const CompilePolicy& policy);

    }
}

#endif //TUPLEX_RESOLVEHELPER_H
