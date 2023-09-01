//
// Created by leonhards on 3/11/23.
//

#ifndef TUPLEX_EXCEPTIONTRACE_H
#define TUPLEX_EXCEPTIONTRACE_H

#include <codegen/LLVMEnvironment.h>

namespace tuplex {
    namespace codegen {
        enum class CodePathIdentifier {
            NORMAL=1,
            GENERAL=2,
            FALLBACK=3
        };

        // use this for the future to trace exceptions properly.
        extern void traceExceptionOrigin(LLVMEnvironment& env,
                                  llvm::IRBuilder<>& builder,
                                  const ExceptionCode& ec,
                                  const CodePathIdentifier& code_path,
                                  const std::string& function,
                                  const std::string& message,
                                  size_t row, size_t col);
    }
}

#endif //TUPLEX_EXCEPTIONTRACE_H
