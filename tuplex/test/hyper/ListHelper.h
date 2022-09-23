//
// Created by leonhard on 9/22/22.
//

#ifndef TUPLEX_LISTHELPER_H
#define TUPLEX_LISTHELPER_H

#include <LLVMEnvironment.h>

// contains helper functions to generate code to work with lists

namespace tuplex {
    namespace codegen {
        extern void list_init_empty(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type);
    }
}

#endif //TUPLEX_LISTHELPER_H
