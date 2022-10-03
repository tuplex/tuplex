//
// Created by leonhard on 10/3/22.
//

#include <physical/JsonSourceTaskBuilder.h>

namespace tuplex {
    namespace codegen {
        JsonSourceTaskBuilder::JsonSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment>& env,
                                                     const python::Type& rowType, const std::string& name) : BlockBasedTaskBuilder(env, rowType, name) {

        }

        llvm::Function* JsonSourceTaskBuilder::build(bool terminateEarlyOnFailureCode) {
            return nullptr;
        }
    }
}