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
                                  const python::Type& rowType, const std::string& name);

            llvm::Function* build(bool terminateEarlyOnFailureCode) override;
        private:
            std::string _functionName; /// name of the LLVM function
        };
    }
}

#endif //TUPLEX_JSONSOURCETASKBUILDER_H
