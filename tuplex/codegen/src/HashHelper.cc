//
// Created by Leonhard Spiegelberg on 7/3/22.
//
#include <HashHelper.h>

namespace tuplex {
    namespace codegen {
        // these are helper functions to deal with generating code to hash different keys etc.
        extern void hashmap_put(llvm::IRBuilder<>& builder,
                                const SerializableValue& key,
                                const SerializableValue& value) {
            // check what type key is => this determines the structure
        }
    }
}