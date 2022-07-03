//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 7/4/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef HASHHELPER_HEADER_
#define HASHHELPER_HEADER_

#include "LLVMEnvironment.h"
#include "CodegenHelper.h"

namespace tuplex {
    namespace codegen {

        // this is a simple hashmap proxy structure
        class HashProxy {
        public:
            /*!
             * create a new hashmap (codegen)
             * @param builder
             * @param global whether to create the hashmap as global variable or not.
             */
            HashProxy(llvm::IRBuilder<>& builder, bool global=false);

            /*!
             * put a value into the hashmap
             * @param builder
             * @param key
             * @param value if default serializable value, then no
             */
            void put(llvm::IRBuilder<>& builder,
                                    const SerializableValue& key,
                                    const SerializableValue& value=SerializableValue());

        };

        // these are helper functions to deal with generating code to hash different keys etc.
        extern void hashmap_put(llvm::IRBuilder<>& builder,
                                const SerializableValue& key,
                                const SerializableValue& value);
    }
}
#endif