//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_BUILTINDICTPROXYIMPL_H
#define TUPLEX_BUILTINDICTPROXYIMPL_H

#include <TypeSystem.h>
#include <Row.h>
#include "codegen/CodegenHelper.h"

namespace tuplex {
    namespace codegen {
        class BuiltinDictProxyImpl {
        public:
            virtual void putItem(const Field& key, const Field& value) = 0;
            virtual void putItem(const python::Type& keyType, const SerializableValue& key, const python::Type& valueType, const SerializableValue& value) = 0;

            virtual bool keyExists(const Field& key) = 0;

            virtual Field getItem(const Field& key) = 0;

            virtual void replaceItem(const Field& key, const Field& value) = 0;

            virtual void deleteItem(const Field& key) = 0;

            virtual std::vector<Field> getKeysView() = 0;

            virtual std::vector<Field> getValuesView() = 0;
        };
    }
}

#endif //TUPLEX_BUILTINDICTPROXYIMPL_H