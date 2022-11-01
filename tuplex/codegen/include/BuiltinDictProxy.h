//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_BUILTINDICTPROXY_H
#define TUPLEX_BUILTINDICTPROXY_H

#include <memory>

#include <TypeSystem.h>
#include <BuiltinDictProxyImpl.h>
#include <cJSONDictProxyImpl.h>

// TODO: Could also use a general object based system which would make things easier...
// -> i.e., sequence protocol strings/lists/...

// basically for each object we need
// 1.) representation as C++ object (field)
// 2.) code-generated logic (i.e., codegen specialization)
// 3.) to/from python object

namespace tuplex {
    namespace codegen {
        class BuiltinDictProxy {
        public:
            // BuiltinDictProxy (--> specializedDictType)
            BuiltinDictProxy(const python::Type& specializedDictType) : _specializedType(specializedDictType) {
                // use cJSON as default for now...
                _impl = std::make_shared<cJSONDictProxyImpl>();
            }

            // use both codegen/non-codegen version
            // putItem
            BuiltinDictProxy& putItem(const Field& key, const Field& value) { assert(_impl); _impl->putItem(key, value); return *this; }
            BuiltinDictProxy& putItem(const python::Type& keyType, const SerializableValue& key, const python::Type& valueType, const SerializableValue& value) { assert(_impl); _impl->putItem(keyType, key, valueType, value); return *this; }

//            // getItem
//            BuiltinDictProxy& getItem(const Field& key);
//            BuiltinDictProxy& getItem(const python::Type& keyType, const SerializableValue& key);
//
//            // delItem
//            BuiltinDictProxy& delItem(const Field& key);
//            BuiltinDictProxy& delItem(const python::Type& keyType, const SerializableValue& key);
//
//            // allocSize() --> helpful when dict size is known upfront, can be used for optimization.
//            BuiltinDictProxy& allocSize(llvm::Value* size);

            // getKeyView() --> codegen object

            // getValuesView() --> codegen object

            python::Type dictType() const {
                throw std::runtime_error("not yet implemented");
            }

            python::Type specializedDictType() const {
                return _specializedType;
            }

            // codegenToMemory

            // codegenFromMemory
            // static function?

            // codegenSerializedLength

            // toMemory

            // fromMemory
            // static function?

            // serializedLength
        private:
            python::Type _specializedType;

            // implementation...
            // -> cJSON
            // -> ...
            // -> ...
            std::shared_ptr<BuiltinDictProxyImpl> _impl;
        };
    }
}

#endif //TUPLEX_BUILTINDICTPROXY_H
