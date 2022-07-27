//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_CJSONDICTPROXYIMPL_H
#define TUPLEX_CJSONDICTPROXYIMPL_H

#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#include "optional.h"
#include <BuiltinDictProxyImpl.h>

namespace tuplex {
    namespace codegen {
        class cJSONDictProxyImpl : public BuiltinDictProxyImpl {
        public:
            cJSONDictProxyImpl() : _root(nullptr) {}
            ~cJSONDictProxyImpl() {
                if(_root) {
                    cJSON_free(_root);
                    _root = nullptr;
                }
            }
            cJSONDictProxyImpl(const cJSONDictProxyImpl& other) = delete;
            cJSONDictProxyImpl& operator = (const cJSONDictProxyImpl& other) = delete;

            void putItem(const Field& key, const Field& value) override;
            void putItem(const python::Type& keyType, const SerializableValue& key, const python::Type& valueType, const SerializableValue& value) override;


            // notes:
            // for cJSON subscripting, need to perform
            //  SerializableValue BlockGeneratorVisitor::subscriptCJSONDictionary(NSubscription *sub, SerializableValue index,
            //                                                                          const python::Type &index_type,
            //                                                                          SerializableValue value) {

        private:
            cJSON *_root;   // a map of the elements
            cJSON *_typeMap; // a map of strings -> types (nested)

            /*!
            * returns a string representing a type prefix when storing type information in cJSON object as well.
            * @param type
            * @return
            */
            static std::string typePrefix(const python::Type& type);
        };
    }
}

#endif //TUPLEX_CJSONDICTPROXYIMPL_H
