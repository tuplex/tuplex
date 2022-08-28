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
            cJSONDictProxyImpl() {
                _root = cJSON_CreateObject();   
            }
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

            bool keyExists(const Field& key) override;

            Field getItem(const Field& key) override;

            void replaceItem(const Field& key, const Field& value) override;

            void deleteItem(const Field& key) override;

            std::vector<Field> getKeysView() override;

            std::vector<Field> getValuesView() override;

            // notes:
            // for cJSON subscripting, need to perform
            //  SerializableValue BlockGeneratorVisitor::subscriptCJSONDictionary(NSubscription *sub, SerializableValue index,
            //                                                                          const python::Type &index_type,
            //                                                                          SerializableValue value) {

        private:
            cJSON *_root;   // a map of the elements

            /*!
            * returns a key (as a string) with the added type prefix
            * @param key
            * @param type
            * @return
            */
            std::string addTypePrefix(std::string key, const python::Type& type);

            /*!
            * converts a key (stored as a string in cJSON) to equivalent Field value
            * @param prefixed_key
            * @return
            */
            Field keyToField(std::string prefixed_key);
        };
    }
}

#endif //TUPLEX_CJSONDICTPROXYIMPL_H
