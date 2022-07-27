//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include <cJSONDictProxyImpl.h>

namespace tuplex {
    namespace codegen {


        // in general cJSON supports following data types:
        // string
        // number
        // boolean
        // null
        // object
        // array
        // --> yet type info from python might get lost. Hence, store it when possible as well!

        // this is a general helper function to turn a Field into a cJSON object

        /*!
         * converts a field into a cJSON object. If not convertible, returns nullptr.
         * @param f Field
         * @param includeTypePrefix
         * @return cJSON* object
         */
        cJSON* fieldToCJSON(const Field& f, bool includeTypePrefix=false) {

            return nullptr;
        }

        Field cJSONToField(const cJSON* object) {
            assert(object);

            return Field::null();
        }

        std::string cJSONDictProxyImpl::typePrefix(const python::Type& type) {

            // init map for a couple common types (int, float, bool, ...)

            // since keys in JSON are always strings, need to store type info in that string!
            return "";
        }

        void cJSONDictProxyImpl::putItem(const Field &key, const Field &value) {
            // put into cJSON, yet due to both key/type being not necessary type stable, encode type as base64 into values!
            // map primitive types directly into cJSON if possible
            if(!_root)
                _root = cJSON_CreateObject();

            // type prefix

            throw std::runtime_error("to implement...");
        }

        void cJSONDictProxyImpl::putItem(const python::Type &keyType, const SerializableValue &key,
                                         const python::Type &valueType, const SerializableValue &value) {

            throw std::runtime_error("to implement...");
        }
    }
}
