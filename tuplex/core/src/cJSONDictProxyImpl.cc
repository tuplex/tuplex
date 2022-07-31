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
            
            cJSON* to_add; // = cJSON_CreateNull();

            // check type of value, create corresponding cJSON type object
            if (value.getType() == python::Type::BOOLEAN) {
                if (value.getInt() > 0) {
                    to_add = cJSON_CreateTrue();
                } else {
                    to_add = cJSON_CreateFalse();
                }
            } else if (value.getType() == python::Type::F64) {
                to_add = cJSON_CreateNumber(value.getDouble());
            } else if (value.getType() == python::Type::I64) {
                // should I be upcasting?
                to_add = cJSON_CreateNumber((double)value.getInt());
            } else if (value.getType() == python::Type::STRING) {
                to_add = cJSON_CreateString((const char*)value.getPtr());
            } else if (value.getType().isTupleType()) {
                assert(value.getPtr());

                std::tuple* tup = (std::tuple*)value.getPtr();
                to_add = cJSON_CreateArray();
                
                for (auto i : tup) {
                    
                }
            } else {
                throw std::runtime_error("cannot put value with type " + value.getType().desc() + " into cJSON object");
            }

            // add to cJSON object
            cJSON_AddItemToObject(_root, key.desc().c_str(), to_add);

            // type prefix

            // throw std::runtime_error("to implement...");
        }

        void cJSONDictProxyImpl::putItem(const python::Type &keyType, const SerializableValue &key,
                                         const python::Type &valueType, const SerializableValue &value) {
            if(!_root)
                _root = cJSON_CreateObject();

            throw std::runtime_error("to implement...");
        }
    }
}
