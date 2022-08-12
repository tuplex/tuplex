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
            // initialise cJSON object
            cJSON* cjson_obj = nullptr;

            // check type of Field, create corresponding cJSON type object
            if (f.getType() == python::Type::BOOLEAN) {
                if (f.getInt() > 0) {
                    cjson_obj = cJSON_CreateTrue();
                } else {
                    cjson_obj = cJSON_CreateFalse();
                }
            } else if (f.getType() == python::Type::F64) {
                cjson_obj = cJSON_CreateNumber(f.getDouble());
            } else if (f.getType() == python::Type::I64) {
                // should I be upcasting?
                cjson_obj = cJSON_CreateNumber((double)f.getInt());       
            } else if (f.getType() == python::Type::STRING) {
                assert(f.getPtr());
                cjson_obj = cJSON_CreateString((const char*)f.getPtr());
            } else if (f.getType().isListType()) {
                assert(f.getPtr());

                tuplex::List* lis = (tuplex::List*)f.getPtr();
                cjson_obj = cJSON_CreateArray();
                
                for (int i = 0; i < lis->numElements(); i++) {
                    // retrieve ith element from list
                    Field element = lis->getField(i);
                    // convert to cJSON object
                    cJSON* cjson_elt = fieldToCJSON(element);

                    // add element to cJSON array
                    cJSON_AddItemToArray(cjson_obj, cjson_elt);
                }
            } else if (f.getType().isTupleType()) {
                assert(f.getPtr());

                tuplex::Tuple* tup = (tuplex::Tuple*)f.getPtr();
                cjson_obj = cJSON_CreateArray();
                
                for (int i = 0; i < tup->numElements(); i++) {
                    // retrieve ith element from tuple
                    Field element = tup->getField(i);
                    // convert to cJSON object
                    cJSON* cjson_elt = fieldToCJSON(element);

                    // add element to cJSON array
                    cJSON_AddItemToArray(cjson_obj, cjson_elt);
                }
            } else if (f.getType() == python::Type::NULLVALUE) {
                cjson_obj = cJSON_CreateNull();
            } else {
                // throw std::runtime_error("cannot change value with type " + value.getType().desc() + " into cJSON object");
            }

            return cjson_obj;
        }

        Field cJSONToField(const cJSON* object) {
            assert(object);

            Field ret = Field::null();

            if (cJSON_IsNumber(object)) {
                ret = Field(cJSON_GetNumberValue(object));
            } else if (cJSON_IsString(object)) {
                ret = Field(cJSON_GetStringValue(object));
            } else if (cJSON_IsTrue(object)) {
                ret = Field(true);
            } else if (cJSON_IsFalse(object)) {
                ret = Field(false);
            } else if (cJSON_IsNull(object)) {
                ret = Field::null();
            } else if (cJSON_IsArray(object)) {
                throw std::runtime_error("not yet implemented...");                
            } else if (cJSON_IsObject(object)) {
                throw std::runtime_error("not yet implemented...");
            }

            return ret;
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
                // _root = cJSON_CreateObject();
                throw std::runtime_error("cannot use putItem on an uninitialised dictionary");
            
            cJSON* to_add = fieldToCJSON(value);
            if (!to_add) {
                throw std::runtime_error("item to add not convertible to cJSON object");
            }

            // add to cJSON object
            // TODO: what's the difference between key.desc and getting the key's ptr value?
            // A: key.desc gets the string of the Field regardless of the type of the Field
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

        bool cJSONDictProxyImpl::keyExists(const Field& key) {
            if(!_root)
                throw std::runtime_error("cannot use keyExists on an uninitialised dictionary");
            
            cJSON* res = cJSON_GetObjectItemCaseSensitive(_root, key.desc().c_str());

            return (res != NULL);
        }

        Field cJSONDictProxyImpl::getItem(const Field& key) {
            if (!_root)
                throw std::runtime_error("cannot use getItem on an uninitialised dictionary");
            
            // retrieve value from dict
            cJSON* item = cJSON_GetObjectItemCaseSensitive(_root, key.desc().c_str());

            if (!item)
                throw std::runtime_error("error retrieving value from cJSON dictionary");

            // convert into Field
            Field field_item = cJSONToField(item);

            return field_item;
        }

        void cJSONDictProxyImpl::replaceItem(const Field& key, const Field& value) {
            if (!_root)
                throw std::runtime_error("cannot use replaceItem on an uninitialised dictionary");
            
            // assert(key.getType() == python::Type::STRING);

            // attempt to retrieve value from dict
            cJSON* item = cJSON_GetObjectItemCaseSensitive(_root, key.desc().c_str());

            if (!item) {
                // key doesn't already exist; simply perform putItem instead (?)
                putItem(key, value);
            } else {
                // replace value at key
                cJSON* new_item = fieldToCJSON(value);
                if (!new_item) {
                    throw std::runtime_error("new item not convertible to cJSON object");
                }

                cJSON_ReplaceItemInObjectCaseSensitive(_root, key.desc().c_str(), new_item);
            }
        }

        void cJSONDictProxyImpl::deleteItem(const Field& key) {
            if (!_root)
                throw std::runtime_error("cannot use deleteItem on an uninitialised dictionary");
            
            // delete value from dict
            cJSON_DeleteItemFromObjectCaseSensitive(_root, (const char*)key.desc().c_str());
        }
    }
}
