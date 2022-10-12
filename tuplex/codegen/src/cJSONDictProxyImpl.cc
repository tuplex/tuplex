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
                if (f.getInt() == 0) {
                    cjson_obj = cJSON_CreateFalse();
                } else {
                    cjson_obj = cJSON_CreateTrue();
                }
            } else if (f.getType() == python::Type::F64) {
                cjson_obj = cJSON_CreateNumber(f.getDouble(), 0);
            } else if (f.getType() == python::Type::I64) {
                cjson_obj = cJSON_CreateNumber(f.getInt(), 1);       
            } else if (f.getType() == python::Type::STRING) {
                assert(f.getPtr());
                cjson_obj = cJSON_CreateString((const char*)f.getPtr());
            } else if (f.getType().isListType()) {
                assert(f.getPtr());

                tuplex::List* lis = (tuplex::List*)f.getPtr();
                cjson_obj = cJSON_CreateArray(1);
                
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
                cjson_obj = cJSON_CreateArray(0);
                
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
                throw std::runtime_error("cannot change Field with type " + f.getType().desc() + " into cJSON object");
            }

            return cjson_obj;
        }

        Field cJSONToField(const cJSON* object) {
            assert(object);

            Field ret = Field::null();

            if (cJSON_IsNumber(object)) {
                if (((object->type & ~cJSON_IsReference) & ~cJSON_StringIsConst) == cJSON_Int64) {
                    // type is int, convert ret to int
                    double dbl_val = cJSON_GetNumberValue(object);
                    int64_t int_val = (int64_t) std::round(dbl_val);
                    ret = Field(int_val);
                } else {
                    assert(((object->type & ~cJSON_IsReference) & ~cJSON_StringIsConst) == cJSON_Double);
                    ret = Field(cJSON_GetNumberValue(object));
                }
            } else if (cJSON_IsString(object)) {
                ret = Field(cJSON_GetStringValue(object));
            } else if (cJSON_IsTrue(object)) {
                ret = Field(true);
            } else if (cJSON_IsFalse(object)) {
                ret = Field(false);
            } else if (cJSON_IsNull(object)) {
                ret = Field::null();
            } else if (cJSON_IsArray(object)) {
                std::vector<tuplex::Field> init_vec;
                init_vec.reserve(cJSON_GetArraySize(object));

                for (int i = 0; i < cJSON_GetArraySize(object); i++) {
                    // retrieve ith element from array
                    cJSON* cjson_elt = cJSON_GetArrayItem(object, i);
                    if (!cjson_elt)
                        throw std::runtime_error("could not retrieve element from cJSON array");

                    // convert to field
                    Field field_elt = cJSONToField(cjson_elt);

                    // add element to init vector
                    init_vec.push_back(field_elt);
                }

                if (((object->type & ~cJSON_IsReference) & ~cJSON_StringIsConst) == cJSON_List) {
                    List ret_list = List::from_vector(init_vec);
                    ret = Field(ret_list);
                } else {
                    assert(((object->type & ~cJSON_IsReference) & ~cJSON_StringIsConst) == cJSON_Tuple);
                    Tuple ret_tup = Tuple::from_vector(init_vec);
                    ret = Field(ret_tup);
                }
            } else if (cJSON_IsObject(object)) {
                /** TODO: what type should nested dictionaries
                 *        (i.e. cjson objects) be converted to as a Field? */
                throw std::runtime_error("not yet implemented...");
            }

            return ret;
        }

        std::string cJSONDictProxyImpl::addTypePrefix(std::string key, const python::Type& type) {
            auto ret = type.desc() + "/" + key;

            return ret;
        }

        // general helper function to convert a string into a Field given a python type
        /*!
         * convert a string into a Field given a python type, if not convertible, returns nullptr
         * @param str string
         * @param type python type
         * @return Field object
         */
        Field stringToField(std::string str, python::Type type) {
            if (str.empty())
                throw std::runtime_error("cannot pass in empty string");
            
            Field ret_val = Field::null();
            
            if (type == python::Type::BOOLEAN) {
                if (str.compare("True") == 0) {
                    ret_val = Field(true);
                } else if (str.compare("False") == 0) {
                    ret_val = Field(false);
                } else {
                    throw std::runtime_error("expected bool value, got " + str);
                }
            } else if (type == python::Type::F64) {
                double dbl_val = std::stod(str);
                ret_val = Field(dbl_val);
            } else if (type == python::Type::I64) {
                long long int_val = std::stoll(str);
                ret_val = Field((int64_t)int_val);
            } else if (type == python::Type::STRING) {
                ret_val = Field(str.substr(1, str.length() - 2));
            } else if (type.isListType()) {
                throw std::runtime_error("(list) not yet implemented...");
            } else if (type.isTupleType()) {
                std::vector<tuplex::Field> init_vec;
                init_vec.reserve(type.parameters().size());

                assert(str[0] == '(');
                assert(str[str.length() - 1] == ')');
                int done = 1;
                int curr_index = 0;
                while (done < (str.length() - 1)) {
                    std::string curr_elt = "";
                    python::Type curr_type = type.parameters().at(curr_index);
                    Field field_elt = Field::null();

                    if (str[done] == '\'') {
                        // current item is a string; need to find next '
                        assert(curr_type == python::Type::STRING);

                        size_t next_quote = str.find('\'', done + 1);
                        if (next_quote == std::string::npos)
                            throw std::runtime_error("could not parse tuple string: matching \' not present");

                        curr_elt = str.substr(done + 1, next_quote);
                        assert(str[next_quote + 1] == ',');
                        done = next_quote + 2;
                    } else {
                        size_t next_comma = str.find(',', done);
                        
                        if (next_comma == std::string::npos) {
                            // last element in tuple
                            curr_elt = str.substr(done, str.length() - 1);
                            done = str.length() - 1;
                        } else {
                            curr_elt = str.substr(done, next_comma);
                            done = next_comma + 1;
                        }
                    }

                    field_elt = stringToField(curr_elt, curr_type);
                    if (field_elt == nullptr)
                        throw std::runtime_error("could not parse tuple string: could not convert element into Field");
                        // return nullptr;
                    init_vec.push_back(field_elt);
                    curr_index++;
                }

                assert(type.parameters().size() == curr_index);
                ret_val = Field(Tuple::from_vector(init_vec));
            } else if (type == python::Type::NULLVALUE) {
                ret_val = Field::null();
            } else {
                throw std::runtime_error("conversion from string " + str + " to type " + type.desc() + " not supported");
            }

            return ret_val;
        }

        Field cJSONDictProxyImpl::keyToField(std::string prefixed_key) {
            std::size_t slash_index = prefixed_key.find("/");

            std::string key_type = prefixed_key.substr(0, slash_index);
            std::string key_str = prefixed_key.substr(slash_index + 1);

            python::Type ret_type = python::Type::NULLVALUE;

            if (key_type.substr(0, 4).compare("bool") == 0) {
                ret_type = python::Type::BOOLEAN;
            } else {
                ret_type = python::decodeType(key_type);
            }

            Field ret_val = stringToField(key_str, ret_type);
            if (ret_val.isNull() && (ret_type != python::Type::NULLVALUE))
                throw std::runtime_error("could not convert key-string to Field object");

            return ret_val;
        }

        void cJSONDictProxyImpl::putItem(const Field &key, const Field &value) {
            if(!_root)
                throw std::runtime_error("cannot use putItem on an uninitialised dictionary");
            
            cJSON* to_add = fieldToCJSON(value);
            if (!to_add) {
                throw std::runtime_error("item to add not convertible to cJSON object");
            }

            // check if key already exists
            if (keyExists(key)) {
                // replace existing key
                replaceItem(key, value);
            }

            // add type prefix to key
            std::string prefixed = addTypePrefix(key.desc(), key.getType());

            // key doesn't exist; add to cJSON object
            cJSON_AddItemToObject(_root, prefixed.c_str(), to_add);
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

            // make prefixed key
            std::string prefixed = addTypePrefix(key.desc(), key.getType());
            
            cJSON* res = cJSON_GetObjectItemCaseSensitive(_root, prefixed.c_str());

            if (!res) {
                return false;
            }
            
            return true;
        }

        Field cJSONDictProxyImpl::getItem(const Field& key) {
            if (!_root)
                throw std::runtime_error("cannot use getItem on an uninitialised dictionary");
            
            // make prefixed key
            std::string prefixed = addTypePrefix(key.desc(), key.getType());
            
            // retrieve value from dict
            cJSON* item = cJSON_GetObjectItemCaseSensitive(_root, prefixed.c_str());

            if (!item)
                throw std::runtime_error("error retrieving value from cJSON dictionary");

            // convert into Field
            Field field_item = cJSONToField(item);

            return field_item;
        }

        void cJSONDictProxyImpl::replaceItem(const Field& key, const Field& value) {
            if (!_root)
                throw std::runtime_error("cannot use replaceItem on an uninitialised dictionary");
            
            // make prefixed key
            std::string prefixed = addTypePrefix(key.desc(), key.getType());
            
            // attempt to retrieve value from dict
            cJSON* item = cJSON_GetObjectItemCaseSensitive(_root, prefixed.c_str());

            if (!item) {
                // key doesn't already exist; do putItem instead
                putItem(key, value);
            } else {
                // make new cJSON item
                cJSON* new_item = fieldToCJSON(value);
                if (!new_item) {
                    throw std::runtime_error("new item not convertible to cJSON object");
                }

                cJSON_ReplaceItemInObjectCaseSensitive(_root, prefixed.c_str(), new_item);
            }
        }

        void cJSONDictProxyImpl::deleteItem(const Field& key) {
            if (!_root)
                throw std::runtime_error("cannot use deleteItem on an uninitialised dictionary");
            
            // make prefixed key
            std::string prefixed = addTypePrefix(key.desc(), key.getType());

            // delete value from dict
            cJSON_DeleteItemFromObjectCaseSensitive(_root, prefixed.c_str());
        }

        std::vector<Field> cJSONDictProxyImpl::getKeysView() {
            std::vector<Field> ret;
            ret.reserve(cJSON_GetArraySize(_root));

            cJSON* entry = NULL;
            cJSON_ArrayForEach(entry, _root) {
                // convert key to Field
                std::string key_str = entry->string;
                Field field_val = keyToField(key_str);

                // add to end of ret vector
                ret.push_back(field_val);
            }

            return ret;
        }

        std::vector<Field> cJSONDictProxyImpl::getValuesView() {
            std::vector<Field> ret;
            ret.reserve(cJSON_GetArraySize(_root));

            cJSON* entry = NULL;
            cJSON_ArrayForEach(entry, _root) {
                // convert entry to Field
                Field field_val = cJSONToField(entry);

                // add to end of ret vector
                ret.push_back(field_val);
            }

            return ret;
        }
    }
}
