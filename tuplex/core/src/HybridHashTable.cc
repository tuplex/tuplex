//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <HybridHashTable.h>
#include <bucket.h>

namespace tuplex {
    Py_ssize_t HybridLookupTable::length() {
        // @TODO: implement this properly...
        // i.e. sum of all buckets...
        return 0;
    }

    PyObject* HybridLookupTable::getItem(PyObject *key) {

        assert(valueMode != LookupStorageMode::UNKNOWN);

        assert(sink);

        // nullptr? keyerror
        if(!key) {
            PyErr_SetString(PyExc_KeyError, "could not find key nullptr");
            return nullptr;
        }

        if(hmBucketType == python::Type::UNKNOWN) {
            PyErr_SetString(PyExc_KeyError, "unknown bucket type");
            return nullptr;
        }

        // get type of key => is elementType? => fetch from internal hashmap.
        // else, use python dict
        auto key_type = python::mapPythonClassToTuplexType(key, false);

        PyObject* ret_list = nullptr;

        // None? => return decoded null-bucket
        if(python::Type::NULLVALUE == key_type) {
            assert(sink);
            return decodeBucket(sink->null_bucket, hmBucketType);
        } else if(hmElementType == key_type) {
            // perform hashmap lookup
            if(python::Type::STRING == key_type) {
                char *value = nullptr;
                auto skey = python::PyString_AsString(key);
                // Note: there's a subtle trick here. Because the generated code stores zero-terminated strings,
                // use +1 when searching for the string. Else the keylen check will fail...
                if(sink->hm && MAP_OK == hashmap_get(sink->hm, skey.c_str(), skey.length() + 1, (void**)(&value))) {
                    // value is a bucket now
                    assert(value);
                    // --> fetch list and return
                    ret_list = decodeBucket(reinterpret_cast<uint8_t *>(value), hmBucketType);
                } else {
                    // not in regular dict. Is it in backup dict? I.e. when key type coincides but bucket type is different
                    if(!backupDict) {
                        // key error
                        std::string msg = "could not find key " + python::PyString_AsString(key) + " in tuplex dict";
                        PyErr_SetString(PyExc_KeyError, msg.c_str());
                        return nullptr;
                    }

                    // backupDict valid, perform lookup
                    auto bucket = PyDict_GetItem(backupDict, key);
                    if(!bucket) {
                        // key error
                        std::string msg = "could not find key " + python::PyString_AsString(key) + " in tuplex dict";
                        PyErr_SetString(PyExc_KeyError, msg.c_str());
                        return nullptr;
                    }
                    assert(PyList_Check(bucket));
                    return bucket;
                }
            } else if(python::Type::I64 == key_type) {
                char *value = nullptr;
                auto ikey = PyLong_AsUnsignedLongLong(key);
                if(sink->hm && MAP_OK == int64_hashmap_get(sink->hm, ikey, (void **) (&value))) {
                    // value is a bucket now
                    assert(value);
                    // --> fetch list and return
                    ret_list = decodeBucket(reinterpret_cast<uint8_t *>(value), hmBucketType);
                } else {
                    // not in regular dict. Is it in backup dict? I.e. when key type coincides but bucket type is different
                    if(!backupDict) {
                        // key error
                        std::string msg = "could not find key " + std::to_string(PyLong_AsUnsignedLongLong(key)) + " in tuplex dict";
                        PyErr_SetString(PyExc_KeyError, msg.c_str());
                        return nullptr;
                    }

                    // backupDict valid, perform lookup
                    auto bucket = PyDict_GetItem(backupDict, key);
                    if(!bucket) {
                        // key error
                        std::string msg = "could not find key " + std::to_string(PyLong_AsUnsignedLongLong(key)) + " in tuplex dict";
                        PyErr_SetString(PyExc_KeyError, msg.c_str());
                        return nullptr;
                    }
                    assert(PyList_Check(bucket));
                    return bucket;
                }
            } else throw std::runtime_error("unsupported key type in lookups: " + key_type.desc());
        } else {
            // check with internal python dict!
            // empty? key error
            if(!backupDict) {
                std::string msg = "could neither find key " + python::PyString_AsString(key) + " in tuplex dict nor in backup dict";
                PyErr_SetString(PyExc_KeyError, msg.c_str());
                return nullptr;
            }

            // check in pure python mode
            return PyObject_GetItem(backupDict, key);
        }

        // need to fetch entry from backup too.
        if(backupDict) {
            if(valueMode == LookupStorageMode::LISTOFVALUES) {
                auto backup_list = PyDict_GetItem(backupDict, key);
                if(backup_list)
                    ret_list = PySequence_Concat(ret_list, backup_list); // concat lists!
            } else {
                return PyDict_GetItem(backupDict, key);
            }
        }

        // list empty?
        assert(ret_list);
        if(valueMode == LookupStorageMode::LISTOFVALUES && PyList_Size(ret_list) == 0) {
            Py_DECREF(ret_list);
            // raise keyerror if not found!
            PyErr_SetString(PyExc_KeyError, ("could not find key " + python::PyString_AsString(key)).c_str());
            return nullptr;
        } else {
            return ret_list;
        }
    }

    PyObject *HybridLookupTable::decodeBucket(const uint8_t *bucket, const python::Type &bucketType) {
        if(!bucket)
            return nullptr;

        switch(valueMode) {
            case LookupStorageMode::VALUE: {
                // value size & decode
                auto value_size = *(uint64_t*)bucket;
                auto value_buf = bucket + sizeof(int64_t);
                Row row = Row::fromMemory(Schema(Schema::MemoryLayout::ROW,
                                                 python::Type::propagateToTupleType(bucketType)), value_buf, value_size);
                return python::rowToPython(row, true);
                break;
            }
            case LookupStorageMode::LISTOFVALUES: {
                return decodeBucketToPythonList(bucket, bucketType);
                break;
            }
            default:
                return nullptr;
        }
    }

    PyObject* wrapValueAsRow(PyObject* o) {
        // is tuple? nothing todo.
        if(PyTuple_Check(o) != 0 && PyTuple_Size(o) > 1)
            return o; // nothing todo, empty tuple will get wrapped
        auto t = PyTuple_New(1);
        PyTuple_SET_ITEM(t, 0, o);
        return t;
    }

    PyObject* unwrapRow(PyObject *o) {
        if(!o)
            return nullptr;

        // internal row class?
        if(0 == strcmp(o->ob_type->tp_name, "Row")) {
           // check how many elements, if 1 unwrap!
           auto data = PyObject_GetAttrString(o, "data");
           auto seq_len = PySequence_Length(data);
           if(1 == seq_len) {
               if(PyTuple_Check(data)) {
                   auto item = PyTuple_GetItem(data, 0);
                   Py_XINCREF(item);
                   return item;
               } else if(PyList_Check(data)) {
                   auto item = PyList_GetItem(data, 0);
                   Py_XINCREF(item);
                   return item;
               } else {
                   std::cerr<<"internal error while attempting to unwrap row object"<<std::endl;
               }
           }
       }

        return o;
    }

    size_t HybridLookupTable::backupItemCount() const {
        if(!backupDict)
            return 0;
        return PyObject_Length(backupDict);
    }

    int HybridLookupTable::putKey(PyObject *key) {
        assert(sink);

        assert(valueMode != LookupStorageMode::UNKNOWN);

        // make sure value type is null or UNKNOWN
        if(!(hmBucketType == python::Type::UNKNOWN || hmBucketType == python::Type::NULLVALUE)) {
            PyErr_SetString(PyExc_KeyError, "using hybrid hash table likely for unique, yet bucket type is set. Wrong internal typing?");
            return -1;
        }

        auto key_type = python::mapPythonClassToTuplexType(key, false);

        // could be direct key_type == hmElementType comparison,
        // yet, let's be lazy so objects can be properly extracted...

        // special case: null bucket
        if (key_type == python::Type::NULLVALUE) {
            sink->null_bucket = extend_bucket(sink->null_bucket, nullptr, 0);
            return 0;
        }

        // can upcast? do so!
        if(key_type != hmElementType && python::canUpcastToRowType(key_type, hmElementType)) {
            // upcast python object!
            // -> requires decoding and encoding!
            std::cerr<<"unsupported, auto upcasting in fallback object"<<std::endl;
            // NOT YET IMPLEMENTED...
        }

        if(key_type == hmElementType) { // regular case
            // simply insert into hashmap (lazy create)
            if (!sink->hm)
                sink->hm = hashmap_new();
            if (key_type != python::Type::I64 && key_type != python::Type::STRING &&
                key_type != python::Type::NULLVALUE) {
                PyErr_SetString(PyExc_KeyError, "only i64, string or None as keys yet supported");
                return -1;
            }

            if (key_type == python::Type::STRING) {
                // regular, key bucket
                auto key_str = python::PyString_AsString(key);

                if (!sink->hm)
                    sink->hm = hashmap_new();

                // check whether bucket exists, if not set. Else, update
                uint8_t *bucket = nullptr;
                hashmap_get(sink->hm, key_str.c_str(), key_str.length() + 1, (void **) (&bucket));

                // update or new entry
                bucket = extend_bucket(bucket, nullptr, 0);
                // Note the +1 to get the '\0' char as well!
                hashmap_put(sink->hm, key_str.c_str(), key_str.length() + 1, bucket);
            } else if (key_type == python::Type::I64) {
                // regular, key bucket
                auto key_int = PyLong_AsUnsignedLongLong(key);

                if (!sink->hm)
                    sink->hm = int64_hashmap_new();

                // check whether bucket exists, if not set. Else, update
                uint8_t *bucket = nullptr;
                int64_hashmap_get(sink->hm, key_int, (void **) (&bucket));

                // update or new entry
                bucket = extend_bucket(bucket, nullptr, 0);
                int64_hashmap_put(sink->hm, key_int, bucket);
            }
        } else {
            // fallback:
            if(!backupDict)
                backupDict = PyDict_New();

            // check whether element already exists, if not add new list
            // else append
            auto bucket = PyDict_GetItem(backupDict, key);
            if(!bucket) {
                bucket = PyLong_FromLong(1);
                return PyDict_SetItem(backupDict, key, bucket);
            } else {
                // append to bucket
                PyNumber_Add(bucket, PyLong_FromLong(1));
                return PyDict_SetItem(backupDict, key, bucket);
            }
            if(PyErr_Occurred())
                return -1;
            return 0;
        }
        return 0;
    }

    int HybridLookupTable::putItem(PyObject *key, PyObject *value) {
        using namespace tuplex;

        assert(valueMode != LookupStorageMode::UNKNOWN);

        if(!value && key)
            return putKey(key);

        // return -1 + set exception on failure!
        if(!key || !value) {
            // no del here supported!
            PyErr_SetString(PyExc_KeyError, "key and value must be non-null");
            return -1;
        }

        // special case: is class of value the dummy Row class created within the code?
        // => unpack or keep as tuple!
        value = unwrapRow(value);

        // decoce types of both key and val
        auto key_type = python::mapPythonClassToTuplexType(key, false);
        auto val_type = python::mapPythonClassToTuplexType(value, false);

        // @TODO: upcasting b.c. of NVO!

        // match of internal dict? -> else use backup dict
        if((key_type == hmElementType || key_type == python::Type::NULLVALUE)
            && val_type == hmBucketType) {
            // simply insert into hashmap (lazy create)
            if(!sink->hm)
                sink->hm = hashmap_new();
            if(key_type != python::Type::I64
               && key_type != python::Type::STRING
               && key_type != python::Type::NULLVALUE) {
                PyErr_SetString(PyExc_KeyError, "only i64, string or None as keys yet supported");
                return -1;
            }

            // serialize content of value
            auto bucket_type = this->hmBucketType;
            auto bucket_row = python::pythonToRow(value, bucket_type, false);
            // serialize to buffer
            auto buf_length = bucket_row.serializedLength();
            auto buf = new uint8_t [buf_length + 32]; // some security bytes
#ifdef NDEBUG
            memset(buf, 0, buf_length);
#endif
            bucket_row.serializeToMemory(buf, buf_length);

            switch(valueMode) {
                case LookupStorageMode::VALUE: {
                    // store the value as is (in serialized form)

                    // create buf with 8 byte length field for value
                    uint8_t* value_buf = (uint8_t*)malloc(buf_length + sizeof(int64_t));
                    if(!value_buf) {
                        PyErr_SetString(PyExc_RuntimeError, "allocated null buffer for hybrid.");
                    }
                    *((int64_t*)value_buf) = buf_length;
                    memcpy(value_buf + sizeof(int64_t), buf, buf_length);

                    // special case: null bucket
                    if(key_type == python::Type::NULLVALUE) {
                        if(sink->null_bucket)
                            ::free(sink->null_bucket);
                        sink->null_bucket = value_buf;
                    } else if(key_type == python::Type::STRING) {
                        // regular, key bucket
                        auto key_str = python::PyString_AsString(key);

                        if(!sink->hm)
                            sink->hm = hashmap_new();

                        // check whether bucket exists, if not set. Else, update
                        uint8_t *bucket = nullptr;
                        hashmap_get(sink->hm, key_str.c_str(), key_str.length() + 1, (void**)(&bucket));

                        // update or new entry
                        if(bucket)
                            ::free(bucket);
                        bucket = value_buf;

                        // Note the +1 to get the '\0' char as well!
                        hashmap_put(sink->hm, key_str.c_str(), key_str.length() + 1, bucket);
                    } else if(key_type == python::Type::I64) {
                        // regular, key bucket
                        auto key_int = PyLong_AsUnsignedLongLong(key);

                        if(!sink->hm)
                            sink->hm = int64_hashmap_new();


                        // check whether bucket exists, if not set. Else, update
                        uint8_t *bucket = nullptr;
                        int64_hashmap_get(sink->hm, key_int, (void **) (&bucket));

                        // update or new entry
                        if(bucket)
                            ::free(bucket);
                        bucket = value_buf;
                        int64_hashmap_put(sink->hm, key_int, bucket);
                    }
                    delete [] buf;
                    break;
                }
                case LookupStorageMode::LISTOFVALUES: {

                    // store as list (i.e. extend bucket)
                    // special case: null bucket
                    if(key_type == python::Type::NULLVALUE) {
                        sink->null_bucket = extend_bucket(sink->null_bucket, buf, buf_length);
                    } else if(key_type == python::Type::STRING) {
                        // regular, key bucket
                        auto key_str = python::PyString_AsString(key);

                        if(!sink->hm)
                            sink->hm = hashmap_new();

                        // check whether bucket exists, if not set. Else, update
                        uint8_t *bucket = nullptr;
                        hashmap_get(sink->hm, key_str.c_str(), key_str.length() + 1, (void**)(&bucket));

                        // update or new entry
                        bucket = extend_bucket(bucket, reinterpret_cast<uint8_t*>(buf), buf_length);
                        // Note the +1 to get the '\0' char as well!
                        hashmap_put(sink->hm, key_str.c_str(), key_str.length() + 1, bucket);
                    } else if(key_type == python::Type::I64) {
                        // regular, key bucket
                        auto key_int = PyLong_AsUnsignedLongLong(key);

                        if(!sink->hm)
                            sink->hm = int64_hashmap_new();


                        // check whether bucket exists, if not set. Else, update
                        uint8_t *bucket = nullptr;
                        int64_hashmap_get(sink->hm, key_int, (void **) (&bucket));

                        // update or new entry
                        bucket = extend_bucket(bucket, reinterpret_cast<uint8_t*>(buf), buf_length);
                        int64_hashmap_put(sink->hm, key_int, bucket);
                    }
                    delete [] buf;
                    break;
                }
                default:
                    PyErr_SetString(PyExc_ValueError, "undefined storage mode encountered");
                    delete [] buf;
                    return -1;
            }
        } else {
            if(!backupDict)
                backupDict = PyDict_New();

            switch(valueMode) {
                case LookupStorageMode::VALUE: {
                    // simple, just update value (wrap as row)
                    return PyDict_SetItem(backupDict, key, wrapValueAsRow(value));
                }
                case LookupStorageMode::LISTOFVALUES: {
                    // check whether element already exists, if not add new list
                    // else append
                    auto bucket = PyDict_GetItem(backupDict, key);
                    if(!bucket) {
                        bucket = PyList_New(1);
                        PyList_SetItem(bucket, 0, wrapValueAsRow(value));
                        return PyDict_SetItem(backupDict, key, bucket);
                    } else {
                        // append to bucket
                        PyList_Append(bucket, wrapValueAsRow(value));
                        return PyDict_SetItem(backupDict, key, bucket);
                    }
                }
                default:
                    PyErr_SetString(PyExc_ValueError, "undefined storage mode encountered");
                    return -1;
            }

            return 0;
        }

        return 0;
    }

    bool HybridLookupTable::_key_exists(PyObject *key) {
        if(!key) {
            PyErr_SetString(PyExc_KeyError, "could not find key nullptr in setdefault");
            return false;
        }

        // check if key exists in backup dict
        if(backupDict) {
            auto item = PyObject_GetItem(backupDict, key);
            if(item)
                return true;
        }

        // if not, check in C++ hashmap.
        if(hmBucketType == python::Type::UNKNOWN) {
            PyErr_SetString(PyExc_KeyError, "unknown bucket type");
            return false;
        }

        auto key_type = python::mapPythonClassToTuplexType(key, false);

        // None? -> check null bucket
        if(python::Type::NULLVALUE == key_type) {
            assert(sink);
            // null bucket full?
            if(!sink->null_bucket)
                return false;
            // no elements?
            uint64_t info = *(const uint64_t*)sink->null_bucket;
            auto num_elements = info >> 32ul;
            return 0 != num_elements;
        } else if(hmElementType == key_type) {
            if(python::Type::STRING == key_type) {
                char *value = nullptr;
                auto skey = python::PyString_AsString(key);
                if(sink->hm && MAP_OK == hashmap_get(sink->hm, skey.c_str(), skey.length() + 1, (void**)(&value))) {
                    uint8_t* bucket = reinterpret_cast<uint8_t*>(value);
                    if(!bucket)
                        return false;
                    return elements_in_bucket(bucket) != 0;
                } else {
                    // not in regular dict
                    if(!backupDict)
                        return false;
                    auto bucket = PyDict_GetItem(backupDict, key);
                    return bucket;
                }
            } else if(python::Type::I64 == key_type) {
                char *value = nullptr;
                auto ikey = PyLong_AsUnsignedLongLong(key);
                if(sink->hm && MAP_OK == int64_hashmap_get(sink->hm, ikey, (void**)(&value))) {
                    uint8_t* bucket = reinterpret_cast<uint8_t*>(value);
                    if(!bucket)
                        return false;
                    return elements_in_bucket(bucket) != 0;
                } else {
                    if(!backupDict)
                        return false;
                    auto bucket = PyDict_GetItem(backupDict, key);
                    return bucket;
                }
            } else
                throw std::runtime_error("unsupported key type in lookup " + key_type.desc());
        }

        return false;
    }

    PyObject *HybridLookupTable::setDefault(PyObject *key, PyObject *value) {
        assert(valueMode != LookupStorageMode::UNKNOWN);

        // debug
        {
            Py_XINCREF(key);
            auto skey = python::PyString_AsString(key);
            std::cout<<"setdefault w. key="<<skey<<std::endl;
        }

        // check if item exists, if so return. Else, set to default value!
        Py_XINCREF(key);
        Py_XINCREF(value);
        if(!_key_exists(key)) {
            PyErr_Clear(); // clear key error
            putItem(key, value);
            return value;
        } else {
            //Py_XDECREF(value); // ??
            return getItem(key);
        }
    }


    PyObject* decodeBucketToPythonList(const uint8_t* bucket, const python::Type& bucketType) {
        using namespace tuplex;

        if(!bucket) {
            auto L = PyList_New(0);
            return L;
        }

        Deserializer ds(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(bucketType)));

        // how many rows?
        // extract size, num rows etc. and merge
        uint64_t info = *(const uint64_t*)bucket;
        auto bucket_size = info & 0xFFFFFFFF;
        auto num_elements = (info >> 32ul);

        // go over elements, first get size then contents!
        auto ptr = bucket + sizeof(int64_t);
        auto L = PyList_New(num_elements);
        for(auto i = 0; i < num_elements; ++i) {
            auto row_size = *(const uint32_t*)ptr;
            ptr += sizeof(int32_t);
            auto r = Row::fromMemory(ds, ptr, row_size);
            ptr += row_size;
            auto obj = python::rowToPython(r);
            if(!obj)
                PyList_SET_ITEM(L, i, python::none());
            else
                PyList_SET_ITEM(L, i, obj);
        }

        return L;
    }




    // helper function to create the object and associate with a hashmap
    HybridLookupTable* CreatePythonHashMapWrapper(HashTableSink& sink, const python::Type& elementType,
                                                  const python::Type& bucketType, const LookupStorageMode& valueMode) {
        assert(sink.hm); // this needs to exist.

        assert(elementType != python::Type::UNKNOWN);
        if(elementType.isOptionType()) {
            throw std::runtime_error("element type needs to be a non-option type!");
        }

        // lazy init type
        if(InternalHybridTableType.tp_dict == nullptr) {
            if(PyType_Ready(&InternalHybridTableType) < 0)
                throw std::runtime_error("initializing internal hybrid table type failed");
            Py_INCREF(&InternalHybridTableType);
            assert(InternalHybridTableType.tp_dict);

            // should we register type as well with main module?
        }

        Py_INCREF(&InternalHybridTableType);
        auto o = (HybridLookupTable*)PyType_GenericNew(&InternalHybridTableType, nullptr, nullptr);
        if(!o) {
            Py_DECREF(&InternalHybridTableType);
            return nullptr;
        }

        // assign hashtable sink + type
        o->hmElementType = elementType;
        o->hmBucketType = bucketType;
        o->backupDict = nullptr;
        o->sink = &sink;
        o->valueMode = valueMode;

        // vice versa, make hybrid based on newly allocated object
        sink.hybrid_hm = (PyObject*)o;

        return o;
    }

    size_t HybridLookupTable::elements_in_bucket(const uint8_t *bucket) {
        if(!bucket)
            return 0;

        // check depending on storage mode (list of value?)
        switch(valueMode) {
            case LookupStorageMode::VALUE: {
                // if size != 0 => then value is stored.
                uint64_t value_size = *(const uint64_t*)bucket;

                // value_size should be != 0.

                return 1;
            }
            case LookupStorageMode::LISTOFVALUES: {
                uint64_t info = *(const uint64_t*)bucket;
                auto bucket_size = info & 0xFFFFFFFF;
                auto num_elements = (info >> 32ul);
                return num_elements;
            }

            default: {
                return 0;
            }
        }
    }

    PyObject* HybridLookupTable::pythonDict(bool remove) {
        if(remove) {
            auto dict = backupDict;
            backupDict = nullptr;
            return dict;
        } else {
            Py_XINCREF(backupDict);
            return backupDict;
        }
    }

    void HybridLookupTable::update(PyObject* dictObject) {
        assert(valueMode != LookupStorageMode::UNKNOWN);

        assert(dictObject && PyDict_Check(dictObject));

        // iterate and insert using putItem!
        PyObject *key = nullptr, *val = nullptr;
        Py_ssize_t pos = 0;
        while (PyDict_Next(dictObject, &pos, &key, &val)) {
           Py_XINCREF(key);
           Py_XINCREF(val);
           putItem(key, val);
        }
    }

    void HybridLookupTable::free() {
        // release pyobject and also internal hashmap
        if(sink && sink->hm) {
            if(hmElementType == python::Type::STRING) {
                hashmap_free_key_and_data(sink->hm);
            } else {
                assert(hmElementType == python::Type::I64);
                int64_hashmap_free_key_and_data(sink->hm);
            }
            sink->hm = nullptr;
        }

        if(backupDict) {
            Py_XDECREF(backupDict);
            backupDict = nullptr;
        }

        // clear sink
        if(sink) {
            sink->hm = nullptr;
            sink->null_bucket = nullptr;
            sink->hybrid_hm = nullptr;
        }

        valueMode = LookupStorageMode::UNKNOWN;
        hmElementType = python::Type::UNKNOWN;
        hmBucketType = python::Type::UNKNOWN;
    }
}