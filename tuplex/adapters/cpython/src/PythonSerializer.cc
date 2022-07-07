//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Field.h>
#include <TupleTree.h>

#include "PythonSerializer.h"
#include "PythonSerializer_private.h"
#include "PythonHelpers.h"

namespace tuplex {
    namespace cpython {
        PyObject *PyObj_FromCJSONKey(const char *serializedKey) {
            assert(serializedKey);
            assert(strlen(serializedKey) >= 2);
            const char *keyString = serializedKey + 2;
            switch (serializedKey[0]) {
                case 's':
                    return PyUnicode_DecodeUTF8(keyString, strlen(keyString), nullptr);
                case 'b':
                    if (strcmp(keyString, "True") == 0)
                        return PyBool_FromLong(1);
                    if (strcmp(keyString, "False") == 0)
                        return PyBool_FromLong(0);
                    Logger::instance().defaultLogger().error(
                            "invalid boolean key: " + std::string(keyString) + ", returning Py_None");
                    Py_XINCREF(Py_None);
                    return Py_None;
                case 'i':
                    return PyLong_FromString(keyString, nullptr, 10);
                case 'f':
                    return PyFloat_FromDouble(strtod(keyString, nullptr));
                default:
                    Logger::instance().defaultLogger().error("unknown type " + std::string(serializedKey)
                                                             + " in field encountered. Returning Py_None");
                    Py_XINCREF(Py_None);
                    return Py_None;
            }
        }

        PyObject *PyObj_FromCJSONVal(const cJSON *obj, const char type) {
            switch (type) {
                case 's':
                    return PyUnicode_DecodeUTF8(obj->valuestring, strlen(obj->valuestring), nullptr);
                case 'b':
                    return cJSON_IsTrue(obj) ? PyBool_FromLong(1) : PyBool_FromLong(0);
                case 'i':
                    return PyLong_FromDouble(obj->valuedouble);
                case 'f':
                    return PyFloat_FromDouble(obj->valuedouble);
                default:
                    std::string errorString =
                            "unknown type identifier" + std::string(&type) + " in field encountered. Returning Py_None";
                    Logger::instance().defaultLogger().error(errorString);
                    Py_XINCREF(Py_None);
                    return Py_None;
            }
        }

        PyObject *PyDict_FromCJSON(const cJSON *dict) {
            auto dictObj = PyDict_New();
            cJSON *cur_item = dict->child;
            while (cur_item) {
                char *key = cur_item->string;
                auto keyObj = PyObj_FromCJSONKey(key);
                auto valObj = PyObj_FromCJSONVal(cur_item, key[1]);
                PyDict_SetItem(dictObj, keyObj, valObj);
                cur_item = cur_item->next;
            }
            return dictObj;
        }

        PyObject *createPyTupleFromMemory(const uint8_t *ptr, const python::Type &row_type, uintptr_t max_ptr) {
            int64_t current_buffer_index = 0;

            auto tree = tuplex::TupleTree<tuplex::Field>(row_type);
            PyObject *obj = PyTuple_New(row_type.parameters().size());
            std::vector<PyObject *> obj_stack;
            PyObject *curr_obj = obj;

            std::vector<int> curr;
            std::vector<int> prev;

            unsigned bitmap_index = 0;
            const uint8_t *bitmap = ptr;
            auto num_bitmap_fields = core::ceilToMultiple(python::numOptionalFields(row_type), 64ul)/64;
            ptr += sizeof(int64_t) * num_bitmap_fields;

            for (const std::vector<int> &index: tree.getMultiIndices()) {
                curr = index;

                int position_diff = -1;
                if (prev.empty()) {
                    position_diff = 0;
                } else {
                    for (int i = 0; i < std::min(prev.size(), curr.size()); i++) {
                        if (prev[i] != curr[i]) {
                            position_diff = i;
                            break;
                        }
                    }
                }
                if (position_diff != -1) {
                    auto num_elem_to_pop = obj_stack.size() - position_diff;
                    for (int i = 0; i < num_elem_to_pop; i++) {
                        curr_obj = obj_stack.back();
                        obj_stack.pop_back();
                    }
                    std::vector<int> curr_index_stack;
                    for (int i = 0; i < position_diff; i++) {
                        curr_index_stack.push_back(curr[i]);
                    }
                    for (int i = position_diff; i < curr.size() - 1; i++) {
                        int curr_index_value = curr[i];
                        curr_index_stack.push_back(curr_index_value);
                        PyObject *new_tuple = PyTuple_New(tree.fieldType(curr_index_stack).parameters().size());
                        PyTuple_SetItem(curr_obj, curr_index_value, new_tuple);
                        obj_stack.push_back(curr_obj);
                        curr_obj = new_tuple;
                    }
                }

                int curr_index = curr.back();
                python::Type current_type = tree.fieldType(curr);
                PyObject *elem_to_insert = nullptr;
                if (current_type.isOptionType() && current_type.getReturnType().isTupleType()) {
                    // createPyTupleFromMemory requires a ptr to start of the actual tuple data, so need to decode and add offset here
                    uint64_t offset = *((uint64_t *)(ptr + current_buffer_index));
                    assert((uintptr_t)(ptr + current_buffer_index + offset) <= max_ptr);
                    elem_to_insert = createPyObjectFromMemory(ptr + current_buffer_index + offset, current_type,
                                                              max_ptr, bitmap, bitmap_index);
                } else {
                    // otherwise, simply pass ptr to the current field
                    elem_to_insert = createPyObjectFromMemory(ptr + current_buffer_index, current_type, max_ptr,
                                                              bitmap, bitmap_index);
                }

                if(current_type.isOptionType()) bitmap_index++;
                if (elem_to_insert == nullptr) {
                    return nullptr;
                }

                PyTuple_SetItem(curr_obj, curr_index, elem_to_insert);
                if (!current_type.withoutOptions().isSingleValued())
                    current_buffer_index += SIZEOF_LONG;

                prev = curr;
            }

            return obj;
        }

        PyObject *createPyDictFromMemory(const uint8_t *ptr) {
#ifndef NDEBUG
            if(!ptr)
                throw std::runtime_error("empty pointer, can't create dict");
#endif

            // access the field element using Tuplex's serialization format.
            auto elem = *(uint64_t *) (ptr);
            auto offset = (uint32_t) elem;
            auto length = (uint32_t) (elem >> 32ul);

            auto cjson_str = reinterpret_cast<const char *>(&ptr[offset]);
            cJSON *dict = cJSON_Parse(cjson_str);
#ifndef NDEBUG
            if(!dict)
                throw std::runtime_error("could not parse string " + std::string(cjson_str));
#endif
            auto test = PyDict_FromCJSON(dict);
            return test;
        }

        PyObject *createPyListFromMemory(const uint8_t *ptr, const python::Type &row_type, uintptr_t max_ptr) {
            assert(row_type.isListType() && row_type != python::Type::EMPTYLIST);
            auto elementType = row_type.elementType();
            if(elementType.isSingleValued()) {
                auto numElements = *(int64_t*)ptr;
                auto ret = PyList_New(numElements);
                for(size_t i=0; i<numElements; i++) {
                    PyObject* element;
                    if(elementType == python::Type::NULLVALUE) {
                        element = Py_None;
                        Py_XINCREF(Py_None);
                    } else if(elementType == python::Type::EMPTYDICT) {
                        element = PyDict_New();
                    } else if(elementType == python::Type::EMPTYTUPLE) {
                        element = PyTuple_New(0);
                    } else if(elementType == python::Type::EMPTYLIST) {
                        element = PyList_New(0);
                    } else {
                        throw std::runtime_error("Invalid list type: " + row_type.desc());
                    }
                    PyList_SET_ITEM(ret, i, element);
                }
                return ret;
            } else {
                // access the field element
                auto elem = *(uint64_t *) ptr;
                auto offset = (uint32_t) elem;

                // move to varlen field
                assert((uintptr_t)(ptr + offset) <= max_ptr);
                ptr = &ptr[offset];

                // get number of elements
                auto numElements = *(int64_t*)ptr;
                ptr += sizeof(int64_t);
                auto ret = PyList_New(numElements);

                // get bitmap
                std::vector<bool> bitmapV = getBitmapFromType(row_type, ptr, numElements);

                for(size_t i=0; i<numElements; i++) {
                    PyObject* element;
                    if(elementType == python::Type::I64) {
                        element = PyLong_FromLong(*reinterpret_cast<const int64_t*>(ptr));
                        ptr += sizeof(int64_t);
                    } else if(elementType == python::Type::F64) {
                        element = PyFloat_FromDouble(*reinterpret_cast<const double*>(ptr));
                        ptr += sizeof(int64_t);
                    } else if(elementType == python::Type::BOOLEAN) {
                        element = PyBool_FromLong(*reinterpret_cast<const int64_t*>(ptr));
                        ptr += sizeof(int64_t);
                    } else if (elementType == python::Type::STRING) {
                        char *string_errors = nullptr;
                        // get offset for string
                        auto currOffset = *reinterpret_cast<const uint64_t *>(ptr);
                        assert((uintptr_t)(ptr + currOffset) <= max_ptr);
                        auto currStr = reinterpret_cast<const char*>(&ptr[currOffset]);
                        element = PyUnicode_DecodeUTF8(currStr, (long)(strlen(currStr)), string_errors);
                        ptr += sizeof(int64_t);
                    } else if(elementType.isTupleType()) {
                        auto currOffset = *(uint64_t *)ptr;
                        assert((uintptr_t)(ptr + currOffset) <= max_ptr);
                        element = createPyTupleFromMemory(ptr + currOffset, elementType, max_ptr);
                        ptr += sizeof(int64_t);
                    } else if(elementType.isListType()) {
                        element = createPyListFromMemory(ptr, elementType, max_ptr);
                        ptr += sizeof(int64_t);
                    } else if(elementType.isDictionaryType()) {
                        element = createPyDictFromMemory(ptr);
                        ptr += sizeof(int64_t);
                    } else if(elementType.isOptionType()) {
                        auto underlyingType = elementType.getReturnType();
                        if(underlyingType == python::Type::BOOLEAN) {
                            if(bitmapV[i]) {
                                Py_XINCREF(Py_None);
                                element = Py_None;
                            } else {
                                element = PyBool_FromLong(*reinterpret_cast<const int64_t*>(ptr));
                                ptr += sizeof(int64_t);
                            }
                        } else if(underlyingType == python::Type::I64) {
                            if(bitmapV[i]) {
                                Py_XINCREF(Py_None);
                                element = Py_None;
                            } else {
                                element = PyLong_FromLong(*reinterpret_cast<const int64_t*>(ptr));
                                ptr += sizeof(int64_t);
                            }
                        } else if(underlyingType == python::Type::F64) {
                            if(bitmapV[i]) {
                                Py_XINCREF(Py_None);
                                element = Py_None;
                            } else {
                                element = PyFloat_FromDouble(*reinterpret_cast<const double*>(ptr));
                                ptr += sizeof(int64_t);
                            }
                        } else if(underlyingType == python::Type::STRING) {
                            if(bitmapV[i]) {
                                Py_XINCREF(Py_None);
                                element = Py_None;
                            } else {
                                char *string_errors = nullptr;
                                // get offset for string
                                auto currOffset = *reinterpret_cast<const uint64_t *>(ptr);
                                assert((uintptr_t)(ptr + currOffset) <= max_ptr);
                                auto currStr = reinterpret_cast<const char*>(&ptr[currOffset]);
                                element = PyUnicode_DecodeUTF8(currStr, (long)(strlen(currStr)), string_errors);
                                ptr += sizeof(int64_t);
                            }
                        } else if(underlyingType.isListType()) {
                            if(bitmapV[i]) {
                                Py_XINCREF(Py_None);
                                element = Py_None;
                            } else {
                                element = createPyListFromMemory(ptr, underlyingType, max_ptr);
                                ptr += sizeof(int64_t);
                            }
                        } else if(underlyingType.isTupleType()) {
                            if(bitmapV[i]) {
                                Py_XINCREF(Py_None);
                                element = Py_None;
                            } else {
                                uint64_t currOffset = *((uint64_t *)(ptr));
                                assert((uintptr_t)(ptr + currOffset) <= max_ptr);
                                element = createPyTupleFromMemory(ptr + currOffset, underlyingType, max_ptr);
                                ptr += sizeof(int64_t);
                            }
                        } else throw std::runtime_error("Invalid list type: " + row_type.desc());
                    } else throw std::runtime_error("Invalid list type: " + row_type.desc());
                    PyList_SET_ITEM(ret, i, element);
                }
                return ret;
            }
        }

        PyObject *
        createPyObjectFromMemory(const uint8_t *ptr, const python::Type &row_type, uintptr_t max_ptr,
                                 const uint8_t *bitmap, unsigned index) {
            if (row_type == python::Type::BOOLEAN) {
                return PyBool_FromLong(ptr[0]);
            } else if (row_type == python::Type::I64) {
                return PyLong_FromLong(*(int64_t *) (ptr));
            } else if (row_type == python::Type::F64) {
                return PyFloat_FromDouble(*(double *) (ptr));
            } else if (row_type == python::Type::STRING) {
                auto elem = *(uint64_t *) (ptr);
                auto offset = (uint32_t) elem;
                auto length = (uint32_t) (elem >> 32ul);
                assert((uintptr_t)(ptr + offset + length) <= max_ptr);
                auto str = reinterpret_cast<const char *>(&ptr[offset]);
                char *string_errors = nullptr;
                return PyUnicode_DecodeUTF8(str, length - 1, string_errors);
            } else if (row_type == python::Type::EMPTYTUPLE) {
                return PyTuple_New(0);
            } else if (row_type.isTupleType()) {
                return createPyTupleFromMemory(ptr, row_type, max_ptr);
            } else if (row_type == python::Type::EMPTYDICT) {
                return PyDict_New();
            } else if (row_type.isDictionaryType() || row_type == python::Type::GENERICDICT) {
                return createPyDictFromMemory(ptr);
            } else if(row_type == python::Type::EMPTYLIST) {
                return PyList_New(0);
            } else if(row_type.isListType()) {
                return createPyListFromMemory(ptr, row_type, max_ptr);
            } else if(row_type.isOptionType()) { // TODO: should this be [isOptional()]?
                bool singleValue = false;
                if(!bitmap) {
                    // If bitmap was null, this means that it was a single value, not part of a tuple
                    singleValue = true;
                    bitmap = ptr;
                    index = 0;
                    ptr += (sizeof(uint64_t)/sizeof(uint8_t));
                }
                bool is_null = bitmap[index/64] & (1UL << (index % 64));
                if(is_null) {
                    Py_XINCREF(Py_None);
                    return Py_None;
                }

                auto t = row_type.getReturnType();
                if (t.isTupleType() && singleValue) {
                    // offset exists
                    uint64_t sizeOffset = *((uint64_t *)ptr);
                    uint64_t offset = sizeOffset & 0xFFFFFFFF;
                    assert((uintptr_t)(ptr + offset) <= max_ptr);
                    ptr += offset;
                }
                return createPyObjectFromMemory(ptr, t, max_ptr);
            } else if(row_type == python::Type::PYOBJECT) {
                // cloudpickle, deserialize
                auto elem = *(uint64_t *) (ptr);
                auto offset = (uint32_t) elem;
                auto buf_size = (uint32_t) (elem >> 32ul);
                assert((uintptr_t)(ptr + offset + buf_size) <= max_ptr);
                auto buf = reinterpret_cast<const char *>(&ptr[offset]);
                return python::deserializePickledObject(python::getMainModule(), buf, buf_size);
            } else {
#ifndef NDEBUG
                Logger::instance().logger("serializer").debug("unknown type '" + row_type.desc() + "' encountered, replacing with None.");
#endif
            }
            Py_XINCREF(Py_None);
            return Py_None;
        }

        int64_t checkTupleCapacity(const uint8_t *ptr, size_t capacity, const python::Type &row_type) {
            auto tree = tuplex::TupleTree<tuplex::Field>(row_type);
            auto indices = tree.getMultiIndices();
            auto num_bytes = static_cast<int64_t>(indices.size() * sizeof(int64_t));
            Logger::instance().logger("python").debug("checkTupleCapacity num_bytes: " + std::to_string(num_bytes));
            if (num_bytes > capacity) {
                return -1;
            }

            // check that varlen field doesn't overflow the capacity
            auto num_bitmap_fields = core::ceilToMultiple(python::numOptionalFields(row_type), 64ul)/64;
            auto varlen_field_length = *((int64_t*)(ptr + sizeof(int64_t) * num_bitmap_fields + num_bytes));
            Logger::instance().logger("python").debug("checkTupleCapacity num_bitmap_fields: " + std::to_string(num_bitmap_fields));
            Logger::instance().logger("python").debug("checkTupleCapacity varlen_field_length: " + std::to_string(varlen_field_length));
            if(sizeof(int64_t) * num_bitmap_fields + num_bytes + varlen_field_length > capacity) {
                return -1;
            }

            return num_bytes;
        }

        int64_t serializationSize(const uint8_t *ptr, size_t capacity, const python::Type &row_type) {

            // should be identical to Deserializer.inferlength...

            // handle special empty cases
            if (row_type.isSingleValued())
                return 0;

            auto num_bitmap_fields = core::ceilToMultiple(python::numOptionalFields(row_type), 64ul) / 64;
            // option[()], option[{}] only have bitmap
            if (row_type.isOptionType() && row_type.getReturnType().isSingleValued())
                return sizeof(int64_t) * num_bitmap_fields;

            // move ptr by bitmap
            ptr += sizeof(int64_t) * num_bitmap_fields;

            // decode rest of fields...
            int64_t capacitySize = sizeof(int64_t);
            if ((row_type == python::Type::STRING || row_type.isDictionaryType() ||
                    row_type == python::Type::GENERICDICT ||
                    (row_type.isListType() && row_type != python::Type::EMPTYLIST && !row_type.elementType().isSingleValued())) &&
                row_type != python::Type::EMPTYDICT) {
                auto elem = *(uint64_t *) (ptr);
                auto offset = (uint32_t) elem;
                auto length = (uint32_t) (elem >> 32ul);
                if (offset + length > capacity) {
                    capacitySize = -1;
                }
            } else if (row_type != python::Type::EMPTYTUPLE && row_type.isTupleType()) {
                capacitySize = checkTupleCapacity(ptr, capacity, row_type);
            }

            if (capacitySize <= -1) {
                return capacitySize;
            }
            capacitySize += sizeof(int64_t) * num_bitmap_fields;

            if (!row_type.isFixedSizeType()) {
                int64_t var_length_region_length = *(int64_t *) (ptr + capacitySize);
                capacitySize += var_length_region_length + sizeof(int64_t);
            }

            return capacitySize;
        }

        bool isCapacityValid(const uint8_t *ptr, int64_t capacity, const python::Type &row_type) {
            if (capacity <= 0) {
                return false;
            }

            int64_t size = serializationSize(ptr, capacity, row_type);

            if (size <= -1) {
                return false;
            }

            return size <= capacity;
        }


        // TODO: check for errors when creating PyObjects
        bool fromSerializedMemory(const uint8_t *ptr, size_t capacity, const tuplex::Schema &schema, PyObject **obj,
                                  const uint8_t **nextptr) {
            python::Type row_type = schema.getRowType();


            // @TODO: fix this function in debug mode
            // this function is not working. leave for now...
            // check is anyways a waste of time...
            // // check for bitmap size & push pointer...
            // if (!isCapacityValid(ptr, capacity, row_type)) {
            //     return false;
            // }

            *obj = createPyObjectFromMemory(ptr, row_type, (uintptr_t)(ptr + capacity));

            if (nextptr) {
                *nextptr = ptr + serializationSize(ptr, capacity, row_type);
            }
            return *obj != nullptr;
        }

        std::vector<bool> getBitmapFromType(const python::Type &objectType, const uint8_t *&ptr, size_t numElements) {
            std::vector<bool> bitmapV;
            bitmapV.reserve(numElements);
            if (objectType.isListType()) {
                if (objectType.elementType().isOptionType()) {
                    auto numBitmapFields = core::ceilToMultiple((unsigned long)numElements, 64ul) / 64;
                    auto bitmapSize = numBitmapFields * sizeof(uint64_t);
                    auto *bitmapAddr = (uint64_t *)ptr;
                    ptr += bitmapSize;
                    for (size_t i = 0; i < numElements; i++) {
                        bool currBit = (bitmapAddr[i / 64] >> (i % 64)) & 0x1;
                        bitmapV.push_back(currBit);
                    }
                }
            }
            return bitmapV;
        }
    }
}
