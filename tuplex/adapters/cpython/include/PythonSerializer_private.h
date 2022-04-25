//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONSERIALIZER_PRIVATE_H
#define TUPLEX_PYTHONSERIALIZER_PRIVATE_H

#include <Python.h>

#include <TypeSystem.h>

namespace tuplex {
    namespace cpython {
        /*!
         * Creates Python object from raw memory (deserialize)
         * @param ptr memory location to where serialized data is
         * @param row_type holding information on types of input memory
         * @param bitmap pointer to bitmap (i.e. multiple 64bit blocks)
         * @param index of the element within the bitmap
         * @return Python object holding deserialized elements
         */
        PyObject *createPyObjectFromMemory(const uint8_t *ptr, const python::Type &row_type, const uint8_t *bitmap = nullptr, int index = 0);

        /*!
         * Creates Python tuple object from raw memory (deserialize)
         * @param ptr memory location to where serialized data is
         * @param row_type holding information on types of input memory
         * @return Python object holding deserialized elements
         */
        PyObject *createPyTupleFromMemory(const uint8_t *ptr, const python::Type &row_type);

        PyObject *createPyDictFromMemory(const uint8_t *ptr);

        PyObject *createPyListFromMemory(const uint8_t *ptr, const python::Type &row_type);

        /*!
         * Checks if capacity for buffer with schema is valid (if it is possible for buffer to hold such data given schema)
         * @param ptr memory location to where serialized data is
         * @param capacity size of buffer
         * @param row_type holding information on types of input memory
         * @return bool for if capacity is valid or not
         */
        bool isCapacityValid(const uint8_t *ptr, int64_t capacity, const python::Type &row_type);

        /*!
         * Check capacity of tuple for given buffer (and associated tree for schema information)
         * @param ptr memory location to where serialized data is
         * @param capacity size of buffer
         * @param row_type holding information on types of input memory
         * @return -1 if invalid, size of serialized data if valid
         */
        int64_t checkTupleCapacity(const uint8_t *ptr, int64_t capacity, const python::Type &row_type);

        std::vector<bool> getBitmapFromType(const python::Type &objectType, const uint8_t *&ptr, int64_t numElements) {
            std::vector<bool> bitmapV;
            bitmapV.reserve(numElements);
            if (objectType.isListType()) {
                if (objectType.elementType().isOptionType()) {
                    auto numBitmapFields = core::ceilToMultiple((unsigned long)numElements, 64ul)/64;
                    auto bitmapSize = numBitmapFields * sizeof(uint64_t);
                    auto *bitmapAddr = (uint64_t *)ptr;
                    ptr += bitmapSize;
                    for (size_t i = 0; i < numElements; i++) {
                        bool currBit = (bitmapAddr[i/64] >> (i % 64)) & 1;
                        bitmapV.push_back(currBit);
                    }
                }
            }
            return bitmapV;
        }
    }
}



#endif //TUPLEX_PYTHONSERIALIZER_PRIVATE_H