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
         * @param capacity size of buffer
         * @param bitmap pointer to bitmap (i.e. multiple 64bit blocks)
         * @param index of the element within the bitmap
         * @return Python object holding deserialized elements
         */
        PyObject *
        createPyObjectFromMemory(const uint8_t *ptr, const python::Type &row_type, int64_t capacity,
                                 const uint8_t *bitmap = nullptr, int index = 0);

        /*!
         * Creates Python tuple object from raw memory (deserialize)
         * @param ptr memory location to where serialized data is
         * @param row_type holding information on types of input memory
         * @param capacity size of buffer
         * @return Python object holding deserialized elements
         */
        PyObject *createPyTupleFromMemory(const uint8_t *ptr, const python::Type &row_type, int64_t capacity);

        PyObject *createPyDictFromMemory(const uint8_t *ptr);

        /*!
         * Creates Python list object from raw memory (deserialize)
         * @param ptr memory location to where serialized data is
         * @param row_type holding information on types of input memory
         * @param capacity size of buffer
         * @return Python object holding deserialized elements
         */
        PyObject *createPyListFromMemory(const uint8_t *ptr, const python::Type &row_type, int64_t capacity);

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

        /*!
         * map bitmap of the object at ptr to a vector with numElements true/false values
         * @param objectType current object type that contains optional value
         * @param ptr memory location to where the start of bitmap
         * @param numElements number of elements in objectType for which bitmap is needed
         * @return
         */
        std::vector<bool> getBitmapFromType(const python::Type &objectType, const uint8_t *&ptr, int64_t numElements);
    }
}



#endif //TUPLEX_PYTHONSERIALIZER_PRIVATE_H