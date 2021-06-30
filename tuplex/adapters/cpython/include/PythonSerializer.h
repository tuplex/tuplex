//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONSERIALIZER_H
#define TUPLEX_PYTHONSERIALIZER_H

#include <Python.h>

#include <Schema.h>

namespace tuplex {
    namespace cpython {
        /*!
         * Creates PyObject from raw memory (deserialize)
         * @param ptr memory location to where serialized data is
         * @param capacity size of buffer
         * @param schema Schema holding types of data stored in memory in order stored
         * @param obj pointer to PyObject created from memory (set)
         * @param nextptr ptr position after deserialization
         * @return bool for if deserialization was successful or not
         */
        extern bool fromSerializedMemory(const uint8_t *ptr, int64_t capacity, const Schema &schema, PyObject **obj,
                                         const uint8_t **nextptr = nullptr);
    }

}

#endif //TUPLEX_PYTHONSERIALIZER_H