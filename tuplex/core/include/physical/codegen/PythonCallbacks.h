//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONCALLBACKS_H
#define TUPLEX_PYTHONCALLBACKS_H

#include <PythonHelpers.h>

extern "C" {

    /*!
     * call python function by passing as single parameter or tuple. i.e. for lambda x: (x[0], x[5]) suited or lambda a: a + 1
     * @param func
     * @param out
     * @param out_size
     * @param input
     * @param input_size
     * @param in_typeHash
     * @param out_typeHash
     * @return
     */
    int32_t callPythonCodeSingleParam(PyObject* func, uint8_t** out, int64_t* out_size,
                           uint8_t* input, int64_t input_size, int32_t in_typeHash, int32_t out_typeHash);

    /*!
     * like above, but for functions lambda a, b: a + b
     * @param func
     * @param out
     * @param out_size
     * @param input
     * @param input_size
     * @param in_typeHash
     * @param out_typeHash
     * @return
     */
    int32_t callPythonCodeMultiParam(PyObject* func, uint8_t** out, int64_t* out_size,
                       uint8_t* input, int64_t input_size, int32_t in_typeHash, int32_t out_typeHash);

    extern uint8_t* deserializePythonFunction(char* pickledCode, int64_t codeLength) __attribute__((used));

    extern void releasePythonFunction(uint8_t* pyobj) __attribute__((used));

}

#endif //TUPLEX_PYTHONCALLBACKS_H