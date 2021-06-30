//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonException.h>

namespace tuplex {

    void translateCCException(const PythonException& e) {

#ifndef NDEBUG
        printf("C++ exception raised: %s", e.what());
#endif


        PyErr_SetString(PyExc_Exception, e.what());
    }
}