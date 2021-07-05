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
#include <iostream>

namespace tuplex {

    void translateCCException(const PythonException& e) {
        

        std::cerr<<"C++ exception raised: "<<e.what()<<std::endl;
	std::cerr.flush();

#ifndef NDEBUG
        printf("C++ exception raised: %s", e.what());
#endif


        PyErr_SetString(PyExc_Exception, e.what());
    }
}
