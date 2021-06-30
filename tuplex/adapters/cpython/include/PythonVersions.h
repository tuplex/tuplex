//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONVERSIONS_H
#define TUPLEX_PYTHONVERSIONS_H

#include <Python.h>

// supported python versions: 3.7 dev
// maybe everything over 3?
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 7)
#define PYTHON37
#endif

#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 6)
    #define PYTHON36
#endif

#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 5)
    #define PYTHON35
#endif

#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 4)
    #define PYTHON34
#endif


#endif //TUPLEX_PYTHONVERSIONS_H