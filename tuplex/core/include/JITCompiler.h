//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JITCOMPILER_H
#define TUPLEX_JITCOMPILER_H

// common interface
#include "IJITCompiler.h"

// depending on LLVM version, include specific implementation as ORC API is super unstable
#if LLVM_VERSION_MAJOR <= 9
#include "llvm9/JITCompiler_llvm9.h"
#else
#include "llvm13/JITCompiler_llvm13.h"
#endif

#endif //TUPLEX_COMPILER_H