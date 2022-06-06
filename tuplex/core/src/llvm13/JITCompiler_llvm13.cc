//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 6/6/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
// need to include some llvm file, so version is picked up
#ifndef LLVM13_JITCOMPILER_HEADER_
#define LLVM13_JITCOMPILER_HEADER_

#include <llvm/IR/IRBuilder.h>

#if LLVM_VERSION_MAJOR >= 10
#include <llvm13/JITCompiler_llvm13.h>

namespace tuplex {
    // llvm10+ compatible (designed for llvm13+) compiler class using ORC

    JITCompiler::JITCompiler() {

    }

    JITCompiler::~JITCompiler() {

    }

    void *JITCompiler::getAddrOfSymbol(const std::string &Name) {
        return nullptr;
    }

    bool JITCompiler::compile(const std::string &llvmIR) {
        return false;
    }

    bool JITCompiler::compile(std::unique_ptr<llvm::Module> mod) {
        return false;
    }


}

#endif
#endif
