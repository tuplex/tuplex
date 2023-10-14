//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 5/18/2022                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_IJITCOMPILER_H
#define TUPLEX_IJITCOMPILER_H

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <Utils.h>
#include <CodegenHelper.h>

// for the mangling hack
#include <physical/PythonCallbacks.h>
#include <hashmap.h>

#include <llvm/IR/Module.h>


namespace tuplex {
    // abstract JIT compiler interface
    class IJITCompiler {
    public:

        /*!
         * return pointer address of compiled symbol
         * @param Name (un)mangled name of address.
         * @return address of compiled function, nullptr if not found
         */
        virtual void* getAddrOfSymbol(const std::string& Name) = 0;

        /*!
         * compile string based IR
         * @param llvmIR string of a valid llvm Module in llvm's intermediate representation language
         * @return true if compilation was successful, false in case of failure
         */
        virtual bool compile(const std::string& llvmIR) = 0;

        /*!
         * compile llvm module
         * @param mod module to compile
         * @return true if compilation was successful, false in case of failure.
         */
        virtual bool compile(std::unique_ptr<llvm::Module> mod) = 0;
    };
}

#endif //TUPLEX_IJITCOMPILER_H
