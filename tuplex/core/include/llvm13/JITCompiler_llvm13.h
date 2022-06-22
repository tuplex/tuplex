//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

// need to include some llvm file, so version is picked up
#include <llvm/IR/IRBuilder.h>

#if LLVM_VERSION_MAJOR > 9
#ifndef TUPLEX_JITCOMPILER_LLVM13_H
#define TUPLEX_JITCOMPILER_LLVM13_H

// common interface
#include "IJITCompiler.h"

#include <llvm/ExecutionEngine/Orc/LLJIT.h>

namespace tuplex {

    // JIT compiler based on LLVM's ORCv2 JIT classes
    class JITCompiler : public IJITCompiler {
    public:
        JITCompiler();
        ~JITCompiler();

        /*!
         * return pointer address of compiled symbol
         * @param Name (un)mangled name of address.
         * @return address of compiled function, nullptr if not found
         */
        void* getAddrOfSymbol(const std::string& Name) override;

        /*!
         * compile string based IR
         * @param llvmIR string of a valid llvm Module in llvm's intermediate representation language
         * @return true if compilation was successful, false in case of failure
         */
        bool compile(const std::string& llvmIR) override;

        bool compile(std::unique_ptr<llvm::Module> mod) override;

        /*!
         * registers symbol with Name as new addressable for linking
         * @param Name for which to link
         * @param addr of Symbol
         */
        template<typename Function> void registerSymbol(const std::string& Name, Function f) {
            using namespace llvm;
            //using namespace llvm::orc;

//            auto addr = reinterpret_cast<llvm::JITTargetAddress>(f);
//            assert(addr);
//
//            // with addressof a C++ function can be hacked into this.
//            // however may lead to hard to debug bugs!
//            _customSymbols[Name] = JITEvaluatedSymbol(addr, JITSymbolFlags::Exported);
        }

    private:

        // @TODO: reimplement JIT using own threadpool for better access on stuff.
        std::unique_ptr<llvm::orc::LLJIT> _lljit;

        // @TODO: add function to remove llvm lib here! Else indefinite grow with queries!
        std::vector<llvm::orc::JITDylib*> _dylibs; // for name lookup search

        // custom symbols
        std::unordered_map<std::string, llvm::JITEvaluatedSymbol> _customSymbols;

    };
}
#endif
#endif