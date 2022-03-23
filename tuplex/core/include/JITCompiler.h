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

#include "llvm/ADT/iterator_range.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/RTDyldMemoryManager.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/LambdaResolver.h"
#include "llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/Mangler.h"
#include "llvm/Support/DynamicLibrary.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Target/TargetMachine.h"

#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/ExecutionEngine/Orc/CompileUtils.h"
#include "llvm/ExecutionEngine/Orc/IRCompileLayer.h"
#include "llvm/ExecutionEngine/Orc/LambdaResolver.h"
#include "llvm/ExecutionEngine/RuntimeDyld.h"
#include "llvm/ExecutionEngine/SectionMemoryManager.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <Utils.h>
#include <codegen/CodegenHelper.h>

// for the mangling hack
#include <physical/PythonCallbacks.h>
#include <hashmap.h>

#include <codegen/CodegenHelper.h>


#if LLVM_VERSION_MAJOR > 8
// ORCv2 APIs
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#endif

namespace tuplex {

#if LLVM_VERSION_MAJOR < 9
    namespace legacy {
        extern std::shared_ptr<llvm::TargetMachine*> getOrCreateTargetMachine();

        /*!
         * LLVM based compiler.
         * Inspired from https://github.com/llvm-mirror/llvm/blob/master/examples/Kaleidoscope/include/KaleidoscopeJIT.h
         * Must not be a class member.
         */
        class JITCompiler {
        public:
            using ObjLayerT = llvm::orc::RTDyldObjectLinkingLayer;
            using CompileLayerT = llvm::orc::IRCompileLayer<ObjLayerT, llvm::orc::SimpleCompiler>;
            using ModuleHandleT = CompileLayerT::ModuleHandleT;
        private:
            std::unique_ptr<llvm::TargetMachine> TM;
            std::string dataLayoutStr;
        ObjLayerT *ObjectLayer;
        CompileLayerT *CompileLayer;
        std::vector<ModuleHandleT> ModuleHandles;

            // allow user to register custom symbols
        std::unordered_map<std::string, llvm::JITTargetAddress> _customSymbols;

        /*!
             * names need to be mangled, prepends '_' on OSX or '\x1' on Windows
             * @param Name
             * @return mangled Name
             */
            std::string mangle(const std::string &Name) {
                std::string MangledName;
                llvm::raw_string_ostream MangledNameStream(MangledName);
                assert(TM);

            // make sure there is a compatible Data Layout
            assert(TM->createDataLayout().getStringRepresentation() == dataLayoutStr);

                llvm::Mangler::getNameWithPrefix(MangledNameStream, Name, llvm::DataLayout(dataLayoutStr));

            MangledName = MangledNameStream.str(); // flush stream contents
            assert(!MangledName.empty());

                return MangledName;
            }

            llvm::JITSymbol findMangledSymbol(const std::string &Name) {
#ifdef LLVM_ON_WIN32
                // The symbol lookup of ObjectLinkingLayer uses the SymbolRef::SF_Exported
    // flag to decide whether a symbol will be visible or not, when we call
    // IRCompileLayer::findSymbolIn with ExportedSymbolsOnly set to true.
    //
    // But for Windows COFF objects, this flag is currently never set.
    // For a potential solution see: https://reviews.llvm.org/rL258665
    // For now, we allow non-exported symbols on Windows as a workaround.
    const bool ExportedSymbolsOnly = false;
#else
                const bool ExportedSymbolsOnly = true;
#endif

                using namespace std;
            // cout<<"looking up: "<<Name<<endl;


            // check custom map
            auto it = _customSymbols.find(Name);
            if(it != _customSymbols.end())
                return llvm::JITSymbol(it->second, llvm::JITSymbolFlags::Exported);

            //cout<<"not found in custom symbols, checking modules..."<<endl;

            // Search modules in reverse order: from last added to first added.
            // This is the opposite of the usual search order for dlsym, but makes more
            // sense in a REPL where we want to bind to the newest available definition.
            for (auto H : llvm::make_range(ModuleHandles.rbegin(), ModuleHandles.rend()))
                if (auto Sym = CompileLayer->findSymbolIn(H, Name, ExportedSymbolsOnly))
                    return Sym;

                // note: this codepiece only works under Mac OS X when the library is linked via C++,
                // not under Ubuntu / Docker / GCC.
                // solution is to manually load runtime during runtime
                // or add functions via LLVMEnvironment::registerBuiltinFunction (stubbed for now)
                // another option (used in codegen) is to cast a function pointer in the IR (runtime generated IR only!)

            //cout<<"not found in modules, searching in process..."<<endl;

                // small hack to allow python callbacks. ==> System needs refactoring!
#warning "refactor Compiler and LLVM Environment to avoid this ugly hack here"
                if(Name == mangle("callPythonCode"))
                    return llvm::JITSymbol(reinterpret_cast<llvm::JITTargetAddress>(callPythonCode), llvm::JITSymbolFlags::Exported);

                if(Name == mangle("hashmap_get"))
                    return llvm::JITSymbol(reinterpret_cast<llvm::JITTargetAddress>(hashmap_get), llvm::JITSymbolFlags::Exported);

            // @TODO: possibly for docker this here needs to add the other two python callback functions??

            // If we can't find the symbol in the JIT, try looking in the host process.
            if (auto SymAddr = llvm::RTDyldMemoryManager::getSymbolAddressInProcess(Name))
                return llvm::JITSymbol(SymAddr, llvm::JITSymbolFlags::Exported);

#ifdef LLVM_ON_WIN32
                // For Windows retry without "_" at beginning, as RTDyldMemoryManager uses
    // GetProcAddress and standard libraries like msvcrt.dll use names
    // with and without "_" (for example "_itoa" but "sin").
    if (Name.length() > 2 && Name[0] == '_')
      if (auto SymAddr =
              RTDyldMemoryManager::getSymbolAddressInProcess(Name.substr(1)))
        return JITSymbol(SymAddr, JITSymbolFlags::Exported);
#endif


                Logger::instance().logger("JITcompiler").error("Could not resolve symbol " + Name);

                return nullptr;
            }

            ModuleHandleT addModule(std::shared_ptr<llvm::Module> M) {
                // We need a memory manager to allocate memory and resolve symbols for this
                // new module. Create one that resolves symbols by looking back into the
                // JIT.
                auto Resolver = llvm::orc::createLambdaResolver(
                        [&](const std::string &Name) {
                            if (auto Sym = findMangledSymbol(Name))
                                return Sym;
                            return llvm::JITSymbol(nullptr);
                        },
                        [](const std::string &S) { return nullptr; });
                assert(M.get());
                auto H = cantFail(CompileLayer->addModule(std::move(M),
                                                          std::move(Resolver)));

                ModuleHandles.push_back(H);
                return H;
            }

            void removeModule(ModuleHandleT H) {

                auto it = std::find(ModuleHandles.begin(), ModuleHandles.end(), H);
                ModuleHandles.erase(it);
                cantFail(CompileLayer->removeModule(H));
            }

        public:
            JITCompiler() {
                // required, because else functions fail.
                codegen::initLLVM();

            TM.reset(codegen::getOrCreateTargetMachine());
            assert(TM);

            // store dataLayout
            dataLayoutStr = TM->createDataLayout().getStringRepresentation();

            // std::cout<<"created JIT Compiler with layout: "<<dataLayoutStr<<std::endl;
            // std::cout<<"target triple of this machine is: "<<TM->getTargetTriple().str()<<std::endl;

            ObjectLayer = new ObjLayerT([]() { return std::make_shared<llvm::SectionMemoryManager>(); });
            assert(ObjectLayer);
            CompileLayer = new CompileLayerT(*ObjectLayer, llvm::orc::SimpleCompiler(*TM));
            assert(CompileLayer);

                // load own executable as (dummy) dynamic library for symbol lookup
                llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
            }

            ~JITCompiler() {
                if(CompileLayer)
                    delete CompileLayer;
                if(ObjectLayer)
                    delete ObjectLayer;
                CompileLayer = nullptr;
                ObjectLayer = nullptr;
                TM = nullptr;
            }

            llvm::TargetMachine& getTargetMachine() { assert(TM); return *TM.get(); }

            llvm::JITSymbol findSymbol(const std::string& Name) {
                return findMangledSymbol(mangle(Name));
            }

            void* getAddrOfSymbol(const std::string& Name);

            /*!
             * compile string based IR
             * @param llvmIR string of a valid llvm Module in llvm's intermediate representation language
             */
            bool compile(const std::string& llvmIR);

            /*!
             * registers symbol with Name as new addressable for linking
             * @param Name for which to link
             * @param addr of Symbol
             */
            template<typename Function> void registerSymbol(const std::string& Name, Function f) {

                // with addressof a C++ function can be hacked into this.
                // however may lead to hard to debug bugs!

                _customSymbols[mangle(Name)] = reinterpret_cast<llvm::JITTargetAddress>(f);
            }

        };
    }
#endif
    /*!
    * helper function to initialize LLVM targets for this platform
    */
#if LLVM_VERSION_MAJOR < 9
    using JITCompiler=legacy::JITCompiler;
#else

    // JIT compiler based on LLVM's ORCv2 JIT classes
    class JITCompiler {
    public:
        JITCompiler();
        ~JITCompiler();

        /*!
         * return pointer address of compiled symbol
         * @param Name (un)mangled name of address.
         * @return address of compiled function, nullptr if not found
         */
        void* getAddrOfSymbol(const std::string& Name);

        /*!
         * compile string based IR
         * @param llvmIR string of a valid llvm Module in llvm's intermediate representation language
         * @return true if compilation was successful, false in case of failure
         */
        bool compile(const std::string& llvmIR);

        bool compile(std::unique_ptr<llvm::Module> mod);

        /*!
         * registers symbol with Name as new addressable for linking
         * @param Name for which to link
         * @param addr of Symbol
         */
        template<typename Function> void registerSymbol(const std::string& Name, Function f) {
            using namespace llvm;
            using namespace llvm::orc;

            auto addr = reinterpret_cast<llvm::JITTargetAddress>(f);
            assert(addr);

            // with addressof a C++ function can be hacked into this.
            // however may lead to hard to debug bugs!
            _customSymbols[Name] = JITEvaluatedSymbol(addr, JITSymbolFlags::Exported);
        }

    private:

        // @TODO: reimplement JIT using own threadpool for better access on stuff.
        std::unique_ptr<llvm::orc::LLJIT> _lljit;

        // @TODO: add function to remove llvm lib here! Else indefinite grow with queries!
        std::vector<llvm::orc::JITDylib*> _dylibs; // for name lookup search

        // custom symbols
        std::unordered_map<std::string, llvm::JITEvaluatedSymbol> _customSymbols;

    };
#endif
}


#endif //TUPLEX_COMPILER_H