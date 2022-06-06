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


#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/IndirectionUtils.h>
#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Host.h>


namespace tuplex {
    // llvm10+ compatible (designed for llvm13+) compiler class using ORC

    // Note: According to https://llvm.org/docs/ORCv2.html JITEventListeners are NOT supported with ORC.
    // should use MCJIT therefore??

    static std::vector<std::string> getFeatureList() {
        using namespace llvm;
        SubtargetFeatures Features;

        // If user asked for the 'native' CPU, we need to autodetect features.
        // This is necessary for x86 where the CPU might not support all the
        // features the autodetected CPU name lists in the target. For example,
        // not all Sandybridge processors support AVX.
        StringMap<bool> HostFeatures;
        if (sys::getHostCPUFeatures(HostFeatures))
            for (auto &F : HostFeatures)
                Features.AddFeature(F.first(), F.second);

        return Features.getFeatures();
    }

    JITCompiler::JITCompiler() {

        codegen::initLLVM(); // lazy initialization of LLVM backend.

        using namespace llvm;
        using namespace llvm::orc;

        // load host process into LLVM
        llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);


        // target machine builder
        auto tmBuilder = JITTargetMachineBuilder::detectHost();

        // check that SSE4.2 is supported by target system
        if(!tmBuilder)
            throw std::runtime_error("could not auto-detect host system target machine");

        // get host machine's features
        auto triple = sys::getProcessTriple();
        std::string CPUStr = sys::getHostCPUName().str();

        // set optimized flags for host system
        auto& tmb = tmBuilder.get();
        tmb.setCodeGenOptLevel(CodeGenOpt::Aggressive);
        tmb.setCodeModel(CodeModel::Large);
        tmb.setCPU(CPUStr);
        tmb.setRelocationModel(Reloc::Model::PIC_);
        tmb.addFeatures(getFeatureList());
        //tmb.addFeatures(codegen::getLLVMFeatureStr()); //<-- should add here probably SSE4.2.??
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
