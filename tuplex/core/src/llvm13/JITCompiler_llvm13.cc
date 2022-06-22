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
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/ExecutionEngine/JITLink/JITLinkMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/Orc/TargetProcess/TargetExecutionUtils.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>

// C functions
#include <hashmap.h>
#include <int_hashmap.h>
#include <third_party/i64toa_sse2.h>
#include <third_party/ryu/ryu.h>

namespace tuplex {
    // llvm10+ compatible (designed for llvm13+) compiler class using ORC

    // helper function to deal with llvm error
    static std::string errToString(const llvm::Error& err) {
        std::string errString = "";
        llvm::raw_string_ostream os(errString);
        os<<err;
        os.flush();
        return errString;
    }

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

        // build on top of this:
        // https://github.com/llvm/llvm-project/blob/release/13.x/llvm/examples/OrcV2Examples/LLJITWithGDBRegistrationListener/LLJITWithGDBRegistrationListener.cpp

        auto jitFuture = LLJITBuilder().setJITTargetMachineBuilder(std::move(tmb))
                .setObjectLinkingLayerCreator([&](ExecutionSession& ES, const Triple& TT) {
                    auto GetMemMgr = []() { return std::make_unique<SectionMemoryManager>(); };
                    auto ObjLinkingLayer =
                            std::make_unique<RTDyldObjectLinkingLayer>(
                                    ES, std::move(GetMemMgr));

                    // Register the event listener.
                    ObjLinkingLayer->registerJITEventListener(
                            *JITEventListener::createGDBRegistrationListener());

                    // Make sure the debug info sections aren't stripped.
                    ObjLinkingLayer->setProcessAllSections(true);

                    return ObjLinkingLayer;
                }).create();

        _lljit = std::move(jitFuture.get());
        if(!_lljit)
            throw std::runtime_error("failed to access LLJIT pointer");


        auto& JD = _lljit->getMainJITDylib();
        // JD.define to add symbols according to https://llvm.org/docs/ORCv2.html#how-to-create-jitdylibs-and-set-up-linkage-relationships

        const auto& DL = _lljit->getDataLayout();
        MangleAndInterner Mangle(_lljit->getExecutionSession(), _lljit->getDataLayout());
        auto ProcessSymbolsGenerator =
                DynamicLibrarySearchGenerator::GetForCurrentProcess(
                        DL.getGlobalPrefix(), [MainName = Mangle("main")](const orc::SymbolStringPtr &Name) {
            return Name != MainName;
        });

        // check whether successful
        if(!ProcessSymbolsGenerator)
            throw std::runtime_error("failed to create linker to host process " + errToString(ProcessSymbolsGenerator.takeError()));

        JD.addGenerator(std::move(*ProcessSymbolsGenerator));

        // add custom symbols / lookup to main dylib.
        // ==> needs to be checked under Ubuntu as well, not sure if this won't produce an error.
        registerSymbol("callPythonCodeMultiParam", callPythonCodeMultiParam);
        registerSymbol("callPythonCodeSingleParam", callPythonCodeMultiParam);
        registerSymbol("releasePythonFunction", releasePythonFunction);
        registerSymbol("deserializePythonFunction", deserializePythonFunction);

        // Ubuntu errors???
        // register hashmap symbols
        registerSymbol("hashmap_get", hashmap_get);
        registerSymbol("hashmap_put", hashmap_put);
        registerSymbol("int64_hashmap_get", int64_hashmap_get);
        registerSymbol("int64_hashmap_put", int64_hashmap_put);

        // fast converters
        // int i64toa_sse2(int64_t value, char* buffer)
        // int d2fixed_buffered_n(double d, uint32_t precision, char* result);
        registerSymbol("i64toa_sse2", i64toa_sse2);
        registerSymbol("d2fixed_buffered_n", d2fixed_buffered_n);


        // AWS SDK cJSON
#ifdef BUILD_WITH_AWS
        // cJSON_PrintUnformatted, cJSON_AddItemToObject, cJSON_CreateObject, cJSON_DetachItemViaPointer, cJSON_CreateString
        registerSymbol("cJSON_PrintUnformatted", cJSON_PrintUnformatted);
        registerSymbol("cJSON_AddItemToObject", cJSON_AddItemToObject);
        registerSymbol("cJSON_CreateObject", cJSON_CreateObject);
        registerSymbol("cJSON_DetachItemViaPointer", cJSON_DetachItemViaPointer);
        registerSymbol("cJSON_CreateString", cJSON_CreateString);
        registerSymbol("cJSON_GetObjectItemCaseSensitive", cJSON_GetObjectItemCaseSensitive);
        registerSymbol("cJSON_GetArraySize", cJSON_GetArraySize);
        registerSymbol("cJSON_CreateNumber", cJSON_CreateNumber);
        registerSymbol("cJSON_CreateBool", cJSON_CreateBool);
        registerSymbol("cJSON_IsTrue", cJSON_IsTrue);
        registerSymbol("cJSON_Parse", cJSON_Parse);
        registerSymbol("cJSON_CreateString", cJSON_CreateString);
#endif

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
        using namespace llvm;
        //Expected<orc::ThreadSafeModule> tsm = orc::ThreadSafeModule(std::move(mod),  std::make_unique<llvm::LLVMContext>());



        return false;
    }


}

#endif
#endif
