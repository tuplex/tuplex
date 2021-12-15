//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <JITCompiler.h>
#include <Logger.h>

#include <llvm/IR/Verifier.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>

#include <llvm/Support/TargetSelect.h>
#include <thread>
#include <Timer.h>

//LLVM9 fixes
#include <FixedRTDyldObjectLinkingLayer.h>

// C functions
#include <hashmap.h>
#include <int_hashmap.h>
#include <third_party/i64toa_sse2.h>
#include <third_party/ryu/ryu.h>


namespace tuplex {

#if LLVM_VERSION_MAJOR > 8
    inline llvm::Expected<llvm::orc::ThreadSafeModule> parseToModule(const std::string& llvmIR) {
        using namespace llvm;
        using namespace llvm::orc;

        // first parse IR. It would be also an alternative to directly the LLVM Module from the ModuleBuilder class,
        // however if something went wrong there, memory errors would occur. Better is to first transform to a string
        // and then parse it because LLVM will validate the IR on the way.

        SMDiagnostic err; // create an SMDiagnostic instance
        std::unique_ptr<MemoryBuffer> buff = MemoryBuffer::getMemBuffer(llvmIR);

        auto ctx = std::make_unique<LLVMContext>();
        assert(ctx);
        std::unique_ptr<Module> mod = parseIR(buff->getMemBufferRef(), err, *ctx); // use err directly

        // check if any errors occured during module parsing
        if(nullptr == mod.get()) {
            // print errors
            std::stringstream errStream;
            errStream<<"could not compile module:\n>>>>>>>>>>>>>>>>>\n"
               <<core::withLineNumbers(llvmIR)<<"\n<<<<<<<<<<<<<<<<<\n";
            errStream<<"line " + std::to_string(err.getLineNo()) + ": " + err.getMessage().str();

            return make_error<StringError>(errStream.str(), inconvertibleErrorCode());
        }


        // run verify pass on module and print out any errors, before attempting to compile it
        std::string moduleErrors = "";
        llvm::raw_string_ostream os(moduleErrors);
        if(verifyModule(*mod, &os)) {
            std::stringstream errStream;
            os.flush();
            errStream<<"could not verify module:\n>>>>>>>>>>>>>>>>>\n"<<core::withLineNumbers(llvmIR)<<"\n<<<<<<<<<<<<<<<<<\n";
            errStream<<moduleErrors;

            return make_error<StringError>(errStream.str(), inconvertibleErrorCode());
        }
        return ThreadSafeModule(std::move(mod), std::move(ctx));
    }


    // helper function to deal with llvm error
    std::string errToString(const llvm::Error& err) {
        std::string errString = "";
        llvm::raw_string_ostream os(errString);
        os<<err;
        os.flush();
        return errString;
    }

    // maybe use as basis https://github.com/llvm/llvm-project/blob/e816421087b40e79ef0312f49434a75a97ac69e8/clang/examples/clang-interpreter/main.cpp

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

        // create new LLJIT instance, details under https://www.youtube.com/watch?v=MOQG5vkh9J8
        // notes from https://llvm.org/docs/ORCv2.html#how-to-create-jitdylibs-and-set-up-linkage-relationships
        using namespace llvm;
        using namespace llvm::orc;

        // load host process into LLVM linker layer
        llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);

        // target machine builder
        auto tmBuilder = JITTargetMachineBuilder::detectHost();

        // check that SSE4.2 is supported by target system
        if(!tmBuilder)
            throw std::runtime_error("could not auto-detect host system target machine");

        // get host machine's features
        auto triple = sys::getProcessTriple();
        std::string CPUStr = sys::getHostCPUName();

        // set optimized flags for host system
        auto& tmb = tmBuilder.get();
        tmb.setCodeGenOptLevel(CodeGenOpt::Aggressive);
        tmb.setCodeModel(CodeModel::Large);
        tmb.setCPU(CPUStr);
        tmb.setRelocationModel(Reloc::Model::PIC_);
        tmb.addFeatures(getFeatureList());
        //tmb.addFeatures(codegen::getLLVMFeatureStr()); //<-- should add here probably SSE4.2.??


        Logger::instance().logger("LLVM").info("compiling code for " + CPUStr);
        auto featureStr = tmBuilder.get().getFeatures().getString();

        // cf. https://github.com/tensorflow/mlir/blob/master/lib/ExecutionEngine/ExecutionEngine.cpp
        // for an idea how things are done??

        // to avoid multiple symbol definition problem, maybe use https://github.com/llvm/llvm-project/blob/release/9.x/llvm/examples/LLJITExamples/LLJITWithObjectCache/LLJITWithObjectCache.cpp?

        // more notes here: http://llvm.1065342.n5.nabble.com/llvm-dev-ORC-v2-question-td130489.html

        // checkout https://github.com/dibyendumajumdar/ravi/blob/master/include/ravi_llvmcodegen.h & https://github.com/dibyendumajumdar/ravi/blob/master/src/ravi_llvmjit.cpp


        // LLVM 9.x has a bug when it comes to the ...
        // => exception frames do not get deregistered properly, so any C++ exception after it will segfault
        // => solution: either update to llvm10 or simply use own linking layer which is fixed.
        // use https://github.com/llvm/llvm-project/blob/dbc601e25b6da8d6dd83c8fa6ac8865bb84394a2/llvm/examples/OrcV2Examples/LLJITWithGDBRegistrationListener/LLJITWithGDBRegistrationListener.cpp as example
        // Bug: https://www.mail-archive.com/llvm-bugs@lists.llvm.org/msg33393.html

        auto jitFuture = LLJITBuilder()//.setNumCompileThreads(std::thread::hardware_concurrency())
                .setJITTargetMachineBuilder(tmBuilder.get())
                .setObjectLinkingLayerCreator([](ExecutionSession& ES) {
                    // note in llvm10 the function signature changes to ExecutionSession& Es, const Triple& TT
                    auto GetMemMgr = []() {
                        return std::make_unique<SectionMemoryManager>();
                    };
                    auto ObjLinkingLayer =
                            std::make_unique<FixedRTDyldObjectLinkingLayer>(
                                    ES, std::move(GetMemMgr));

#ifndef NDEBUG
//// in debug mode, register gdb event listener
// ==> need to adjust apis to that a bit...
// could also register vtune listener to benchmark JIT Code!
//                    // Register the event listener.
//                    ObjLinkingLayer->registerJITEventListener(
//                            *JITEventListener::createGDBRegistrationListener());
//                    ObjLinkingLayer->setProcessAllSections(true);
#endif

                    return ObjLinkingLayer;
                }).create();
        if(!jitFuture) {
            std::stringstream err_stream;
            err_stream<<"failed to create LLJIT, details: "<<errToString(jitFuture.takeError());
            std::cerr<<err_stream.str()<<std::endl;
            throw std::runtime_error(err_stream.str());
        }
        _lljit = std::move(jitFuture.get());
        if(!_lljit)
            throw std::runtime_error("failed to access LLJIT pointer");

        auto& JD = _lljit->getMainJITDylib();
        // JD.define to add symbols according to https://llvm.org/docs/ORCv2.html#how-to-create-jitdylibs-and-set-up-linkage-relationships

        const auto& DL = _lljit->getDataLayout();
        auto ProcessSymbolsGenerator =
                DynamicLibrarySearchGenerator::GetForCurrentProcess(
                        DL.getGlobalPrefix());

        // check whether successful
        if(!ProcessSymbolsGenerator)
            throw std::runtime_error("failed to create linker to host process " + errToString(ProcessSymbolsGenerator.takeError()));

        JD.setGenerator(std::move(*ProcessSymbolsGenerator));

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

    bool JITCompiler::compile(std::unique_ptr<llvm::Module> mod) {
        llvm::Expected<llvm::orc::ThreadSafeModule> tsm = llvm::orc::ThreadSafeModule(std::move(mod), std::make_unique<llvm::LLVMContext>());
        if(!tsm) {
            auto err_msg = errToString(tsm.takeError());
            std::cerr<<__FILE__<<":"<<__LINE__<<" thread-safe mod not ok, error: "<<err_msg<<std::endl;
            throw std::runtime_error(err_msg);
            return false;
        }

        auto mIdentifier = tsm->getModule()->getModuleIdentifier(); // this should not be an empty string...

        // change module target triple, data layout etc. to target machine
        tsm->getModule()->setDataLayout(_lljit->getDataLayout());

        // look into https://github.com/llvm/llvm-project/blob/master/llvm/examples/ModuleMaker/ModuleMaker.cpp on how to ouput bitcode

        // create for this module own jitlib
        auto& ES = _lljit->getExecutionSession();
        auto& jitlib = ES.createJITDylib(tsm->getModule()->getName());
        const auto& DL = _lljit->getDataLayout();
        llvm::orc::MangleAndInterner Mangle(ES, DL);

        // link with host process symbols....
        auto ProcessSymbolsGenerator =
                llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(
                        DL.getGlobalPrefix());

        // check whether successful
        if(!ProcessSymbolsGenerator) {
            auto err_msg = "failed to create linker to host process " + errToString(ProcessSymbolsGenerator.takeError());
            std::cerr<<__FILE__<<":"<<__LINE__<<" error: "<<err_msg<<std::endl;
            throw std::runtime_error(err_msg);
        }

        jitlib.setGenerator(std::move(*ProcessSymbolsGenerator));

        // define symbols from custom symbols for this jitlib
        for(auto keyval: _customSymbols)
            auto rc = jitlib.define(llvm::orc::absoluteSymbols({{Mangle(keyval.first), keyval.second}}));

        _dylibs.push_back(&jitlib); // save reference for search

        assert(tsm);
        auto err = _lljit->addIRModule(jitlib, std::move(tsm.get()));
        if(err) {
            std::stringstream err_stream;
            err_stream<<"compilation failed, "<<errToString(err);
            std::cerr<<err_stream.str()<<std::endl;
            throw std::runtime_error(err_stream.str());
        }

        // other option: modify module with unique prefix!
        // // one option to do this, is to iterate over functions and prefix them with a query number...
        // // ==> later, make this more sophisticated...
        // // llvm::Function* function;
        // // function->setName("query1_" + function->getName())
        // // ==> this is stupid though... but well, seems to be required.
        // // ==> smarter way is to do lookup!
        // // i.e. iterate over all functions in the module to change them...
        // auto err =_lljit->addIRModule(std::move(tsm.get()));
        // if(err)
        //     throw std::runtime_error("compilation failed, " + errToString(err));

        // // another reference: https://doxygen.postgresql.org/llvmjit_8c_source.html

        return true;
    }

    bool JITCompiler::compile(const std::string &llvmIR) {
        using namespace llvm;
        using namespace llvm::orc;

        assert(_lljit);

        // parse module, make new threadsafe module
        auto tsm = parseToModule(llvmIR);
        if(!tsm)
            throw std::runtime_error(errToString(tsm.takeError()));

        auto mIdentifier = tsm->getModule()->getModuleIdentifier(); // this should not be an empty string...
        tsm->getModule()->setDataLayout(_lljit->getDataLayout());

        // look into https://github.com/llvm/llvm-project/blob/master/llvm/examples/ModuleMaker/ModuleMaker.cpp on how to ouput bitcode

        // create for this module own jitlib
        auto& ES = _lljit->getExecutionSession();
        auto& jitlib = ES.createJITDylib(tsm->getModule()->getName());
        const auto& DL = _lljit->getDataLayout();
        MangleAndInterner Mangle(ES, DL);

        // link with host process symbols....
        auto ProcessSymbolsGenerator =
                DynamicLibrarySearchGenerator::GetForCurrentProcess(
                        DL.getGlobalPrefix());

        // check whether successful
        if(!ProcessSymbolsGenerator)
            throw std::runtime_error("failed to create linker to host process " + errToString(ProcessSymbolsGenerator.takeError()));
        jitlib.setGenerator(std::move(*ProcessSymbolsGenerator));

        // define symbols from custom symbols for this jitlib
        for(auto keyval: _customSymbols)
            auto rc = jitlib.define(absoluteSymbols({{Mangle(keyval.first), keyval.second}}));

        _dylibs.push_back(&jitlib); // save reference for search
        auto err = _lljit->addIRModule(jitlib, std::move(tsm.get()));
        if(err)
            throw std::runtime_error("compilation failed, " + errToString(err));

        // other option: modify module with unique prefix!
        // // one option to do this, is to iterate over functions and prefix them with a query number...
        // // ==> later, make this more sophisticated...
        // // llvm::Function* function;
        // // function->setName("query1_" + function->getName())
        // // ==> this is stupid though... but well, seems to be required.
        // // ==> smarter way is to do lookup!
        // // i.e. iterate over all functions in the module to change them...
        // auto err =_lljit->addIRModule(std::move(tsm.get()));
        // if(err)
        //     throw std::runtime_error("compilation failed, " + errToString(err));

        // // another reference: https://doxygen.postgresql.org/llvmjit_8c_source.html

        return true;
    }

    void* JITCompiler::getAddrOfSymbol(const std::string &Name) {
        if(Name.empty())
            return nullptr;

        // search for symbol in all dylibs
        for(auto it = _dylibs.rbegin(); it != _dylibs.rend(); ++it) {
            auto sym = _lljit->lookup(**it, Name);

            if(sym)
                return reinterpret_cast<void*>(sym.get().getAddress());
            else {
                auto err = sym.takeError();
                Logger::instance().logger("LLVM").error(errToString(err));
            }
        }

        Logger::instance().logger("LLVM").error("could not find symbol " + Name + ". ");
        return nullptr;
    }
#else
    namespace legacy {
        bool JITCompiler::compile(const std::string& llvmIR) {
            using namespace llvm;
            // first parse IR. It would be also an alternative to directly the LLVM Module from the ModuleBuilder class,
            // however if something went wrong there, memory errors would occur. Better is to first transform to a string
            // and then parse it because LLVM will validate the IR on the way.


            // check how long parsing takes etc.
            tuplex::Timer timer;

            SMDiagnostic err; // create an SMDiagnostic instance
            std::unique_ptr<MemoryBuffer> buff = MemoryBuffer::getMemBuffer(llvmIR);

            LLVMContext context;
            std::unique_ptr<Module> mod = parseIR(buff->getMemBufferRef(), err, context); // use err directly

            // check if any errors occured during module parsing
            if(nullptr == mod.get()) {
                // print errors
                Logger::instance().logger("LLVM Backend").error("could not parse module:\n>>>>>>>>>>>>>>>>>\n"
                                                                + core::withLineNumbers(llvmIR)
                                                                + "\n<<<<<<<<<<<<<<<<<");
                Logger::instance().logger("LLVM Backend").error("line " + std::to_string(err.getLineNo()) + ": " + err.getMessage().str());
                return false;
            }

            Logger::instance().logger("LLVM Backend").info("parsing of " + sizeToMemString(llvmIR.length()) + " took " + std::to_string(timer.time() * 1000.0) + "ms");
            // sert datalayout to current machine one's
            mod->setDataLayout(dataLayoutStr);

            // run verify pass on module and print out any errors, before attempting to compile it
            timer.reset();
            std::string moduleErrors = "";
            llvm::raw_string_ostream os(moduleErrors);
            if(verifyModule(*mod, &os)) {
                os.flush();
                Logger::instance().logger("LLVM Backend").error("could not verify module:\n>>>>>>>>>>>>>>>>>\n"
                                                                + core::withLineNumbers(llvmIR)
                                                                + "\n<<<<<<<<<<<<<<<<<");
                Logger::instance().logger("LLVM Backend").error(moduleErrors);
                return false;
            }

            Logger::instance().logger("LLVM Backend").info("verification of module took " + std::to_string(timer.time() * 1000.0) + "ms");


            timer.reset();
            addModule(std::move(mod));
            Logger::instance().logger("LLVM Backend").info("Compilation of module took " + std::to_string(timer.time() * 1000.0) + "ms");
            return true;
        }

        void* JITCompiler::getAddrOfSymbol(const std::string &Name) {
            assert(!Name.empty());

            auto ExprSymbol = findSymbol(Name);
            if(!ExprSymbol) {
                Logger::instance().logger("JITCompiler").error("Symbol '" + Name +"' could not be found in compiled code.s");
                return nullptr;
            };

            // llvm function addr
            void* faddr = (void*)ExprSymbol.getAddress().get();
            return faddr;
        }
    }
#endif
}