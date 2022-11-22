//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <jit/LLVMOptimizer.h>

#include "llvm/ADT/Triple.h"
#include "llvm/Analysis/CallGraph.h"
#include "llvm/Analysis/CallGraphSCCPass.h"
#include "llvm/Analysis/LoopPass.h"
#include "llvm/Analysis/RegionPass.h"
#include "llvm/Analysis/TargetLibraryInfo.h"
#include "llvm/Analysis/TargetTransformInfo.h"
#include "llvm/Bitcode/BitcodeWriterPass.h"
#include "llvm/CodeGen/TargetPassConfig.h"
#include "llvm/IR/DataLayout.h"
#include "llvm/IR/DebugInfo.h"
#include "llvm/IR/IRPrintingPasses.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LegacyPassNameParser.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/InitializePasses.h"
#include "llvm/LinkAllIR.h"
#include "llvm/LinkAllPasses.h"
#include "llvm/MC/SubtargetFeature.h"
#include "llvm/Support/Debug.h"
#include "llvm/Support/FileSystem.h"
#include "llvm/Support/Host.h"
#include "llvm/Support/ManagedStatic.h"
#include "llvm/Support/PluginLoader.h"
#include "llvm/Support/PrettyStackTrace.h"
#include "llvm/Support/Signals.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/SystemUtils.h"
#include "llvm/Support/TargetRegistry.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/ToolOutputFile.h"
#include "llvm/Support/YAMLTraits.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/Coroutines.h"
#include "llvm/Transforms/IPO/AlwaysInliner.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include <algorithm>
#include <memory>
#include <Logger.h>
#include <Utils.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>

using namespace llvm;

namespace tuplex {

    LLVMOptimizer::LLVMOptimizer() : _logger(Logger::instance().logger("optimizer")) {
    }


    static std::unique_ptr<llvm::TargetMachine> GetHostTargetMachine() {
        std::unique_ptr<llvm::TargetMachine> TM(llvm::EngineBuilder().selectTarget());
        return TM;
    }

    // these are the default passes used
    void generateFunctionPassesI(llvm::legacy::FunctionPassManager& fpm) {

#ifndef NDEBUG
        // add lint pass in debug mode to identify problems quickly!
        fpm.add(createLintPass());
#endif

        // perform first some dead code elimination to ease pressure on following passes
        fpm.add(createDeadCodeEliminationPass());
        fpm.add(createDeadStoreEliminationPass());
        
        
//        // function-wise passes
        fpm.add(createSROAPass()); // break up aggregates
        //fpm.add(createInstructionCombiningPass()); // <-- this pass here fails. It has all the powerful patterns though...
        fpm.add(createReassociatePass());
        fpm.add(createGVNPass());
        fpm.add(createCFGSimplificationPass());
        fpm.add(createAggressiveDCEPass());
        fpm.add(createCFGSimplificationPass());

        // added passes...
        fpm.add(createPromoteMemoryToRegisterPass()); // mem2reg pass
        fpm.add(createAggressiveDCEPass());

        fpm.add(createSinkingPass());
        fpm.add(createCFGSimplificationPass());
        fpm.add(createAggressiveInstCombinerPass());

        fpm.add(createAggressiveDCEPass());

        // // try here instruction combining pass...
        // buggy...
        // fpm.add(createInstructionCombiningPass(true));

        // custom added passes
        // ==> Tuplex is memcpy heavy, i.e. optimize!
        fpm.add(createMemCpyOptPass()); // !!! use this pass for sure !!! It's quite expensive first, but it pays off big time.

        // create sel prep pass
        fpm.add(createCodeGenPreparePass());
    }

    void optimizePipelineI(llvm::Module& mod) {
        // Step 1: optimize functions
        auto fpm = llvm::make_unique<legacy::FunctionPassManager>(&mod);
        assert(fpm.get());

        generateFunctionPassesI(*fpm.get());
        fpm->doInitialization();

        // run function passes over each function in the module
        for(Function& f: mod.getFunctionList())
            fpm->run(f);

        // on current master, module optimizations are deactivated. Inlining seems to worsen things!
         // Step 2: optimize over whole module
         // Module passes (function inlining)
         legacy::PassManager pm;
         // inline functions now
         pm.add(createGlobalDCEPass()); // remove dead globals
         pm.add(createConstantMergePass()); // merge global constants
         pm.add(createFunctionInliningPass(200)); // 250 is O3 threshold.
         pm.add(createDeadArgEliminationPass());
         pm.run(mod);

         // run per function pass again
        // run function passes over each function in the module
        for(Function& f: mod.getFunctionList())
            fpm->run(f);
    }

    // // these are the default passes used
    //    void generateFunctionPassesI(llvm::legacy::FunctionPassManager& fpm) {
    //        // function-wise passes
    //        fpm.add(createSROAPass()); // break up aggregates
    //        fpm.add(createInstructionCombiningPass());
    //        fpm.add(createReassociatePass());
    //        fpm.add(createGVNPass());
    //        fpm.add(createCFGSimplificationPass());
    //        fpm.add(createAggressiveDCEPass());
    //        fpm.add(createCFGSimplificationPass());
    //
    //        // added passes...
    //        fpm.add(createPromoteMemoryToRegisterPass()); // mem2reg pass
    //        fpm.add(createAggressiveDCEPass());
    //
    //        // custom added passes
    //        // ==> Tuplex is memcpy heavy, i.e. optimize!
    //        fpm.add(createMemCpyOptPass()); // !!! use this pass for sure !!! It's quite expensive first, but it pays off big time.
    //    }
    //
    //    void optimizePipelineI(llvm::Module& mod) {
    //        // Step 1: optimize functions
    //        auto fpm = llvm::make_unique<legacy::FunctionPassManager>(&mod);
    //        assert(fpm.get());
    //
    //        generateFunctionPassesI(*fpm.get());
    //        fpm->doInitialization();
    //
    //        // run function passes over each function in the module
    //        for(Function& f: mod.getFunctionList())
    //            fpm->run(f);
    //
    //        // on current master, module optimizations are deactivated. Inlining seems to worsen things!
    //        // // Step 2: optimize over whole module
    //        // // Module passes (function inlining)
    //        // legacy::PassManager pm;
    //        // // inline functions now
    //        // pm.add(createFunctionInliningPass());
    //        // pm.add(createDeadArgEliminationPass());
    //        // pm.run(mod);
    //    }

    void optimizePipelineII(llvm::legacy::FunctionPassManager& fpm) {
        // inspired from https://courses.engr.illinois.edu/cs426/fa2015/Project/mp4.pdf
        // i.e.
        // simplify-cfg
        // instcombine
        // => module pass: inline
        // => module pass: global dce
        // instcombine
        // simplify-cfg
        // sclarrepl?
        // mem2reg (should use sora?)
        // sccp ? what is that?
        // adce (aggressive dead code elimination)
        // instcombine
        // dce
        // simplify-cfg
        // => module pass: deadarg
        // => module pass: global dce

        // TODO: to tune, need to add loop passes + memcpy opt somewhere in this pipeline...
        // also, constant propagation might be a good idea...
        // because attributes are used not always, a good idea might be to run functionattrs as well

        fpm.add(createCFGSimplificationPass());
        fpm.add(createInstructionCombiningPass(true));
        fpm.add(createAggressiveInstCombinerPass()); // run this as last one b.c. it's way more complex than the others...
        // inline?
        fpm.add(createGlobalDCEPass());
    }


     static void Optimize(llvm::Module& M, unsigned OptLevel, unsigned OptSize) {

      llvm::Triple Triple{llvm::sys::getProcessTriple()};

      llvm::PassManagerBuilder Builder;
      Builder.OptLevel = OptLevel;
      Builder.SizeLevel = OptSize;
      Builder.LibraryInfo = new llvm::TargetLibraryInfoImpl(Triple);
      Builder.Inliner = llvm::createFunctionInliningPass(OptLevel, OptSize, false);
         Builder.SLPVectorize = false; // true; // enable vectorization! --> not suitable for large parse functions. I.e., deactivate for parse function, but potentially apply to individual, small UDF functions (?)

      std::unique_ptr<llvm::TargetMachine> TM = GetHostTargetMachine();
      assert(TM);
      TM->adjustPassManager(Builder);

      llvm::legacy::PassManager MPM;
      MPM.add(llvm::createTargetTransformInfoWrapperPass(TM->getTargetIRAnalysis()));
      Builder.populateModulePassManager(MPM);

    #ifndef NDEBUG
      MPM.add(llvm::createVerifierPass());
    #endif

      Builder.populateModulePassManager(MPM);

      MPM.run(M);
    }

    __attribute__((no_sanitize_address)) std::string LLVMOptimizer::optimizeIR(const std::string &llvmIR) {

        // manual reading
        using namespace llvm;
        // first parse IR. It would be also an alternative to directly the LLVM Module from the ModuleBuilder class,
        // however if something went wrong there, memory errors would occur. Better is to first transform to a string
        // and then parse it because LLVM will validate the IR on the way.
        LLVMContext context;
        SMDiagnostic err; // create an SMDiagnostic instance
        std::unique_ptr<MemoryBuffer> buff = MemoryBuffer::getMemBuffer(llvmIR);
        std::unique_ptr<Module> mod = parseIR(buff->getMemBufferRef(), err, context); // use err directly

        // check if any errors occured during module parsing
        if(nullptr == mod.get()) {
            // print errors
            Logger::instance().logger("LLVM Optimizer").error("could not compile module:\n>>>>>>>>>>>>>>>>>\n"
                                                            + core::withLineNumbers(llvmIR)
                                                            + "\n<<<<<<<<<<<<<<<<<");
            Logger::instance().logger("LLVM Optimizer").error("line " + std::to_string(err.getLineNo()) + ": " + err.getMessage().str());
            return llvmIR;
        }

        // Some interesting links for LLVM passes
        // @TODO: experiment a bit with this
        // other pass order:
        // simpplifycfg pass
        // sroa
        // earlycsepass
        // lowerexpectinstrinsicpass
        // check out https://stackoverflow.com/questions/15548023/clang-optimization-levels
        // maybe this here works?
        // https://stackoverflow.com/questions/51934964/function-optimization-pass?rq=1
        // need to tune passes a bit more
        // https://llvm.org/docs/Passes.html#passes-sccp
        // check out https://llvm.org/docs/Passes.html
        // note: test carefully when adding passes!
        // sometimes the codegen & passes won't work together!
        // ==> checkout https://blog.regehr.org/archives/1603 super helpful

        //optimizePipelineI(*mod);

        // use level 2 because it's faster than 3 and produces pretty much the same result anyways...
        Optimize(*mod, 2, 0);

        // check out https://github.com/apache/impala/blob/master/be/src/codegen/llvm-codegen.cc


        // @TODO: this is slow, better exchange with llvm bitcode
        std::string ir = "";
        llvm::raw_string_ostream os(ir);
        os.flush();
        mod->print(os, nullptr);
        return ir;
    }

    void LLVMOptimizer::optimizeModule(llvm::Module &mod) {
        // OptLevel 3, SizeLevel 0
       // Optimize(mod, 3, 0);
        
        // Optimize(mod, 2, 0);

        // // perform some basic passes (for fast opt) -> defer complex logic to general-case.
        optimizePipelineI(mod);

    }

    // use https://github.com/jmmartinez/easy-just-in-time/blob/master/runtime/Function.cpp
    // static void Optimize(llvm::Module& M, const char* Name, const easy::Context& C, unsigned OptLevel, unsigned OptSize) {
    //
    //  llvm::Triple Triple{llvm::sys::getProcessTriple()};
    //
    //  llvm::PassManagerBuilder Builder;
    //  Builder.OptLevel = OptLevel;
    //  Builder.SizeLevel = OptSize;
    //  Builder.LibraryInfo = new llvm::TargetLibraryInfoImpl(Triple);
    //  Builder.Inliner = llvm::createFunctionInliningPass(OptLevel, OptSize, false);
    //
    //  std::unique_ptr<llvm::TargetMachine> TM = GetHostTargetMachine();
    //  assert(TM);
    //  TM->adjustPassManager(Builder);
    //
    //  llvm::legacy::PassManager MPM;
    //  MPM.add(llvm::createTargetTransformInfoWrapperPass(TM->getTargetIRAnalysis()));
    //  MPM.add(easy::createContextAnalysisPass(C));
    //  MPM.add(easy::createInlineParametersPass(Name));
    //  Builder.populateModulePassManager(MPM);
    //  MPM.add(easy::createDevirtualizeConstantPass(Name));
    //
    //#ifdef NDEBUG
    //  MPM.add(llvm::createVerifierPass());
    //#endif
    //
    //  Builder.populateModulePassManager(MPM);
    //
    //  MPM.run(M);
    //}
}
