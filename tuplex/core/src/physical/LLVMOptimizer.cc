//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/LLVMOptimizer.h>

#include <llvm/Analysis/CallGraph.h>
#include <llvm/Analysis/CallGraphSCCPass.h>
#include <llvm/Analysis/LoopPass.h>
#include <llvm/Analysis/RegionPass.h>
#include <llvm/Analysis/TargetLibraryInfo.h>
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/Bitcode/BitcodeWriterPass.h>
#include <llvm/CodeGen/TargetPassConfig.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/IR/IRPrintingPasses.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/LegacyPassNameParser.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/InitializePasses.h>
#include <llvm/LinkAllIR.h>
#include <llvm/LinkAllPasses.h>
#include <llvm/Support/Debug.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/ManagedStatic.h>
#include <llvm/Support/PluginLoader.h>
#include <llvm/Support/PrettyStackTrace.h>
#include <llvm/Support/Signals.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/SystemUtils.h>

#if (LLVM_VERSION_MAJOR < 17)
#include <llvm/ADT/Triple.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#else
#include <llvm/TargetParser/Triple.h>
#include <llvm/TargetParser/SubtargetFeature.h>
#endif

#if (LLVM_VERSION_MAJOR < 14)
#include <llvm/Support/TargetRegistry.h>
#else
#include <llvm/MC/TargetRegistry.h>
#endif

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/ToolOutputFile.h>
#include <llvm/Support/YAMLTraits.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <algorithm>
#include <memory>
#include <Logger.h>
#include <Utils.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/Analysis/LoopAnalysisManager.h>
#include <llvm/Passes/PassBuilder.h>

using namespace llvm;

namespace tuplex {

    LLVMOptimizer::LLVMOptimizer() : _logger(Logger::instance().logger("optimizer")) {
    }


    static std::unique_ptr<llvm::TargetMachine> GetHostTargetMachine() {
        std::unique_ptr<llvm::TargetMachine> TM(llvm::EngineBuilder().selectTarget());
        return TM;
    }

    void optimizePipelineII(llvm::legacy::FunctionPassManager &fpm) {
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

        //fpm.add(createCFGSimplificationPass());
        //fpm.add(createInstructionCombiningPass(true));
        //fpm.add(createAggressiveInstCombinerPass()); // run this as last one b.c. it's way more complex than the others...
        // inline?
        //fpm.add(createGlobalDCEPass());
    }


    static void Optimize(llvm::Module &M, unsigned OptLevel, unsigned OptSize) {
        using namespace llvm;

        // this is based on the new PassBuilder
        // https://llvm.org/docs/NewPassManager.html
        // and https://blog.llvm.org/posts/2021-03-26-the-new-pass-manager/

        llvm::Triple Triple{llvm::sys::getProcessTriple()};

        // Create the analysis managers.
        LoopAnalysisManager LAM;
        FunctionAnalysisManager FAM;
        CGSCCAnalysisManager CGAM;
        ModuleAnalysisManager MAM;

        // Create the new pass manager builder.
        // Take a look at the PassBuilder constructor parameters for more
        // customization, e.g. specifying a TargetMachine or various debugging
        // options.
        PassBuilder PB;

        // Register all the basic analyses with the managers.
        PB.registerModuleAnalyses(MAM);
        PB.registerCGSCCAnalyses(CGAM);
        PB.registerFunctionAnalyses(FAM);
        PB.registerLoopAnalyses(LAM);
        PB.crossRegisterProxies(LAM, FAM, CGAM, MAM);

        // Create the pass manager.
        // This one corresponds to a typical -O2 optimization pipeline.
#if (LLVM_VERSION_MAJOR < 14)
        auto opt_level = llvm::PassBuilder::OptimizationLevel::O2;
#else
        auto opt_level = OptimizationLevel::O2;
#endif
        ModulePassManager MPM = PB.buildPerModuleDefaultPipeline(opt_level);

        // Optimize the IR!F
        MPM.run(M, MAM);
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
        if (nullptr == mod.get()) {
            // print errors
            Logger::instance().logger("LLVM Optimizer").error("could not compile module:\n>>>>>>>>>>>>>>>>>\n"
                                                              + core::withLineNumbers(llvmIR)
                                                              + "\n<<<<<<<<<<<<<<<<<");
            Logger::instance().logger("LLVM Optimizer").error(
                    "line " + std::to_string(err.getLineNo()) + ": " + err.getMessage().str());
            return llvmIR;
        }

        // use level 2 because it's faster than 3 and produces pretty much the same result anyways...
        Optimize(*mod, 2, 0);

        // @TODO: this is slow, better exchange with llvm bitcode
        std::string ir = "";
        llvm::raw_string_ostream os(ir);
        os.flush();
        mod->print(os, nullptr);
        return ir;
    }

    void LLVMOptimizer::optimizeModule(llvm::Module &mod) {
        // OptLevel 3, SizeLevel 0
        Optimize(mod, 3, 0);
    }
}