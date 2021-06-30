//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LLVMOPTIMIZER_H
#define TUPLEX_LLVMOPTIMIZER_H

#include <string>
#include <Logger.h>
#include <llvm/IR/Module.h>

// should include memcpy optimizer https://llvm.org/doxygen/MemCpyOptimizer_8cpp_source.html

namespace tuplex {

    /*!
     * class to optimize LLVM IR code. Currently stubbed with code form the LLVM opt tool. Should later also include
     * the polly project.
     */
    class LLVMOptimizer {
    private:
        MessageHandler& _logger;
    public:

        LLVMOptimizer();

        /*!
         * takes LLVM IR as string and applies O3 passes as in the llvm opt tool
         * @param ir LLVM IR as string
         * @return LLVM IR as string
         */
        std::string optimizeIR(const std::string&);

        void optimizeModule(llvm::Module& mod);
    };
}

#endif //TUPLEX_LLVMOPTIMIZER_H