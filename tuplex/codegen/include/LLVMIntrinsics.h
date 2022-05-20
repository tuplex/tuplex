//
// Created by leonhards on 5/17/22.
//

#ifndef TUPLEX_LLVMINTRINSICS_H
#define TUPLEX_LLVMINTRINSICS_H

#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/IntrinsicsX86.h>

// in this commit https://github.com/llvm/llvm-project/commit/5d986953c8b917bacfaa1f800fc1e242559f76be, the intrinsic structure was changed
// hence, list here intrinsics
namespace tuplex {
    namespace codegen {
#if LLVM_VERSION_MAJOR > 9
        enum LLVMIntrinsic : llvm::Intrinsic::ID {
            sin = llvm::Intrinsic::IndependentIntrinsics::sin,
            cos = llvm::Intrinsic::IndependentIntrinsics::cos,
            sqrt = llvm::Intrinsic::IndependentIntrinsics::sqrt,
            exp = llvm::Intrinsic::IndependentIntrinsics::exp,
            log = llvm::Intrinsic::IndependentIntrinsics::log,
            log2 = llvm::Intrinsic::IndependentIntrinsics::log2,
            log10 = llvm::Intrinsic::IndependentIntrinsics::log10,
            pow = llvm::Intrinsic::IndependentIntrinsics::pow,
            ceil = llvm::Intrinsic::IndependentIntrinsics::ceil,
            fabs = llvm::Intrinsic::IndependentIntrinsics::fabs,
            // note, for ARM different intrinsic is necessary!
            x86_sse42_pcmpistri128=llvm::Intrinsic::X86Intrinsics::x86_sse42_pcmpistri128
        };
#else
#error "need to add different pattern"
        // works like this: llvm::Intrinsic::ID::ceil
        // x86_sse42_pcmpistri128=Intrinsic::x86_sse42_pcmpistri128;
#endif
    }
}

#endif //TUPLEX_LLVMINTRINSICS_H
