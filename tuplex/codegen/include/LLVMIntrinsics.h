//
// Created by leonhards on 5/17/22.
//

#ifndef TUPLEX_LLVMINTRINSICS_H
#define TUPLEX_LLVMINTRINSICS_H

#include <llvm/IR/Intrinsics.h>
#if LLVM_VERSION_MAJOR > 9
#include <llvm/IR/IntrinsicsX86.h>
#endif

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
        // works like this: llvm::Intrinsic::ID::ceil
        // x86_sse42_pcmpistri128=Intrinsic::x86_sse42_pcmpistri128;

        struct LLVMIntrinsic {
            static const llvm::Intrinsic::ID sin = llvm::Intrinsic::ID::sin;
            static const llvm::Intrinsic::ID cos = llvm::Intrinsic::ID::cos;
            static const llvm::Intrinsic::ID sqrt = llvm::Intrinsic::ID::sqrt;
            static const llvm::Intrinsic::ID exp = llvm::Intrinsic::ID::exp;
            static const llvm::Intrinsic::ID log = llvm::Intrinsic::ID::log;
            static const llvm::Intrinsic::ID log2 = llvm::Intrinsic::ID::log2;
            static const llvm::Intrinsic::ID log10 = llvm::Intrinsic::ID::log10;
            static const llvm::Intrinsic::ID pow = llvm::Intrinsic::ID::pow;
            static const llvm::Intrinsic::ID ceil = llvm::Intrinsic::ID::ceil;
            static const llvm::Intrinsic::ID fabs = llvm::Intrinsic::ID::fabs;
            // note, for ARM different intrinsic is necessary!
            static const llvm::Intrinsic::ID x86_sse42_pcmpistri128 = llvm::Intrinsic::ID::x86_sse42_pcmpistri128;
        };
#endif
    }
}

#endif //TUPLEX_LLVMINTRINSICS_H
