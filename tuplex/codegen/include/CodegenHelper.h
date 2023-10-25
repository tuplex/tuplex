//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CODEGENHELPER_H
#define TUPLEX_CODEGENHELPER_H

#include <llvm/IR/Value.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/raw_ostream.h>
#include <string>
#include <TypeSystem.h>
#include <Field.h>

#if LLVM_VERSION_MAJOR > 9
#include <llvm/AsmParser/Parser.h>
#endif

#if LLVM_VERSION_MAJOR >= 9
// LLVM9 fix
#include <llvm/Target/TargetMachine.h>
#endif


#if LLVM_VERSION_MAJOR > 8
// for parsing string to threadsafemodule (llvm9+ ORC APIs)
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/IR/Verifier.h>
#endif


// builder and codegen funcs
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Value.h>
#include <llvm/IR/Type.h>
#include <unordered_map>

#include <llvm/IR/CFG.h>

#include <Base.h>

namespace tuplex {
    namespace codegen {

        /*!
         * helper class to build LLVM IR. Added because IRBuilder was made non-copyable in llvm source base
         */
         class IRBuilder {
         public:
             IRBuilder() : _llvm_builder(nullptr) {}

             IRBuilder(llvm::IRBuilder<>& llvm_builder);
             IRBuilder(const llvm::IRBuilder<>& llvm_builder);
             IRBuilder(llvm::BasicBlock* bb);

             IRBuilder(llvm::LLVMContext& ctx);

             // copy
             IRBuilder(const IRBuilder& other);

             ~IRBuilder();

             llvm::LLVMContext& getContext() const {
                 return get_or_throw().getContext();
             }

             /*!
              * creates a new builder returning a builder for the first block.
              * @param insertAtEnd if true, sets the IR builder insert point at the end of the first basic block in the function. If false, at start.
              * @return
              */
            IRBuilder firstBlockBuilder(bool insertAtEnd=true) const;

            // CreateAlloca (Type *Ty, unsigned AddrSpace, Value *ArraySize=nullptr, const Twine &Name=""
            inline llvm::Value* CreateAlloca(llvm::Type *type, const std::string& name="") {
                return get_or_throw().CreateAlloca(type, 0, nullptr, name);
            }

             inline llvm::Value* CreateAlloca(llvm::Type *type, unsigned AddrSpace, llvm::Value* ArraySize=nullptr, const std::string& name="") const {
                 assert(type);
                 return get_or_throw().CreateAlloca(type, AddrSpace, ArraySize, name);
             }

            inline llvm::Value* CreateAlloca(llvm::Type *type) const {
                assert(type);
                return get_or_throw().CreateAlloca(type);
            }

            // StoreInst * 	CreateStore (Value *Val, Value *Ptr, bool isVolatile=false)
            inline llvm::Value* CreateStore(llvm::Value* Val, llvm::Value* Ptr, bool isVolatile=false) const {

#ifndef NDEBUG
                // pointer check
                if(Val->getType()->getPointerTo() != Ptr->getType()) {
                    throw std::runtime_error("attempting to store value of incompatible llvm type to llvm pointer");
                }
#endif

                return get_or_throw().CreateStore(Val, Ptr, isVolatile);
            }

            inline llvm::BasicBlock* GetInsertBlock() const {
                return get_or_throw().GetInsertBlock();
            }

             inline llvm::Type* getInt1Ty() const {
                 return get_or_throw().getInt1Ty();
             }
             inline llvm::Type* getInt8Ty() const {
                 return get_or_throw().getInt8Ty();
             }
            inline llvm::Type* getInt32Ty() const {
                return get_or_throw().getInt32Ty();
            }
             inline llvm::Type* getInt64Ty() const {
                 return get_or_throw().getInt64Ty();
             }

            inline llvm::Value* CreateICmp(llvm::CmpInst::Predicate P, llvm::Value *LHS, llvm::Value *RHS,
                                           const std::string& name="") const {
                return get_or_throw().CreateICmp(P, LHS, RHS, name);
            }

            inline llvm::Value *CreateICmpEQ(llvm::Value *LHS, llvm::Value *RHS, const std::string &name = "") const {
                return CreateICmp(llvm::ICmpInst::ICMP_EQ, LHS, RHS, name);
            }
             inline llvm::Value *CreateICmpNE(llvm::Value *LHS, llvm::Value *RHS, const std::string &name = "") const {
                 return CreateICmp(llvm::ICmpInst::ICMP_NE, LHS, RHS, name);
             }

             inline llvm::Value *CreatePointerCast(llvm::Value *V, llvm::Type *DestTy,
                                      const std::string &Name = "") const {
                return get_or_throw().CreatePointerCast(V, DestTy, Name);
            }

            inline llvm::Value *CreateBitOrPointerCast(llvm::Value *V, llvm::Type *DestTy,
                                          const std::string &Name = "") const {
                 return get_or_throw().CreateBitOrPointerCast(V, DestTy, Name);
             }

            inline llvm::Value *CreateBitCast(llvm::Value *V, llvm::Type *DestTy,
                                 const std::string &Name = "") const  {
                return get_or_throw().CreateCast(llvm::Instruction::BitCast, V, DestTy, Name);
            }

            inline llvm::Value *CreateIntCast(llvm::Value *V, llvm::Type *DestTy, bool isSigned,
                                 const std::string &Name = "") const {
                 return get_or_throw().CreateIntCast(V, DestTy, isSigned, Name);
             }

            inline llvm::Value *CreateLShr(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                              bool isExact = false) const {
               return get_or_throw().CreateLShr(LHS, RHS, Name);
            }

            inline llvm::Value *CreateLShr(llvm::Value *LHS, const llvm::APInt &RHS, const std::string &Name = "",
                              bool isExact = false) const {
                return get_or_throw().CreateLShr(LHS, llvm::ConstantInt::get(LHS->getType(), RHS), Name, isExact);
            }

            inline llvm::Value *CreateLShr(llvm::Value *LHS, uint64_t RHS, const std::string &Name = "",
                              bool isExact = false) const {
                return get_or_throw().CreateLShr(LHS, llvm::ConstantInt::get(LHS->getType(), RHS), Name, isExact);
            }

            inline llvm::Value *CreateLifetimeStart(llvm::Value *Ptr, llvm::ConstantInt *Size = nullptr) const {
                 return get_or_throw().CreateLifetimeStart(Ptr, Size);
             }

            inline llvm::Value *CreateLifetimeEnd(llvm::Value *Ptr, llvm::ConstantInt *Size = nullptr) const {
                 return get_or_throw().CreateLifetimeEnd(Ptr, Size);
             }

            inline llvm::Value *CreateExtractValue(llvm::Value *Agg,
                                       llvm::ArrayRef<unsigned> Idxs,
                                       const std::string &Name = "") const {
                return get_or_throw().CreateExtractValue(Agg, Idxs, Name);
            }

            inline llvm::Value *CreateSRem(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                return get_or_throw().CreateSRem(LHS, RHS, Name);
            }

            inline llvm::Value *CreateFRem(llvm::Value *L, llvm::Value *R, const std::string &Name = "",
                                           llvm::MDNode *FPMD = nullptr) const {
                 return get_or_throw().CreateFRem(L, R, Name, FPMD);
             }

            inline llvm::Value *CreateInsertValue(llvm::Value *Agg, llvm::Value *Val,
                                          llvm::ArrayRef<unsigned> Idxs,
                                          const std::string &Name = "") const {
                return get_or_throw().CreateInsertValue(Agg, Val, Idxs, Name);
            }

            inline llvm::Value *CreateInsertElement(llvm::Value *Vec, llvm::Value *NewElt, llvm::Value *Idx,
                                       const std::string &Name = "") const {
                return get_or_throw().CreateInsertElement(Vec, NewElt, Idx, Name);
             }

            inline llvm::Value *CreateInsertElement(llvm::Value *Vec, llvm::Value *NewElt, uint64_t Idx,
                                                    const std::string &Name = "") const {
                return get_or_throw().CreateInsertElement(Vec, NewElt, Idx, Name);
            }

            inline llvm::Value *CreateICmpUGT(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_UGT, LHS, RHS, Name);
            }

            inline llvm::Value *CreateICmpUGE(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_UGE, LHS, RHS, Name);
            }

            inline llvm::Value *CreateICmpULT(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_ULT, LHS, RHS, Name);
            }

            inline llvm::Value *CreateICmpULE(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_ULE, LHS, RHS, Name);
            }


             inline llvm::Value *CreateICmpSGT(llvm::Value *LHS, llvm::Value *RHS, const std::string& Name = "") const {
                 return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_SGT, LHS, RHS, Name);
             }
             inline llvm::Value *CreateICmpSGE(llvm::Value *LHS, llvm::Value *RHS, const std::string& Name = "") const {
                 return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_SGE, LHS, RHS, Name);
             }

            inline llvm::Value *CreateICmpSLT(llvm::Value *LHS, llvm::Value *RHS, const std::string& Name = "") const {
                return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_SLT, LHS, RHS, Name);
            }
            inline llvm::Value *CreateICmpSLE(llvm::Value *LHS, llvm::Value *RHS, const std::string& Name = "") const {
                return get_or_throw().CreateICmp(llvm::ICmpInst::ICMP_SLE, LHS, RHS, Name);
            }

             inline llvm::Value *CreateFNeg(llvm::Value *V, const std::string& Name = "",
                                            llvm::MDNode *FPMathTag = nullptr) const {
                 return get_or_throw().CreateFNeg(V, Name, FPMathTag);
            }
            inline llvm::Value *CreateNeg(llvm::Value *V, const std::string& Name = "",
                                  bool HasNUW = false, bool HasNSW = false) const {
                return get_or_throw().CreateNeg(V, Name, HasNUW, HasNSW);
            }
             inline llvm::Value *CreateXor(llvm::Value *LHS, llvm::Value *RHS, const std::string& Name = "") const {
                 return get_or_throw().CreateXor(LHS, RHS, Name);
             }

             inline llvm::Value *CreateNot(llvm::Value *V, const std::string &Name = "") const {
                 return get_or_throw().CreateNot(V, Name);
            }

            inline llvm::Value* CreateOr(llvm::Value *LHS, llvm::Value *RHS, const std::string &name = "") const {
                return get_or_throw().CreateOr(LHS, RHS, name);
            }

            inline llvm::Value* CreateCondBr(llvm::Value *Cond,
                                             llvm::BasicBlock *True,
                                             llvm::BasicBlock *False,
                                             llvm::MDNode *BranchWeights = nullptr,
                                             llvm::MDNode *Unpredictable = nullptr) const {
                return get_or_throw().CreateCondBr(Cond, True, False, BranchWeights, Unpredictable);
            }

            inline llvm::Value* CreateBr(llvm::BasicBlock *Dest) const {
                return get_or_throw().CreateBr(Dest);
            }

            inline llvm::IndirectBrInst *CreateIndirectBr(llvm::Value *Addr, unsigned NumDests = 10) const {
                 return get_or_throw().CreateIndirectBr(Addr, NumDests);
             }

            inline llvm::SwitchInst *CreateSwitch(llvm::Value *V, llvm::BasicBlock *Dest, unsigned NumCases = 10,
                                                  llvm::MDNode *BranchWeights = nullptr,
                                                  llvm::MDNode *Unpredictable = nullptr) {
                return get_or_throw().CreateSwitch(V, Dest, NumCases, BranchWeights, Unpredictable);
            }

            inline void SetInsertPoint(llvm::BasicBlock *TheBB) const {
                assert(TheBB);
                get_or_throw().SetInsertPoint(TheBB);
            }

            inline void SetInsertPoint(llvm::Instruction* inst) const {
                 assert(inst);
                 get_or_throw().SetInsertPoint(inst);
             }

            llvm::BasicBlock::iterator GetInsertPoint() const {
                 return get_or_throw().GetInsertPoint();
             }

            void SetInstDebugLocation(llvm::Instruction *I) const {
                 return get_or_throw().SetInstDebugLocation(I);
             }

             inline llvm::Value* CreateAdd(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                                           bool HasNUW = false, bool HasNSW = false) const {
                 return get_or_throw().CreateAdd(LHS, RHS, Name, HasNUW, HasNSW);
             }

            inline llvm::Value *CreateNUWAdd(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                 return get_or_throw().CreateNUWAdd(LHS, RHS, Name);
             }

            inline llvm::Value* CreateSub(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                                          bool HasNUW = false, bool HasNSW = false) const {
                return get_or_throw().CreateSub(LHS, RHS, Name, HasNUW, HasNSW);
            }

            inline llvm::Value *CreateMul(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                              bool HasNUW = false, bool HasNSW = false) const {
                return get_or_throw().CreateMul(LHS, RHS, Name, HasNUW, HasNSW);
            }

            // integer shift
            inline llvm::Value *CreateShl(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                                    bool HasNUW = false, bool HasNSW = false) const {
                return get_or_throw().CreateShl(LHS, RHS, Name, HasNUW, HasNSW);
            }

            inline llvm::Value *CreateShl(llvm::Value *LHS, uint64_t RHS, const std::string &Name = "",
                                          bool HasNUW = false, bool HasNSW = false) const {
                return get_or_throw().CreateShl(LHS, RHS, Name, HasNUW, HasNSW);
            }

             inline llvm::Value *CreateAShr(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                               bool isExact = false) const {
                return get_or_throw().CreateAShr(LHS, RHS, Name, isExact);
            }

            // floating point operations
            // FAdd, FSub, FDiv, FMul
            inline llvm::Value *CreateFAdd(llvm::Value *L, llvm::Value *R, const std::string &Name = "",
                              llvm::MDNode *FPMD = nullptr) const {
                return get_or_throw().CreateFAdd(L, R, Name, FPMD);
            }
            inline llvm::Value *CreateFSub(llvm::Value *L, llvm::Value *R, const std::string &Name = "",
                               llvm::MDNode *FPMD = nullptr) const {
                return get_or_throw().CreateFSub(L, R, Name, FPMD);
            }
            inline llvm::Value *CreateFDiv(llvm::Value *L, llvm::Value *R, const std::string &Name = "",
                               llvm::MDNode *FPMD = nullptr) const {
                return get_or_throw().CreateFDiv(L, R, Name, FPMD);
            }

             inline llvm::Value *CreateFMul(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                                            llvm::MDNode *FPMD = nullptr) const {
                 return get_or_throw().CreateFMul(LHS, RHS, Name, FPMD);
             }

            inline llvm::Value *CreateSDiv(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                              bool isExact = false) const {
                 return get_or_throw().CreateSDiv(LHS, RHS, Name, isExact);
             }

            inline llvm::Value *CreateUDiv(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                              bool isExact = false) const { return get_or_throw().CreateUDiv(LHS, RHS, Name, isExact); }

            inline llvm::Value *CreateGEP(llvm::Type *Ty, llvm::Value *Ptr, llvm::ArrayRef<llvm::Value *> IdxList,
                              const std::string &Name = "") const {
                return get_or_throw().CreateGEP(Ty, Ptr, IdxList, Name);
            }

            // helper function to simulate GEP using bytes
            inline llvm::Value *MovePtrByBytes(llvm::Value* Ptr, llvm::Value* num_bytes, const std::string &Name = "") const {
                 assert(num_bytes->getType() == getInt64Ty() || num_bytes->getType() == getInt32Ty());
                 assert(Ptr->getType()->isPointerTy());
                 return get_or_throw().CreateGEP(getInt8Ty(), Ptr, {num_bytes}, Name);
            }

            inline llvm::Value *MovePtrByBytes(llvm::Value* Ptr, int64_t num_bytes, const std::string &Name = "") const {
                 return MovePtrByBytes(Ptr, llvm::Constant::getIntegerValue(getInt64Ty(), llvm::APInt(64, num_bytes)), Name);
           }


            inline llvm::Value *CreateStructGEP(llvm::Value *Ptr, llvm::Type* pointee_type, unsigned Idx,
                                                const std::string &Name = "") const {
#if LLVM_VERSION_MAJOR < 9
                // compatibility
                return get_or_throw().CreateConstInBoundsGEP2_32(nullptr, ptr, 0, idx, Name);
#else
                assert(Ptr->getType()->isPointerTy());
                assert(pointee_type);
                return get_or_throw().CreateStructGEP(pointee_type, Ptr, Idx, Name);
#endif
            }

            inline llvm::Value *CreateFCmpONE(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "",
                                              llvm::MDNode *FPMathTag = nullptr) const {return get_or_throw().CreateFCmpONE(LHS, RHS, Name, FPMathTag); }

            inline llvm::Value *CreateConstInBoundsGEP2_64(llvm::Value *Ptr, llvm::Type* Ty, uint64_t Idx0,
                                                           uint64_t Idx1, const std::string &Name = "") const {
                using namespace llvm;

                assert(Ty); // can't be nullptr, will trigger an error else...
                return get_or_throw().CreateConstGEP2_64(Ty, Ptr, Idx0, Idx1, Name);
            }

            inline llvm::Value *CreatePtrToInt(llvm::Value *V, llvm::Type *DestTy,
                                  const std::string &Name = "") { return get_or_throw().CreatePtrToInt(V, DestTy, Name); }

            inline llvm::Value *CreateIntToPtr(llvm::Value *V, llvm::Type *DestTy,
                                  const std::string &Name = "") { return get_or_throw().CreateIntToPtr(V, DestTy, Name); }


            inline llvm::CallInst *CreateCall(llvm::FunctionType *FTy, llvm::Value *Callee,

#if (LLVM_VERSION_MAJOR >= 10)
                                        llvm::ArrayRef<llvm::Value *> Args = std::nullopt,
#else
                    llvm::ArrayRef<llvm::Value *> Args = {},
#endif
                                        const std::string &Name = "",
                                        llvm::MDNode *FPMathTag = nullptr) const {
                 assert(FTy);
                return get_or_throw().CreateCall(FTy, Callee, Args, Name, FPMathTag);
            }

            inline llvm::CallInst* CreateCall(llvm::Value* func_value,
#if (LLVM_VERSION_MAJOR >= 10)
                    llvm::ArrayRef<llvm::Value *> Args = std::nullopt,
#else
                                              llvm::ArrayRef<llvm::Value *> Args = {},
#endif
                                              const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                 if(llvm::isa<llvm::Function>(func_value))
                     throw std::runtime_error("trying to call a non-function llvm value");
                 auto func = llvm::cast<llvm::Function>(func_value);
                 return CreateCall(func->getFunctionType(), func, Args, Name,
                                  FPMathTag);
            }

            inline llvm::CallInst* CreateCall(llvm::Function* func,
#if (LLVM_VERSION_MAJOR >= 10)
                    llvm::ArrayRef<llvm::Value *> Args = std::nullopt,
#else
                                              llvm::ArrayRef<llvm::Value *> Args = {},
#endif
                                              const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return CreateCall(func->getFunctionType(), func, Args, Name,
                                  FPMathTag);
            }

            inline llvm::CallInst *CreateCall(llvm::FunctionCallee Callee,
#if (LLVM_VERSION_MAJOR >= 10)
                    llvm::ArrayRef<llvm::Value *> Args = std::nullopt,
#else
                                              llvm::ArrayRef<llvm::Value *> Args = {},
#endif
                                        const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return CreateCall(Callee.getFunctionType(), Callee.getCallee(), Args, Name,
                                  FPMathTag);
            }

             inline llvm::LoadInst *CreateLoad(llvm::Type *Ty, llvm::Value *Ptr, const char *Name) const {
                 assert(Ty);
#if LLVM_VERSION_MAJOR <= 9
                 // check type compatibility
                 assert(Ptr->getType() == Ty->getPointerTo());

                 return get_or_throw().CreateLoad(Ty, Ptr, Name);
#elif LLVM_VERSION_MAJOR > 9
                 return get_or_throw().CreateAlignedLoad(Ty, Ptr, llvm::MaybeAlign(), Name);
#else
                return get_or_throw().CreateLoad(Ty, Ptr, Name);
#endif
             }

             inline llvm::LoadInst *CreateLoad(llvm::Type *Ty, llvm::Value *Ptr, const std::string &Name = "") const {
                 assert(Ty);
#if LLVM_VERSION_MAJOR <= 9
                 // check type compatibility
                 assert(Ptr->getType() == Ty->getPointerTo());

                 return get_or_throw().CreateLoad(Ty, Ptr, Name);
#elif LLVM_VERSION_MAJOR > 9
                 return get_or_throw().CreateAlignedLoad(Ty, Ptr, llvm::MaybeAlign(), Name);
#else
                return get_or_throw().CreateLoad(Ty, Ptr, Name);
#endif
             }


            inline llvm::Value* CreateInBoundsGEP(llvm::Value* Ptr, llvm::Type* pointee_type, llvm::Value* Idx) {
                 return get_or_throw().CreateInBoundsGEP(pointee_type, Ptr, {Idx});
             }

            inline llvm::Value *CreateUnaryIntrinsic(llvm::Intrinsic::ID ID, llvm::Value *V,
                                                                         llvm::Instruction *FMFSource = nullptr,
                                                                         const std::string &Name = "") const {
                 return get_or_throw().CreateUnaryIntrinsic(ID, V, FMFSource, Name);
             }

            inline llvm::Value *CreateBinaryIntrinsic(llvm::Intrinsic::ID ID, llvm::Value *LHS,
                                                     llvm::Value* RHS,
                                                     llvm::Instruction *FMFSource = nullptr,
                                                     const std::string &Name = "") const {
                return get_or_throw().CreateBinaryIntrinsic(ID, LHS, RHS, FMFSource, Name);
            }


            inline llvm::Value* CreateFCmp(llvm::CmpInst::Predicate P, llvm::Value *LHS, llvm::Value *RHS,
                               const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return get_or_throw().CreateFCmp(P, LHS, RHS, Name, FPMathTag);
            }

            inline llvm::Value* CreateFCmpOEQ(llvm::Value *LHS, llvm::Value *RHS,
                                              const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return get_or_throw().CreateFCmpOEQ(LHS, RHS, Name, FPMathTag);
            }

            inline llvm::Value* CreateFCmpOLT(llvm::Value *LHS, llvm::Value *RHS,
                                           const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return get_or_throw().CreateFCmpOLT(LHS, RHS, Name, FPMathTag);
            }

            inline llvm::Value* CreateFCmpOLE(llvm::Value *LHS, llvm::Value *RHS,
                                              const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return get_or_throw().CreateFCmpOLE(LHS, RHS, Name, FPMathTag);
            }

            inline llvm::Value* CreateFCmpOGT(llvm::Value *LHS, llvm::Value *RHS,
                                              const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return get_or_throw().CreateFCmpOGT(LHS, RHS, Name, FPMathTag);
            }

            inline llvm::Value* CreateFCmpOGE(llvm::Value *LHS, llvm::Value *RHS,
                                              const std::string &Name = "", llvm::MDNode *FPMathTag = nullptr) const {
                return get_or_throw().CreateFCmpOGE(LHS, RHS, Name, FPMathTag);
            }

             inline llvm::Value *CreateFPToSI(llvm::Value *V, llvm::Type *DestTy, const std::string &Name = "") const {
                return get_or_throw().CreateFPToSI(V, DestTy, Name);
            }
             inline llvm::Value *CreateSIToFP(llvm::Value *V, llvm::Type *DestTy, const std::string &Name = "") const {
                return get_or_throw().CreateSIToFP(V, DestTy, Name);
            }

            // casts
            inline llvm::Value *CreateCast(llvm::Instruction::CastOps Op, llvm::Value *V, llvm::Type *DestTy,
                              const std::string &Name = "") const {
                return get_or_throw().CreateCast(Op, V, DestTy, Name);
             }

            //  Shl, AShr, ZExt
            inline llvm::Value *CreateZExt(llvm::Value *V, llvm::Type *DestTy, const std::string &Name = "") const {
                return get_or_throw().CreateZExt(V, DestTy, Name);
            }

            inline llvm::Value *CreateSExt(llvm::Value *V, llvm::Type *DestTy, const std::string &Name = "") const {
                return get_or_throw().CreateSExt(V, DestTy, Name);
            }

            inline llvm::Value *CreateFPExt(llvm::Value *V, llvm::Type *DestTy, const std::string &Name = "") const { return get_or_throw().CreateFPExt(V, DestTy, Name); }

             inline llvm::Value *CreateTrunc(llvm::Value *V, llvm::Type *DestTy, const std::string &Name = "") const {
                 return get_or_throw().CreateTrunc(V, DestTy, Name);
             }
             inline llvm::Value *CreateZExtOrTrunc(llvm::Value *V, llvm::Type *DestTy,
                                      const std::string &Name = "") const {
                return get_or_throw().CreateZExtOrTrunc(V, DestTy, Name);
            }
             inline llvm::Value *CreateAnd(llvm::Value *LHS, llvm::Value *RHS, const std::string &Name = "") const {
                return get_or_throw().CreateAnd(LHS, RHS, Name);
            }

             inline llvm::Value *CreateSelect(llvm::Value *C, llvm::Value *True, llvm::Value *False,
                                 const std::string &Name = "", llvm::Instruction *MDFrom = nullptr) const {
                return get_or_throw().CreateSelect(C, True, False, Name, MDFrom);
            }

            inline llvm::CallInst *CreateMemCpy(llvm::Value *Dst, unsigned DstAlign, llvm::Value *Src,
                                            unsigned SrcAlign, llvm::Value *Size,
                                            bool isVolatile = false, llvm::MDNode *TBAATag = nullptr,
                                            llvm::MDNode *TBAAStructTag = nullptr,
                                            llvm::MDNode *ScopeTag = nullptr,
                                            llvm::MDNode *NoAliasTag = nullptr) const {
#if LLVM_VERSION_MAJOR == 9
                return get_or_throw().CreateMemCpy(Dst, DstAlign, Src, SrcAlign, Size, isVolatile, TBAATag, TBAAStructTag, ScopeTag, NoAliasTag);
#elif LLVM_VERSION_MAJOR > 9
                return get_or_throw().CreateMemCpy(Dst, llvm::MaybeAlign(DstAlign), Src, llvm::MaybeAlign(SrcAlign), Size, isVolatile, TBAATag, TBAAStructTag, ScopeTag, NoAliasTag);
#else
                return get_or_throw().CreateMemCpy(Dst, Src, Size, SrcAlign);
#endif

            }

            inline llvm::PHINode* CreatePHI(llvm::Type* type, unsigned NumReservedValues, const std::string& twine="") const {
                 assert(type);
                 return get_or_throw().CreatePHI(type, NumReservedValues, twine);
             }

             // helpers
             inline llvm::Value *CreateIsNull(llvm::Value *Arg, const std::string &Name = "") const { return get_or_throw().CreateIsNull(Arg, Name); }

            inline llvm::Value *CreateIsNotNull(llvm::Value *Arg, const std::string &Name = "") const { return get_or_throw().CreateIsNotNull(Arg, Name); }

            inline llvm::Value *CreatePtrDiff(llvm::Type *ElemTy, llvm::Value *LHS, llvm::Value *RHS,
                                              const std::string &Name = "") const {
                assert(LHS->getType() == RHS->getType() && LHS->getType()->isPointerTy());
                assert(ElemTy);
#if (LLVM_VERSION_MAJOR < 14)
                return get_or_throw().CreatePtrDiff(LHS, RHS, Name);
#else
                return get_or_throw().CreatePtrDiff(ElemTy, LHS, RHS, Name);
#endif
            }

            llvm::Value *CreateRetVoid() const {
                return get_or_throw().CreateRetVoid();
            }

            llvm::Value *CreateRet(llvm::Value *V) const {
                return get_or_throw().CreateRet(V);
            }

            /*!
             * create runtime malloc (calling rtmalloc function)
             * @param size
             * @return allocated pointer
             */
            inline llvm::Value* malloc(llvm::Value *size) const {
                 assert(size);

                 auto& ctx = get_or_throw().getContext();
                 auto mod = get_or_throw().GetInsertBlock()->getParent()->getParent();

                 // make sure size_t is 64bit
                 static_assert(sizeof(size_t) == sizeof(int64_t), "sizeof must be 64bit compliant");
                 static_assert(sizeof(size_t) == 8, "sizeof must be 64bit wide");
                 assert(size->getType() == llvm::Type::getInt64Ty(ctx));


                 // create external call to rtmalloc function
                 auto func = mod->getOrInsertFunction("rtmalloc", llvm::Type::getInt8PtrTy(ctx, 0),
                                                                llvm::Type::getInt64Ty(ctx));
                 return get_or_throw().CreateCall(func, size);
             }

         inline llvm::Value* malloc(size_t size) const {
             auto& ctx = get_or_throw().getContext();
                auto i64_size =  llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, size));
                return malloc(i64_size);
            }

            inline llvm::Value *CreateGlobalStringPtr(const std::string &basicString) const {
                return get_or_throw().CreateGlobalStringPtr(basicString);
            }

        private:
            // original LLVM builder
            std::unique_ptr<llvm::IRBuilder<>> _llvm_builder;
            llvm::IRBuilder<>& get_or_throw() const {
                if(!_llvm_builder)
                    throw std::runtime_error("no builder specified");
                return *_llvm_builder;
            }

            IRBuilder(llvm::BasicBlock::iterator it);
            void initFromIterator(llvm::BasicBlock::iterator it);
        };

        // various switches to influence compiler behavior
        struct CompilePolicy {
            bool allowUndefinedBehavior;
            bool allowNumericTypeUnification; // whether bool/i64 get autoupcasted and merged when type conflicts exist within if-branches.
            bool sharedObjectPropagation;
            double normalCaseThreshold;

            CompilePolicy() : allowUndefinedBehavior(false),
            allowNumericTypeUnification(false),
            sharedObjectPropagation(false),
            normalCaseThreshold(0.9) {}

            bool operator == (const CompilePolicy& other) const {
                if(allowUndefinedBehavior != other.allowUndefinedBehavior)
                    return false;
                if(allowNumericTypeUnification != other.allowNumericTypeUnification)
                    return false;
                if(sharedObjectPropagation != other.sharedObjectPropagation)
                    return false;
                if(std::abs(normalCaseThreshold - other.normalCaseThreshold) > 0.001)
                    return false;
                return true;
            }
        };

        static CompilePolicy DEFAULT_COMPILE_POLICY;

        // helper function to determine number of predecessors
        inline size_t successorCount(llvm::BasicBlock* block) {
            assert(block);

            size_t count = 0;
            for(llvm::BasicBlock* pred : llvm::successors(block))
                count++;
            return count;
        }

        inline bool hasSuccessor(llvm::BasicBlock* block) {
            return successorCount(block) != 0;
        }

        // helper function to determine number of predecessors
        inline size_t predecessorCount(llvm::BasicBlock* block) {
            assert(block);

            size_t count = 0;
            for(llvm::BasicBlock* pred : llvm::predecessors(block))
                count++;
            return count;
        }

        inline bool hasPredecessor(llvm::BasicBlock* block) {
            return predecessorCount(block) != 0;
        }

        /*!
         * print the ir code of a single LLVM function.
         * @param func pointer to LLVM function
         * @param withLineNumbers include line numbers in print or not
         * @return formatted string holding llvm ir code
         */
        inline std::string printFunction(llvm::Function* func, bool withLineNumbers=false) {
            std::string ir_string;
            llvm::raw_string_ostream os{ir_string};
            assert(func);
            func->print(os, nullptr, false);
            os.flush();
            if(withLineNumbers)
                return core::withLineNumbers(ir_string);
            else
                return ir_string;
        }

        /*!
         * get a builder for the first block in a function. The first block may be linked already.
         * @param builder
         * @return
         */
        inline llvm::IRBuilder<>&& getFirstBlockBuilder(llvm::IRBuilder<>& builder) {
            assert(builder.GetInsertBlock());
            assert(builder.GetInsertBlock()->getParent());

            // function shouldn't be empty when this function here is called!
            assert(!builder.GetInsertBlock()->getParent()->empty());

            // special case: no instructions yet present?
            auto& firstBlock = builder.GetInsertBlock()->getParent()->getEntryBlock();
            llvm::IRBuilder<> ctorBuilder(&firstBlock);

            // when first block is not empty, go to first instruction
            if(!firstBlock.empty()) {
                llvm::Instruction& inst = *firstBlock.getFirstInsertionPt();
                ctorBuilder.SetInsertPoint(&inst);
            }
// disable here clang/gcc warning just for this - it's a limitation of how ctorbuilder is architected.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreturn-local-addr"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreturn-local-addr"
            return std::move(ctorBuilder);
#pragma GCC diagnostic pop
#pragma clang diagnostic pop
        }

        // in order to serialize/deserialize data properly and deal with
        // varlen variables too, the size needs to be known
        // this is a helper structure to make sure no sizing information is lost
        // during the process
        struct SerializableValue {
            llvm::Value *val;
            llvm::Value *size;
            llvm::Value *is_null; // should be i1, optional

            SerializableValue() : val(nullptr), size(nullptr), is_null(nullptr)   {}
            SerializableValue(llvm::Value *v, llvm::Value* s) : val(v), size(s), is_null(nullptr) {}
            SerializableValue(llvm::Value *v, llvm::Value* s, llvm::Value* n) : val(v), size(s), is_null(n) {}

            SerializableValue(const SerializableValue& other) : val(other.val), size(other.size), is_null(other.is_null) {}

            SerializableValue& operator = (const SerializableValue& other) {
                val = other.val;
                size = other.size;
                is_null = other.is_null;

                return *this;
            }
        };

        /*!
         * retrieves IR stored in LLVM module as string
         * @param mod llvm Module
         * @return string
         */
        inline std::string moduleToString(const llvm::Module& module) {
            std::string ir = "";
            llvm::raw_string_ostream os(ir);
            module.print(os, nullptr);
            os.flush();
            return ir;
        }

        /*!
         * converts llvm IR string to module
         * @param llvmIR
         * @return LLVM module
         */
        extern std::unique_ptr<llvm::Module> stringToModule(llvm::LLVMContext& context, const std::string& llvmIR);

        extern uint8_t* moduleToBitCode(const llvm::Module& module, size_t* bufSize);
        extern std::string moduleToBitCodeString(const llvm::Module& module);
        extern std::unique_ptr<llvm::Module> bitCodeToModule(llvm::LLVMContext& context, void* buf, size_t bufSize);
        inline std::unique_ptr<llvm::Module> bitCodeToModule(llvm::LLVMContext& context, const std::string& bc) {
            return bitCodeToModule(context, (void*)bc.c_str(), bc.size());
        }


        /*!x
         * compute code stats over LLVM IR code
         * @param llvmIR
         * @param include_detailed_counts
         * @return formatted string from InstructionCount Pass
         */
        extern std::string moduleStats(const std::string& llvmIR, bool include_detailed_counts=false);

        /*!
         * retrieves assembly for the Tuplex target machine as string (with comments)
         * @param module Module to lower to Assembly
         * @return string with x86 assembly code
         */
        extern std::string moduleToAssembly(std::shared_ptr<llvm::Module> module);

        /*!
         * get Tuplex specific target machine (i.e. with sse4.2 features or so)
         * @return
         */
        extern llvm::TargetMachine* getOrCreateTargetMachine();

        /*!
         * get features of CPU as llvm feature string
         */
        extern ATTRIBUTE_NO_SANITIZE_ADDRESS std::string getLLVMFeatureStr();

        /*!
         * helper function to initialize LLVM targets for this platform
         */
        extern void initLLVM();

        /*!
         * shutdown llvm
         */
        extern void shutdownLLVM();

        /*
         * cast val to destType (i.e. integer expansion or int to float conversion)
         * @param builder
         * @param val
         * @param destType
         * @return casted llvm Value
         */
        extern llvm::Value* upCast(const codegen::IRBuilder& builder, llvm::Value *val, llvm::Type *destType);

        extern llvm::Value *
        dictionaryKey(llvm::LLVMContext &ctx, llvm::Module *mod, const codegen::IRBuilder &builder, llvm::Value *val,
                      python::Type keyType, python::Type valType);

        extern SerializableValue
        dictionaryKeyCast(llvm::LLVMContext &ctx, llvm::Module* mod,
                          const codegen::IRBuilder &builder, llvm::Value *val, python::Type keyType);
        /*!
         * for debug purposes convert llvm type to string
         * @param type llvm type, if nullptr "null" is returned
         * @return string describing the type. In debug mode, extended description.
         */
        inline std::string llvmTypeToStr(llvm::Type* type) {
            if(!type)
                return "null";
            std::string s = "";
            llvm::raw_string_ostream os(s);
#ifndef NDEBUG
            type->print(os, true);
#else
            type->print(os);
#endif
            os.flush();
            return s;
        }

        /*!
         * verifies function and optionally yields error message
         * @param func
         * @param out
         * @return if function is ok to compile true else false
         */
        extern bool verifyFunction(llvm::Function* func, std::string* out=nullptr);


        /*!
         * counts how many successor blocks a basic block has
         * @param block
         * @return number of sucessor blocks, 0 when nullptr
         */
        extern size_t successorBlockCount(llvm::BasicBlock* block);

        static inline llvm::Function* exception_handler_prototype(llvm::LLVMContext& ctx, llvm::Module* mod, const std::string& name) {
            using namespace llvm;
            std::vector<Type*> eh_argtypes{Type::getInt8PtrTy(ctx, 0),
                                           Type::getInt64Ty(ctx),
                                           Type::getInt64Ty(ctx),
                                           Type::getInt64Ty(ctx),
                                           Type::getInt8PtrTy(ctx, 0),
                                           Type::getInt64Ty(ctx),};
            FunctionType *eh_type = FunctionType::get(Type::getVoidTy(ctx), eh_argtypes, false);

#if LLVM_VERSION_MAJOR < 9
            Function* func = cast<Function>(mod->getOrInsertFunction(name, eh_type));
#else
            Function* func = cast<Function>(mod->getOrInsertFunction(name, eh_type).getCallee());
#endif
            return func;
        }

        template<typename T> inline llvm::Type* ctypeToLLVM(llvm::LLVMContext& ctx) {
#ifndef NDEBUG
            throw std::runtime_error(std::string("unknown type ") + typeid(T).name() + " encountered");
#endif
            return nullptr;
        }

        template<> inline llvm::Type* ctypeToLLVM<int>(llvm::LLVMContext& ctx) {
            switch(sizeof(int)) {
                case 4:
                    return llvm::Type::getInt32Ty(ctx);
                case 8:
                    return llvm::Type::getInt16Ty(ctx);
                default:
                    throw std::runtime_error("unknown integer with");
            }
        }

        template<> inline llvm::Type* ctypeToLLVM<bool>(llvm::LLVMContext& ctx) {
            switch(sizeof(bool)) {
                case 1:
                    return llvm::Type::getInt8Ty(ctx);
                case 4:
                    return llvm::Type::getInt32Ty(ctx);
                case 8:
                    return llvm::Type::getInt16Ty(ctx);
                default:
                    throw std::runtime_error("unknown boolean with");
            }
        }

        template<> inline llvm::Type* ctypeToLLVM<char>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(char) == 1, "char must be 1 byte");
            return llvm::Type::getInt8Ty(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<int64_t>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(int64_t) == 8, "int64_t must be 8 bytes");
            return llvm::Type::getInt64Ty(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<size_t>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(size_t) == 8, "size_t must be 8 bytes");
            return llvm::Type::getInt64Ty(ctx);
        }

        template<> inline llvm::Type* ctypeToLLVM<char*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(char*) == 8, "char* must be 8 byte");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<const char*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(const char*) == 8, "const char* must be 8 byte");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<int64_t*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(int64_t) == 8, "int64_t must be 64bit");
            return llvm::Type::getInt64Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<void*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(void*) == 8, "void* must be 64bit");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<uint8_t*>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(uint8_t*) == 8, "uint8_t* must be 64bit");
            return llvm::Type::getInt8Ty(ctx)->getPointerTo(0);
        }

        template<> inline llvm::Type* ctypeToLLVM<double>(llvm::LLVMContext& ctx) {
            static_assert(sizeof(double) == 8, "double should be 64bit");
            return llvm::Type::getDoubleTy(ctx);
        }

        /*!
         * renames function args and returns them as hashmap for easy access. Order of names in vector corresponds to order of args
         */
        inline std::unordered_map<std::string, llvm::Value*> mapLLVMFunctionArgs(llvm::Function* func, const std::vector<std::string>& names) {
            std::unordered_map<std::string, llvm::Value*> m;

            std::vector<llvm::Argument *> args;
            int counter = 0;
            for (auto &arg : func->args()) {
                if(counter >= names.size()) {
                    throw std::runtime_error("too few names given");
                }
                m[names[counter]] = &arg;
                counter++;
            }

            return m;
        }

        inline int hashtableKeyWidth(const python::Type &t) {
            if (t.withoutOptions() == python::Type::I64 ||
                (t.isTupleType() &&
                 t.parameters().size() == 1 &&
                 t.parameters()[0].withoutOptions() == python::Type::I64)) {
                static_assert(sizeof(int64_t) == 8, "int64_t must be 8 bytes");
                return 8; // single int is hashed in an int hashtable
            }
            return 0; // strings are strings and anything besides int is just serialized to string right now
        }

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
#if LLVM_VERSION_MAJOR >= 10
          std::unique_ptr<Module> mod = llvm::parseAssemblyString(llvmIR, err, *ctx); // use err
#else
            std::unique_ptr<Module> mod = llvm::parseIR(buff->getMemBufferRef(), err, *ctx); // use err directly
#endif
            // check if any errors occured during module parsing
            if(nullptr == mod) {
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
            if(llvm::verifyModule(*mod, &os)) {
                std::stringstream errStream;
                os.flush();
                errStream<<"could not verify module:\n>>>>>>>>>>>>>>>>>\n"<<core::withLineNumbers(llvmIR)<<"\n<<<<<<<<<<<<<<<<<\n";
                errStream<<moduleErrors;

                return make_error<StringError>(errStream.str(), inconvertibleErrorCode());
            }
            return ThreadSafeModule(std::move(mod), std::move(ctx));
        }
#endif

        extern bool validateModule(const llvm::Module& mod);

        /*!
         * transform module by adding print statements to trace what is getting executed.
         * @param mod the Module
         * @param print_values whether to print values as well (or not)
         */
        extern void annotateModuleWithInstructionPrint(llvm::Module& mod, bool print_values=false);

    }
}

#endif //TUPLEX_CODEGENHELPER_H
