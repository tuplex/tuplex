//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <CodegenHelper.h>
#include <Logger.h>
#include <Base.h>

#include <llvm/Target/TargetIntrinsicInfo.h>
#include <llvm/Target/TargetOptions.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/IR/CFG.h> // to iterate over predecessors/successors easily
#include <LLVMEnvironment.h>
#include <LambdaFunction.h>
#include <FunctionRegistry.h>
#include <InstructionCountPass.h>
#include <llvm/Analysis/ValueTracking.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/Bitstream/BitCodes.h>
#include <llvm/Bitcode/BitcodeReader.h>

// llvm 10 refactored sys into Host
#if LLVM_VERSION_MAJOR > 9
#include <llvm/Support/Host.h>
#endif

namespace tuplex {
    namespace codegen {
        // global var because often only references are passed around.
        // CompilePolicy DEFAULT_COMPILE_POLICY = CompilePolicy();

        static bool llvmInitialized = false;
        void initLLVM() {
            if(!llvmInitialized) {
                // LLVM Initialization is required because else
                llvm::InitializeNativeTarget();
                llvm::InitializeNativeTargetAsmPrinter();
                llvm::InitializeNativeTargetAsmParser();
                llvmInitialized = true;
            }
        }

        void shutdownLLVM() {
            llvm::llvm_shutdown();
            llvmInitialized = false;
        }

        // IRBuilder definitions
        IRBuilder::IRBuilder(llvm::BasicBlock *bb) {
            _llvm_builder = std::make_unique<llvm::IRBuilder<>>(bb);
        }

        IRBuilder::IRBuilder(llvm::IRBuilder<> &llvm_builder) : IRBuilder(llvm_builder.GetInsertPoint()) {}

        IRBuilder::IRBuilder(const IRBuilder &other) : _llvm_builder(nullptr) {
            if(other._llvm_builder) {
                auto it = other._llvm_builder->GetInsertPoint();
                initFromIterator(it);
            }
        }

        void IRBuilder::initFromIterator(llvm::BasicBlock::iterator &it) {
            if(it->getParent()->empty())
                _llvm_builder = std::__1::make_unique<llvm::IRBuilder<>>(it->getParent());
            else {
                llvm::Instruction &inst = *it;
                _llvm_builder = std::__1::make_unique<llvm::IRBuilder<>>(&inst);
            }
        }

        IRBuilder::IRBuilder(const llvm::IRBuilder<> &llvm_builder) : IRBuilder(llvm_builder.GetInsertPoint()) {}

        IRBuilder::IRBuilder(llvm::BasicBlock::iterator it) {
            initFromIterator(it);
        }

        // Clang doesn't work well with ASAN, disable here container overflow.
        __attribute__((no_sanitize_address)) std::string getLLVMFeatureStr() {
            using namespace llvm;
            SubtargetFeatures Features;

            // If user asked for the 'native' CPU, we need to autodetect feat3ures.
            // This is necessary for x86 where the CPU might not support all the
            // features the autodetected CPU name lists in the target. For example,
            // not all Sandybridge processors support AVX.
            StringMap<bool> HostFeatures;
            if (sys::getHostCPUFeatures(HostFeatures))
                for (auto &F : HostFeatures)
                    Features.AddFeature(F.first(), F.second);

            return Features.getString();
        }

        llvm::TargetMachine* getOrCreateTargetMachine() {
            using namespace llvm;

            initLLVM();

            // we need SSE4.2 features. So create target machine that has these
            auto& logger = Logger::instance().logger("JITCompiler");

            auto triple = sys::getProcessTriple();//sys::getDefaultTargetTriple();
            std::string error;
            auto theTarget = llvm::TargetRegistry::lookupTarget(triple, error);
            std::string CPUStr = sys::getHostCPUName().str();

            //logger.info("using LLVM for target triple: " + triple + " target: " + theTarget->getName() + " CPU: " + CPUStr);

            // use default target options
            TargetOptions to;


            // need to tune this, so compilation gets fast enough

//            return theTarget->createTargetMachine(triple,
//                                                   CPUStr,
//                                                   getLLVMFeatureStr(),
//                                                   to,
//                                                   Reloc::PIC_,
//                                                   CodeModel::Large,
//                                                   CodeGenOpt::None);

            // confer https://subscription.packtpub.com/book/application_development/9781785285981/1/ch01lvl1sec14/converting-llvm-bitcode-to-target-machine-assembly
            // on how to tune this...

            return theTarget->createTargetMachine(triple,
                                                  CPUStr,
                                                  getLLVMFeatureStr(),
                                                  to,
                                                  Reloc::PIC_,
                                                  CodeModel::Large,
                                                  CodeGenOpt::Aggressive);
        }

        std::string moduleToAssembly(std::shared_ptr<llvm::Module> module) {
            llvm::SmallString<2048> asm_string;
            llvm::raw_svector_ostream asm_sstream{asm_string};

            llvm::legacy::PassManager pass_manager;
            auto target_machine = tuplex::codegen::getOrCreateTargetMachine();

            target_machine->Options.MCOptions.AsmVerbose = true;
#if LLVM_VERSION_MAJOR == 9
            target_machine->addPassesToEmitFile(pass_manager, asm_sstream, nullptr,
                                                llvm::TargetMachine::CGFT_AssemblyFile);
#elif LLVM_VERSION_MAJOR < 9
            target_machine->addPassesToEmitFile(pass_manager, asm_sstream,
                                                llvm::TargetMachine::CGFT_AssemblyFile);
#else
            target_machine->addPassesToEmitFile(pass_manager, asm_sstream, nullptr,
                                                llvm::CodeGenFileType::CGFT_AssemblyFile);
#endif

            pass_manager.run(*module);
            target_machine->Options.MCOptions.AsmVerbose = false;

            return asm_sstream.str().str();
        }

        std::unique_ptr<llvm::Module> stringToModule(llvm::LLVMContext& context, const std::string& llvmIR) {
            using namespace llvm;
            // first parse IR. It would be also an alternative to directly the LLVM Module from the ModuleBuilder class,
            // however if something went wrong there, memory errors would occur. Better is to first transform to a string
            // and then parse it because LLVM will validate the IR on the way.

            SMDiagnostic err; // create an SMDiagnostic instance

            std::unique_ptr<MemoryBuffer> buff = MemoryBuffer::getMemBuffer(llvmIR);
            std::unique_ptr<llvm::Module> mod = parseIR(buff->getMemBufferRef(), err, context); // use err directly

            // check if any errors occurred during module parsing
            if (nullptr == mod.get()) {
                // print errors
                Logger::instance().logger("LLVM Backend").error("could not compile module:\n>>>>>>>>>>>>>>>>>\n"
                                                                + core::withLineNumbers(llvmIR)
                                                                + "\n<<<<<<<<<<<<<<<<<");
                Logger::instance().logger("LLVM Backend").error(
                        "line " + std::to_string(err.getLineNo()) + ": " + err.getMessage().str());
                return nullptr;
            }


            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors;
            llvm::raw_string_ostream os(moduleErrors);
            if (verifyModule(*mod, &os)) {
                os.flush();
                Logger::instance().logger("LLVM Backend").error("could not verify module:\n>>>>>>>>>>>>>>>>>\n"
                                                                + core::withLineNumbers(llvmIR)
                                                                + "\n<<<<<<<<<<<<<<<<<");
                Logger::instance().logger("LLVM Backend").error(moduleErrors);
                return nullptr;
            }

            return std::move(mod);
        }

        std::unique_ptr<llvm::Module> bitCodeToModule(llvm::LLVMContext& context, void* buf, size_t bufSize) {
            using namespace llvm;

            // Note: check llvm11 for parallel codegen https://llvm.org/doxygen/ParallelCG_8cpp_source.html
            auto res = parseBitcodeFile(llvm::MemoryBufferRef(llvm::StringRef((char*)buf, bufSize), "<module>"), context);

            // check if any errors occurred during module parsing
            if (!res) {
                // print errors
                auto err = res.takeError();
                std::string err_msg;
                raw_string_ostream os(err_msg);
                os<<err; os.flush();
                Logger::instance().logger("LLVM Backend").error("could not parse module from bitcode");
                Logger::instance().logger("LLVM Backend").error(err_msg);
                return nullptr;
            }

            std::unique_ptr<llvm::Module> mod = std::move(res.get()); // use err directly

#ifndef NDEBUG
            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors;
            llvm::raw_string_ostream os(moduleErrors);
            if (verifyModule(*mod, &os)) {
                os.flush();
                Logger::instance().logger("LLVM Backend").error("could not verify module from bitcode");
                Logger::instance().logger("LLVM Backend").error(moduleErrors);
                Logger::instance().logger("LLVM Backend").error(core::withLineNumbers(moduleToString(*mod)));
                return nullptr;
            }
#endif

            return mod;
        }

        llvm::Value* upCast(llvm::IRBuilder<> &builder, llvm::Value *val, llvm::Type *destType) {
            // check if types are the same, then just return val
            if (val->getType() == destType)
                return val;
            else {
                // check if dest type is integer
                if(destType->isIntegerTy()) {
                    // check that dest type is larger than val's type
                    if(val->getType()->getIntegerBitWidth() > destType->getIntegerBitWidth())
                        throw std::runtime_error("destination types bitwidth is smaller than the current value ones, can't upcast");
                    return builder.CreateZExt(val, destType);
                } else if(destType->isFloatTy() || destType->isDoubleTy()) {
                    // check if current val is integer or float
                    if(val->getType()->isIntegerTy()) {
                        return builder.CreateSIToFP(val, destType);
                    } else {
                        return builder.CreateFPExt(val, destType);
                    }
                } else {
                    throw std::runtime_error("can't upcast llvm type " + llvmTypeToStr(destType));
                }
            }
        }

        llvm::Value *
        dictionaryKey(llvm::LLVMContext &ctx, llvm::Module *mod, llvm::IRBuilder<> &builder, llvm::Value *val,
                      python::Type keyType, python::Type valType) {
            // get key to string
            auto strFormat_func = strFormat_prototype(ctx, mod);
            std::vector<llvm::Value *> valargs;

            // format for key
            std::string typesstr;
            std::string replacestr;
            if(keyType == python::Type::STRING) {
                typesstr = "s";
                replacestr = "s_{}";
            }
            else if (python::Type::BOOLEAN == keyType) {
                typesstr = "b";
                replacestr = "b_{}";
                val = builder.CreateSExt(val, llvm::Type::getInt64Ty(ctx)); // extend to 64 bit integer
            } else if (python::Type::I64 == keyType) {
                typesstr = "d";
                replacestr = "i_{}";
            } else if (python::Type::F64 == keyType) {
                typesstr = "f";
                replacestr = "f_{}";
            } else {
                throw std::runtime_error("objects of type " + keyType.desc() + " are not supported as dictionary keys");
            }

            // value type encoding
            if(valType == python::Type::STRING) replacestr[1] = 's';
            else if(valType == python::Type::BOOLEAN) replacestr[1] = 'b';
            else if(valType == python::Type::I64) replacestr[1] = 'i';
            else if(valType == python::Type::F64) replacestr[1] = 'f';
            else throw std::runtime_error("objects of type " + valType.desc() + " are not supported as dictionary values");

            auto replaceptr = builder.CreatePointerCast(builder.CreateGlobalStringPtr(replacestr),
                                                        llvm::Type::getInt8PtrTy(ctx, 0));
            auto sizeVar = builder.CreateAlloca(llvm::Type::getInt64Ty(ctx), 0, nullptr);
            auto typesptr = builder.CreatePointerCast(builder.CreateGlobalStringPtr(typesstr),
                                                      llvm::Type::getInt8PtrTy(ctx, 0));
            valargs.push_back(replaceptr);
            valargs.push_back(sizeVar);
            valargs.push_back(typesptr);
            valargs.push_back(val);
            return builder.CreateCall(strFormat_func, valargs);
        }

        // TODO: Do we need to use lfb to add checks?
        SerializableValue
        dictionaryKeyCast(llvm::LLVMContext &ctx, llvm::Module* mod,
                          llvm::IRBuilder<> &builder, llvm::Value *val, python::Type keyType) {
            // type chars
            auto s_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 's'));
            auto b_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 'b'));
            auto i_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 'i'));
            auto f_char = llvm::Constant::getIntegerValue(llvm::Type::getInt8Ty(ctx), llvm::APInt(8, 'f'));

            auto typechar = builder.CreateLoad(val);
            auto keystr = builder.CreateGEP(val, llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 2)));
            auto keylen = builder.CreateCall(strlen_prototype(ctx, mod), {keystr});
            if(keyType == python::Type::STRING) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, s_char));
                return SerializableValue(keystr, builder.CreateAdd(keylen, llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx), llvm::APInt(64, 1))));
            } else if (keyType == python::Type::BOOLEAN) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, b_char));
                auto value = builder.CreateAlloca(llvm::Type::getInt8Ty(ctx), 0, nullptr);
                auto strBegin = keystr;
                auto strEnd = builder.CreateGEP(strBegin, keylen);
                auto resCode = builder.CreateCall(fastatob_prototype(ctx, mod), {strBegin, strEnd, value});
                auto cond = builder.CreateICmpNE(resCode, llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(ctx),
                                                                                          llvm::APInt(32,
                                                                                                      ecToI32(ExceptionCode::SUCCESS))));
//                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);
                return SerializableValue(builder.CreateLoad(value),
                        llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                llvm::APInt(64, sizeof(int64_t))));
            } else if (keyType == python::Type::I64) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, i_char));
                auto value = builder.CreateAlloca(llvm::Type::getInt64Ty(ctx), 0, nullptr);
                auto strBegin = keystr;
                auto strEnd = builder.CreateGEP(strBegin, keylen);
                auto resCode = builder.CreateCall(fastatoi_prototype(ctx, mod), {strBegin, strEnd, value});
                auto cond = builder.CreateICmpNE(resCode, llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(ctx),
                                                                                          llvm::APInt(32,
                                                                                                      ecToI32(ExceptionCode::SUCCESS))));
//                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);
                return SerializableValue(builder.CreateLoad(value),
                                         llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                                                         llvm::APInt(64, sizeof(int64_t))));
            } else if (keyType == python::Type::F64) {
//                lfb.addException(builder, ExceptionCode::UNKNOWN, builder.CreateICmpEQ(typechar, f_char));
                auto value = builder.CreateAlloca(llvm::Type::getDoubleTy(ctx), 0, nullptr);
                auto strBegin = keystr;
                auto strEnd = builder.CreateGEP(strBegin, keylen);
                auto resCode = builder.CreateCall(fastatod_prototype(ctx, mod), {strBegin, strEnd, value});
                auto cond = builder.CreateICmpNE(resCode, llvm::Constant::getIntegerValue(llvm::Type::getInt32Ty(ctx),
                                                                                          llvm::APInt(32,
                                                                                                      ecToI32(ExceptionCode::SUCCESS))));
//                lfb.addException(builder, ExceptionCode::VALUEERROR, cond);
                return SerializableValue(builder.CreateLoad(value),
                                         llvm::Constant::getIntegerValue(llvm::Type::getInt64Ty(ctx),
                                                                         llvm::APInt(64, sizeof(double))));
            } else {
                throw std::runtime_error("objects of type " + keyType.desc() + " are not supported as dictionary keys");
            }
        }


        bool verifyFunction(llvm::Function* func, std::string* out) {
            std::string funcErrors = "";
            llvm::raw_string_ostream os(funcErrors);

            if(llvm::verifyFunction(*func, &os)) {
                os.flush(); // important, else funcErrors may be an empty string

                if(out)
                    *out = funcErrors;
                return false;
            }
            return true;
        }

        size_t successorBlockCount(llvm::BasicBlock* block) {
            if(!block)
                return 0;
            auto term = block->getTerminator();
            if(!term)
                return 0;

            return term->getNumSuccessors();
        }

        std::string moduleStats(const std::string& llvmIR, bool include_detailed_counts) {
            llvm::LLVMContext context;
            auto mod = stringToModule(context, llvmIR);

            char name[] = "inst count";
            InstructionCounts inst_count(*name);
            inst_count.runOnModule(*mod);
            return inst_count.formattedStats(include_detailed_counts);
        }

        std::string globalVariableToString(llvm::Value* value) {
            using namespace llvm;
            assert(value);

            if(!value || !dyn_cast<ConstantExpr>(value))
                throw std::runtime_error("value is not a constant expression");
            auto *CE = dyn_cast<ConstantExpr>(value);
            StringRef Str;
            if(getConstantStringInfo(CE, Str)) {
                return Str.str();
            }
            return "";
        }


        /// If generating a bc file on darwin, we have to emit a
        /// header and trailer to make it compatible with the system archiver.  To do
        /// this we emit the following header, and then emit a trailer that pads the
        /// file out to be a multiple of 16 bytes.
        ///
        /// struct bc_header {
        ///   uint32_t Magic;         // 0x0B17C0DE
        ///   uint32_t Version;       // Version, currently always 0.
        ///   uint32_t BitcodeOffset; // Offset to traditional bitcode file.
        ///   uint32_t BitcodeSize;   // Size of traditional bitcode file.
        ///   uint32_t CPUType;       // CPU specifier.
        ///   ... potentially more later ...
        /// };

        static void writeInt32ToBuffer(uint32_t Value, llvm::SmallVectorImpl<char> &Buffer,
                                       uint32_t &Position) {
            llvm::support::endian::write32le(&Buffer[Position], Value);
            Position += 4;
        }

        static void emitDarwinBCHeaderAndTrailer(llvm::SmallVectorImpl<char> &Buffer,
                                                 const llvm::Triple &TT) {
            using namespace llvm;
            unsigned CPUType = ~0U;

            // Match x86_64-*, i[3-9]86-*, powerpc-*, powerpc64-*, arm-*, thumb-*,
            // armv[0-9]-*, thumbv[0-9]-*, armv5te-*, or armv6t2-*. The CPUType is a magic
            // number from /usr/include/mach/machine.h.  It is ok to reproduce the
            // specific constants here because they are implicitly part of the Darwin ABI.
            enum {
                DARWIN_CPU_ARCH_ABI64      = 0x01000000,
                DARWIN_CPU_TYPE_X86        = 7,
                DARWIN_CPU_TYPE_ARM        = 12,
                DARWIN_CPU_TYPE_POWERPC    = 18
            };

            Triple::ArchType Arch = TT.getArch();
            if (Arch == Triple::x86_64)
                CPUType = DARWIN_CPU_TYPE_X86 | DARWIN_CPU_ARCH_ABI64;
            else if (Arch == Triple::x86)
                CPUType = DARWIN_CPU_TYPE_X86;
            else if (Arch == Triple::ppc)
                CPUType = DARWIN_CPU_TYPE_POWERPC;
            else if (Arch == Triple::ppc64)
                CPUType = DARWIN_CPU_TYPE_POWERPC | DARWIN_CPU_ARCH_ABI64;
            else if (Arch == Triple::arm || Arch == Triple::thumb)
                CPUType = DARWIN_CPU_TYPE_ARM;

            // Traditional Bitcode starts after header.
            assert(Buffer.size() >= BWH_HeaderSize &&
                   "Expected header size to be reserved");
            unsigned BCOffset = BWH_HeaderSize;
            unsigned BCSize = Buffer.size() - BWH_HeaderSize;

            // Write the magic and version.
            unsigned Position = 0;
            writeInt32ToBuffer(0x0B17C0DE, Buffer, Position);
            writeInt32ToBuffer(0, Buffer, Position); // Version.
            writeInt32ToBuffer(BCOffset, Buffer, Position);
            writeInt32ToBuffer(BCSize, Buffer, Position);
            writeInt32ToBuffer(CPUType, Buffer, Position);

            // If the file is not a multiple of 16 bytes, insert dummy padding.
            while (Buffer.size() & 15ul)
                Buffer.push_back(0);
        }

        uint8_t* moduleToBitCode(const llvm::Module& module, size_t* bufSize) {
            using namespace llvm;

            SmallVector<char, 0> Buffer;
            Buffer.reserve(256 * 1014); // 256K
            auto ShouldPreserveUseListOrder = false;
            const ModuleSummaryIndex *Index=nullptr;
            bool GenerateHash=false;
            ModuleHash *ModHash=nullptr;

            Triple TT(module.getTargetTriple());
            if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
                Buffer.insert(Buffer.begin(), BWH_HeaderSize, 0);

            BitcodeWriter Writer(Buffer);
            Writer.writeModule(module, ShouldPreserveUseListOrder, Index,
                    GenerateHash,
                               ModHash);
            Writer.writeSymtab();
            Writer.writeStrtab();

            if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
                emitDarwinBCHeaderAndTrailer(Buffer, TT);

            // alloc buffer & memcpy module
            auto bc_size = Buffer.size();
            auto buf = new uint8_t[bc_size];
            memcpy(buf, (char*)&Buffer.front(), bc_size);
            if(bufSize)
                *bufSize = bc_size;

            return buf;
        }

        std::string moduleToBitCodeString(const llvm::Module& module) {
            using namespace llvm;

            // in debug mode, verify module first
#ifndef NDEBUG
            {
                // run verify pass on module and print out any errors, before attempting to compile it
                std::string moduleErrors;
                llvm::raw_string_ostream os(moduleErrors);
                if (verifyModule(module, &os)) {
                    os.flush();
                    auto llvmIR = moduleToString(module);
                    Logger::instance().logger("LLVM Backend").error("could not verify module:\n>>>>>>>>>>>>>>>>>\n"
                                                                    + core::withLineNumbers(llvmIR)
                                                                    + "\n<<<<<<<<<<<<<<<<<");
                    Logger::instance().logger("LLVM Backend").error(moduleErrors);
                    return "";
                }
            }

#endif

            // simple conversion using LLVM builtins...
            std::string out_str;
            llvm::raw_string_ostream os(out_str);
            WriteBitcodeToFile(module, os);
            os.flush();
            return out_str;

            // could also use direct code & tune buffer sizes better...
            // SmallVector<char, 0> Buffer;
            // Buffer.reserve(256 * 1014); // 256K
            // auto ShouldPreserveUseListOrder = false;
            // const ModuleSummaryIndex *Index=nullptr;
            // bool GenerateHash=false;
            // ModuleHash *ModHash=nullptr;

            // Triple TT(module.getTargetTriple());
            // if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
            //     Buffer.insert(Buffer.begin(), BWH_HeaderSize, 0);

            // BitcodeWriter Writer(Buffer);
            // Writer.writeModule(module, ShouldPreserveUseListOrder, Index,
            //                    GenerateHash,
            //                    ModHash);
            // Writer.writeSymtab();
            // Writer.writeStrtab();

            // if (TT.isOSDarwin() || TT.isOSBinFormatMachO())
            //     emitDarwinBCHeaderAndTrailer(Buffer, TT);

            // // copy buffer to module
            // auto bc_size = Buffer.size();
            // std::string bc_str;
            // bc_str.reserve(bc_size);
            // bc_str.assign((char*)&Buffer.front(), bc_size);
            // assert(bc_str.length() == bc_size);
            // return bc_str;
        }
    }
}