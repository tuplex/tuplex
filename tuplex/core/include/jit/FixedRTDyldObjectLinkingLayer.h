//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FIXEDRTDYLDOBJECTLINKINGLAYER_H
#define TUPLEX_FIXEDRTDYLDOBJECTLINKINGLAYER_H

#include <llvm/ADT/STLExtras.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/Layer.h>
#include <llvm/ExecutionEngine/Orc/Legacy.h>
#include <llvm/ExecutionEngine/RuntimeDyld.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/Error.h>
#include <algorithm>
#include <cassert>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// this class is 1:1 copy from llvm 9.x release but contains LLVM10 fixes for deregistering EH frames
namespace llvm {
    namespace orc {

        class FixedRTDyldObjectLinkingLayer : public ObjectLayer {
        public:
            /// Functor for receiving object-loaded notifications.
            using NotifyLoadedFunction =
            std::function<
            void(VModuleKey,
            const object::ObjectFile &Obj,
            const RuntimeDyld::LoadedObjectInfo &)>;

            /// Functor for receiving finalization notifications.
            using NotifyEmittedFunction =
            std::function<void(VModuleKey, std::unique_ptr < MemoryBuffer > )>;

            using GetMemoryManagerFunction =
            std::function<std::unique_ptr<RuntimeDyld::MemoryManager>()>;


            // FIX, new destructor
            ~FixedRTDyldObjectLinkingLayer() override;

            /// Construct an ObjectLinkingLayer with the given NotifyLoaded,
            ///        and NotifyEmitted functors.
            FixedRTDyldObjectLinkingLayer(ExecutionSession &ES,
                                     GetMemoryManagerFunction GetMemoryManager);

            /// Emit the object.
            void emit(MaterializationResponsibility R,
                      std::unique_ptr <MemoryBuffer> O) override;

            /// Set the NotifyLoaded callback.
            FixedRTDyldObjectLinkingLayer &setNotifyLoaded(NotifyLoadedFunction NotifyLoaded) {
                this->NotifyLoaded = std::move(NotifyLoaded);
                return *this;
            }

            /// Set the NotifyEmitted callback.
            FixedRTDyldObjectLinkingLayer &
            setNotifyEmitted(NotifyEmittedFunction NotifyEmitted) {
                this->NotifyEmitted = std::move(NotifyEmitted);
                return *this;
            }

            /// Set the 'ProcessAllSections' flag.
            ///
            /// If set to true, all sections in each object file will be allocated using
            /// the memory manager, rather than just the sections required for execution.
            ///
            /// This is kludgy, and may be removed in the future.
            FixedRTDyldObjectLinkingLayer &setProcessAllSections(bool ProcessAllSections) {
                this->ProcessAllSections = ProcessAllSections;
                return *this;
            }

            /// Instructs this RTDyldLinkingLayer2 instance to override the symbol flags
            /// returned by RuntimeDyld for any given object file with the flags supplied
            /// by the MaterializationResponsibility instance. This is a workaround to
            /// support symbol visibility in COFF, which does not use the libObject's
            /// SF_Exported flag. Use only when generating / adding COFF object files.
            ///
            /// FIXME: We should be able to remove this if/when COFF properly tracks
            /// exported symbols.
            FixedRTDyldObjectLinkingLayer &
            setOverrideObjectFlagsWithResponsibilityFlags(bool OverrideObjectFlags) {
                this->OverrideObjectFlags = OverrideObjectFlags;
                return *this;
            }

            /// If set, this RTDyldObjectLinkingLayer instance will claim responsibility
            /// for any symbols provided by a given object file that were not already in
            /// the MaterializationResponsibility instance. Setting this flag allows
            /// higher-level program representations (e.g. LLVM IR) to be added based on
            /// only a subset of the symbols they provide, without having to write
            /// intervening layers to scan and add the additional symbols. This trades
            /// diagnostic quality for convenience however: If all symbols are enumerated
            /// up-front then clashes can be detected and reported early (and usually
            /// deterministically). If this option is set, clashes for the additional
            /// symbols may not be detected until late, and detection may depend on
            /// the flow of control through JIT'd code. Use with care.
            FixedRTDyldObjectLinkingLayer &
            setAutoClaimResponsibilityForObjectSymbols(bool AutoClaimObjectSymbols) {
                this->AutoClaimObjectSymbols = AutoClaimObjectSymbols;
                return *this;
            }

        private:
            Error onObjLoad(VModuleKey K, MaterializationResponsibility &R,
                            object::ObjectFile &Obj,
                            std::unique_ptr <RuntimeDyld::LoadedObjectInfo> LoadedObjInfo,
                            std::map <StringRef, JITEvaluatedSymbol> Resolved,
                            std::set <StringRef> &InternalSymbols);

            void onObjEmit(VModuleKey K, std::unique_ptr <MemoryBuffer> ObjBuffer,
                           MaterializationResponsibility &R, Error Err);

            mutable std::mutex RTDyldLayerMutex;
            GetMemoryManagerFunction GetMemoryManager;
            NotifyLoadedFunction NotifyLoaded;
            NotifyEmittedFunction NotifyEmitted;
            bool ProcessAllSections = false;
            bool OverrideObjectFlags = false;
            bool AutoClaimObjectSymbols = false;
            std::vector <std::unique_ptr<RuntimeDyld::MemoryManager>> MemMgrs;
        };
    }
}


#endif //TUPLEX_FIXEDRTDYLDOBJECTLINKINGLAYER_H