//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <RuntimeInterface.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/raw_os_ostream.h>
#include <Logger.h>
#include <llvm/IR/Mangler.h>
#include <llvm/ExecutionEngine/orc/JITTargetMachineBuilder.h>
#if LLVM_VERSION_MAJOR >= 16
#include <llvm/TargetParser/Host.h>
#endif

static bool _loaded = false;
static std::string _libPath = "";

namespace tuplex {
    namespace runtime {


        static void dummy() noexcept {}
        static void dummySRTM(const size_t, size_t) noexcept {}

        void(*setRunTimeMemory)(const size_t, size_t) noexcept= dummySRTM;
        void(*freeRunTimeMemory)() noexcept = dummy;
        void(*releaseRunTimeMemory)() noexcept = dummy;
        void(*rtfree_all)() noexcept = dummy;
        size_t(*runTimeMemorySize)() noexcept = nullptr;
        void*(*rtmalloc)(const size_t) noexcept = nullptr;
        void (*rtfree)(void *) noexcept = nullptr;

        bool loaded() { return _loaded; }


        static void* findAddrOfSymbol(const char* name) {
            auto addr_ptr = llvm::sys::DynamicLibrary::SearchForAddressOfSymbol(name);

            if(!addr_ptr) {
                // try mangled version by prepending "_"
                auto mangled_name = std::string("_") + name;
                addr_ptr = llvm::sys::DynamicLibrary::SearchForAddressOfSymbol(mangled_name.c_str());
            }

            return addr_ptr;
        }

        bool init(const std::string& path) {

            if(path.length() == 0)
                return false;

            // runtime should be loaded only once
            if(_loaded) {
                if(_libPath.compare(path) == 0)
                    return true;
                else {
                    Logger::instance().defaultLogger().error("runtime library should be loaded only once.");
                    return false;
                }
            }


            std::string errMsg = "";

            auto dl = llvm::sys::DynamicLibrary::getPermanentLibrary(path.c_str(), &errMsg);
            if(!dl.isValid()) {
                Logger::instance().defaultLogger().error("error while loading runtime shared library "
                                                         + path + " \nDetails: " + errMsg);
                return false;
            }

            _loaded = true;
            _libPath = path;

            // assign to pointers the established addresses
            setRunTimeMemory = nullptr;
            freeRunTimeMemory = nullptr;
            releaseRunTimeMemory = nullptr;
            runTimeMemorySize = nullptr;
            rtfree_all = nullptr;
            rtmalloc=nullptr;
            rtfree=nullptr;
            setRunTimeMemory = reinterpret_cast<void(*)(const size_t, size_t) noexcept>(findAddrOfSymbol("setRunTimeMemory"));
            freeRunTimeMemory = reinterpret_cast<void(*)() noexcept>(findAddrOfSymbol("freeRunTimeMemory"));
            releaseRunTimeMemory = reinterpret_cast<void(*)() noexcept>(findAddrOfSymbol("releaseRunTimeMemory"));
            rtfree_all = reinterpret_cast<void(*)() noexcept>(findAddrOfSymbol("rtfree_all"));
            rtmalloc = reinterpret_cast<void*(*)(const size_t) noexcept>(findAddrOfSymbol("rtmalloc"));
            rtfree = reinterpret_cast<void (*)(void *) noexcept>(findAddrOfSymbol("rtfree"));
            runTimeMemorySize = reinterpret_cast<size_t(*)() noexcept>(findAddrOfSymbol("getRunTimeMemorySize"));

            cJSON_Hooks tmp = {rtmalloc, rtfree};
            cJSON_InitHooks(&tmp);

            srand(time(0));

            if(!setRunTimeMemory || !freeRunTimeMemory) {
                Logger::instance().defaultLogger().error("Could not find required runtime symbols setRunTimeMemory or freeRunTimeMemory in shared library " + path + ".");
                return false;
            }

            return true;
        }
    }
}