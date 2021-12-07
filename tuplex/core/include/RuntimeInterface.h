//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_RUNTIMEINTERFACE_H
#define TUPLEX_RUNTIMEINTERFACE_H

#include <Base.h>
#include <pcre2.h>

/// this file contains convenience definitions for accessing functions within the runtime, loaded dynamically
namespace tuplex {
    namespace runtime {
        extern bool init(const std::string& path);

        /*!
         * determines whether runtime was loaded or not. Required for any code invoking runtime.
         * @return true if runtime was successfully loaded. False otherwise.
         */
        extern bool loaded();

        extern void(*setRunTimeMemory)(const size_t, size_t) noexcept;
        extern void(*freeRunTimeMemory)() noexcept;
        extern void(*releaseRunTimeMemory)() noexcept;
        extern size_t(*runTimeMemorySize)() noexcept;
        extern void(*rtfree_all)() noexcept;
        extern void*(*rtmalloc)(const size_t) noexcept;
        extern void (*rtfree)(void *) noexcept;
    }
}

#endif //TUPLEX_RUNTIMEINTERFACE_H