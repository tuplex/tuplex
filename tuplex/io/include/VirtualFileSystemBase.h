//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_VIRTUALFILESYSTEMBASE_H
#define TUPLEX_VIRTUALFILESYSTEMBASE_H

namespace tuplex {
    enum class VirtualFileSystemStatus {
        VFS_OK = 0,
        VFS_OUTOFMEMORY,
        VFS_FILENOTFOUND,
        VFS_NOTAFILE,
        VFS_IOERROR,
        VFS_PREFIXALREADYREGISTERED,
        VFS_NOFILESYSTEM,
        VFS_INVALIDPREFIX,
        VFS_FILEEXISTS,
        VFS_NOTYETIMPLEMENTED
    };

    // flags to give for files
    // confer https://stackoverflow.com/questions/1448396/how-to-use-enums-as-flags-in-c
    // for details on different approaches. Best is to define operators for each type
    // and not use the general solution
    enum VirtualFileMode : uint32_t {
        VFS_READ = 1ul << 0,
        VFS_WRITE= 1ul << 1,
        VFS_OVERWRITE=1ul << 2,
        VFS_APPEND= 1ul << 3,
        VFS_TEXTMODE=1ul << 4 // append terminating '\0' if necessary
    };

    // cf. http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3485.pdf, 17.5.2.1.3 Bitmask types
    inline constexpr enum VirtualFileMode operator | (enum VirtualFileMode a, enum VirtualFileMode b) {
        return static_cast<VirtualFileMode>(uint32_t(a) | uint32_t(b));
    }

    inline constexpr enum VirtualFileMode operator & (enum VirtualFileMode a, enum VirtualFileMode b) {
        return static_cast<VirtualFileMode>(uint32_t(a) & uint32_t(b));
    }

    inline VirtualFileMode& operator |= (VirtualFileMode& lhs, VirtualFileMode rhs) {
        lhs = lhs | rhs;
        return lhs;
    }
}
#endif //TUPLEX_VIRTUALFILESYSTEMBASE_H