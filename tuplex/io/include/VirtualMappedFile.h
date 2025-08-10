//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_VIRTUALMAPPEDFILE_H
#define TUPLEX_VIRTUALMAPPEDFILE_H

#include <cstdint>
#include <URI.h>

namespace tuplex {
    class VirtualMappedFile;
    class VirtualFileSystem;

    /*!
     * abstract base class for a (read-only) memory mapped file.
     * memory region is guaranteed to be multiple of 16 for faster
     * string instructions
     */
    class VirtualMappedFile {
    public:
        virtual ~VirtualMappedFile() = default;
        VirtualMappedFile() : _uri("")   {}
        VirtualMappedFile(const URI& uri) : _uri(uri) {}

        virtual uint8_t* getStartPtr() = 0;

        virtual uint8_t* getEndPtr() = 0;

        /*!
         * closes file
         * @return status of close operation
         */
        virtual VirtualFileSystemStatus close() = 0;

        URI getURI() const { return _uri; }

        virtual bool is_open() const = 0;
    protected:
        URI                     _uri;
    };
}

#endif //TUPLEX_VIRTUALMAPPEDFILE_H