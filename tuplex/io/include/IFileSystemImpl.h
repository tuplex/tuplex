//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IFILESYSTEMIMPL_H
#define TUPLEX_IFILESYSTEMIMPL_H

#include "URI.h"
#include "VirtualFileSystem.h"
#include "VirtualFileSystemBase.h"
#include "VirtualFile.h"
#include "VirtualMappedFile.h"
#include <memory>
#include <functional>

namespace tuplex {

    class VirtualFile;

    /*!
     * abstract class to implement a concrete FileSystem implementation
     */
    class IFileSystemImpl {
    public:
        virtual ~IFileSystemImpl() = default;
        virtual VirtualFileSystemStatus create_dir(const URI& uri) = 0;
        virtual VirtualFileSystemStatus remove(const URI& uri) = 0;
        virtual VirtualFileSystemStatus touch(const URI& uri, bool overwrite=false) = 0;
        virtual VirtualFileSystemStatus file_size(const URI& uri, uint64_t& size) = 0;
        virtual VirtualFileSystemStatus ls(const URI& parent, std::vector<URI>* uris) = 0;
        virtual std::unique_ptr<VirtualFile> open_file(const URI& uri, VirtualFileMode vfm) = 0;
        virtual std::unique_ptr<VirtualMappedFile> map_file(const URI& uri) = 0;
        virtual std::vector<URI> glob(const std::string& pattern) = 0;

        // abstract implementation using glob & Co available per default
        virtual bool walkPattern(const URI& pattern, std::function<bool(void*, const URI&, size_t)> callback, void* userData=nullptr);
    };
}
#endif //TUPLEX_IFILESYSTEMIMPL_H