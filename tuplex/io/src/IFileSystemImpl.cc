//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IFileSystemImpl.h>
#include <URI.h>
#include <cassert>

namespace tuplex {
    bool IFileSystemImpl::walkPattern(const tuplex::URI &pattern,
                                      std::function<bool(void *, const tuplex::URI &, size_t)> callback,
                                      void *userData) {

        // make sure there is no , in pattern
        for(auto c : pattern.toPath())
            assert(c != ',');

        // run glob over pattern
        auto uris = glob(pattern.toPath());

        // manually go over files/dirs and call callback
        for(auto uri : uris) {
            auto vfs = VirtualFileSystem::fromURI(uri);
            uint64_t fsize = 0;
            vfs.file_size(uri, fsize);
            if(!uri.isFile()) // TODO: maybe warn user when dir is used instead of files?
                return false;
            if(!callback(userData, uri, fsize))
                return false;
        }

        return true;
    }
}