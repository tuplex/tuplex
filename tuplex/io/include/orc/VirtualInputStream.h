//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_VIRTUALINPUTSTREAM_H
#define TUPLEX_VIRTUALINPUTSTREAM_H

#ifdef BUILD_WITH_ORC

#include <orc/OrcFile.hh>
#include <iostream>
#include <fstream>

namespace tuplex { namespace orc {

/*!
* Implementation of orc InputStream using tuplex's VirtualFileSystem.
*/
class VirtualInputStream : public ::orc::InputStream {
public:
    explicit VirtualInputStream(const URI &uri):
        _filename(uri.toString()), _currentPosition(0) {
        auto file = VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_READ);
        if (!file) {
            throw std::runtime_error("Could not open " + uri.toPath() + " in read mode");
        }
        _file = std::move(file);
    }

    ~VirtualInputStream() override {
        if (_file->is_open()) {
            _file->close();
        }
    }

    uint64_t getLength() const override {
        return _file->size();
    }

    uint64_t getNaturalReadSize() const override {
        return 128 * 1024;
    }

    void read(void *buf, uint64_t length, uint64_t offset) override {
        int64_t seekDelta = offset - _currentPosition;
        _file->seek(seekDelta);
        _currentPosition += seekDelta;

        _file->read(buf, length);
        _currentPosition += length;
    }

    const std::string& getName() const override {
        return _filename;
    }

private:
    std::unique_ptr<VirtualFile> _file;
    size_t _currentPosition;
    std::string _filename;
};

}}

#endif

#endif //TUPLEX_VIRTUALINPUTSTREAM_H
