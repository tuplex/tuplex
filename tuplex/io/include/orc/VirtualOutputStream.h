//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_VIRTUALOUTPUTSTREAM_H
#define TUPLEX_VIRTUALOUTPUTSTREAM_H

#ifdef BUILD_WITH_ORC

#include <orc/OrcFile.hh>

namespace tuplex { namespace orc {

/*!
 * Implementation of orc OutputStream using tuplex's VirtualFileSystem.
 */
class VirtualOutputStream : public ::orc::OutputStream {
public:
    VirtualOutputStream(const tuplex::URI& uri):
        _fileName(uri.toString()) {
        auto file = VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_WRITE);
        if (!file) {
            throw std::runtime_error("Could not open " + uri.toPath() + " in write mode");
        }
        _file = std::move(file);
    }

    ~VirtualOutputStream() override {
        if (_file->is_open()) {
            _file->close();
        }
    }

    uint64_t getLength() const override {
        return _file->size();
    }

    uint64_t getNaturalWriteSize() const override {
        // Constant value from Orc's `OrcFile.cc` definition.
        return 128 * 1024;
    }

    void write(const void* buf, size_t length) override {
        _file->write(buf, length);
    }

    const std::string& getName() const override {
        return _fileName;
    }

    void close() override {
        _file->close();
    }

private:
    std::unique_ptr<tuplex::VirtualFile> _file;
    std::string _fileName;
};

}}

#endif

#endif //TUPLEX_VIRTUALOUTPUTSTREAM_H
