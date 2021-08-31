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

#include <orc/OrcFile.hh>

namespace tuplex { namespace orc {

/*!
 * Implementation of orc OutputStream using tuplex's VirtualFileSystem.
 */
        class VirtualOutputStream : public ::orc::OutputStream {
        public:
            VirtualOutputStream(const tuplex::URI& uri):
                    _file(tuplex::VirtualFileSystem::open_file(uri, tuplex::VirtualFileMode::VFS_WRITE)), _fileName(uri.toString()) {}

            ~VirtualOutputStream() override {
                _file.reset();
            }

            uint64_t getLength() const override {
                return _file.get()->size();
            }

            uint64_t getNaturalWriteSize() const override {
                return 128 * 1024;
            }

            void write(const void* buf, size_t length) override {
                _file->write(buf, length);
            }

            const std::string& getName() const override {
                return _fileName;
            }

            void close() override {
                _file.get()->close();
            }

        private:
            std::unique_ptr<tuplex::VirtualFile> _file;
            std::string _fileName;
        };

    }}

#endif //TUPLEX_VIRTUALOUTPUTSTREAM_H
