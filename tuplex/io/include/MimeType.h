//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IO_MIMETYPE_H
#define TUPLEX_IO_MIMETYPE_H

// provides functionality to quickly detect mime-type of a file
// could use the code from https://github.com/drodil/cpp-util/blob/master/file/mime/detector.hpp e.g. to have it built-in
// yet, let's rely on libmagic https://man7.org/linux/man-pages/man3/libmagic.3.html for now.

#include <magic.h>
#include <Logger.h>

namespace tuplex {

    /*!
     * check whether data stored in ptr is gzip (by checking magic bytes)
     * @param ptr
     * @return true/false
     */
    inline bool is_gzip_file(const uint8_t* ptr) {
        if(!ptr)
            return false;

        // compare magic
        uint8_t c1 = *ptr;
        uint8_t c2 = *(ptr + 1);
        // magic should be 1f 8b followed by compression method (08 = DEFLATE)
        return c1 == 0x1f && c2 == 0x8b;
    }

    /*!
     * returns mime type as string using magic library from a file stored under local path.
     * @param local_path path to a local file
     * @return mime type as string or "application/octet-stream" in case of failure
     */
    inline std::string detectMIMEType(const std::string& local_path) noexcept {
        magic_t mag = magic_open(MAGIC_MIME_TYPE);
        if(!mag) {
#ifndef NDEBUG
            Logger::instance().logger("filesystem").error("could not open magic from " + local_path);
#endif
            return "application/octet-stream";
        }
        magic_load(mag, NULL);
        std::string mime = magic_file(mag, local_path.c_str());
        magic_close(mag);

        return mime;
    }
}

#endif