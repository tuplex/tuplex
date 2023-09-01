//
// Created by Leonhard Spiegelberg on 3/8/22.
//

#ifndef TUPLEX_COMPRESSION_H
#define TUPLEX_COMPRESSION_H

// helper file to provide compression utilities, especially helpful when storing/getting data

#include <string>
#include <stdexcept>
#include <iostream>
#include <iomanip>
#include <sstream>

#include <zlib.h>


// include third_party gzip
#include "third_party/gzip/decompress.hpp"
#include "third_party/gzip/decompress.hpp"
#include "third_party/gzip/utils.hpp"

// from https://gist.github.com/gomons/9d446024fbb7ccb6536ab984e29e154a
namespace tuplex {

    /** Compress a STL string using zlib with given compression level and return
    * the binary data. */
    inline std::string compress_string(const std::string& str,
                                int compressionlevel = Z_BEST_COMPRESSION)
    {
        z_stream zs;                        // z_stream is zlib's control structure
        memset(&zs, 0, sizeof(zs));

        if (deflateInit(&zs, compressionlevel) != Z_OK)
            throw(std::runtime_error("deflateInit failed while compressing."));

        zs.next_in = (Bytef*)str.data();
        zs.avail_in = str.size();           // set the z_stream's input

        int ret;
        char outbuffer[32768];
        std::string outstring;

        // retrieve the compressed bytes blockwise
        do {
            zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
            zs.avail_out = sizeof(outbuffer);

            ret = deflate(&zs, Z_FINISH);

            if (outstring.size() < zs.total_out) {
                // append the block to the output string
                outstring.append(outbuffer,
                                 zs.total_out - outstring.size());
            }
        } while (ret == Z_OK);

        deflateEnd(&zs);

        if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
            std::ostringstream oss;
            oss << "Exception during zlib compression: (" << ret << ") " << zs.msg;
            throw(std::runtime_error(oss.str()));
        }

        return outstring;
    }

    /** Decompress an STL string using zlib and return the original data. */
    inline std::string decompress_string(const std::string& str)
    {
        z_stream zs;                        // z_stream is zlib's control structure
        memset(&zs, 0, sizeof(zs));

        if (inflateInit(&zs) != Z_OK)
            throw(std::runtime_error("inflateInit failed while decompressing."));

        zs.next_in = (Bytef*)str.data();
        zs.avail_in = str.size();

        int ret;
        char outbuffer[32768];
        std::string outstring;

        // get the decompressed bytes blockwise using repeated calls to inflate
        do {
            zs.next_out = reinterpret_cast<Bytef*>(outbuffer);
            zs.avail_out = sizeof(outbuffer);

            ret = inflate(&zs, 0);

            if (outstring.size() < zs.total_out) {
                outstring.append(outbuffer,
                                 zs.total_out - outstring.size());
            }

        } while (ret == Z_OK);

        inflateEnd(&zs);

        if (ret != Z_STREAM_END) {          // an error occurred that was not EOF
            std::ostringstream oss;
            oss << "Exception during zlib decompression: (" << ret << ") "
                << zs.msg;
            throw(std::runtime_error(oss.str()));
        }

        return outstring;
    }
}


#endif //TUPLEX_COMPRESSION_H
