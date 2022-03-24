//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_DEFS_H
#define TUPLEX_DEFS_H

// this file holds common definitions for tuplex
#include <string>

namespace tuplex {
    enum class FileFormat {
        OUTFMT_UNKNOWN=0,
        OUTFMT_TUPLEX,
        OUTFMT_CSV,
        OUTFMT_TEXT,
        OUTFMT_ORC
    };

    inline std::string defaultFileExtension(const FileFormat& fmt) {
        switch (fmt) {
            case FileFormat::OUTFMT_CSV:
                return "csv";
            case FileFormat::OUTFMT_TEXT:
                return "txt";
            case FileFormat::OUTFMT_ORC:
                return "orc";
            case FileFormat::OUTFMT_TUPLEX:
                return "tpx";
            default:
                break;
        }
        return "";
    }
}

#endif //TUPLEX_DEFS_H