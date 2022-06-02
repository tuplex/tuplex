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

    // helper enum to specify sampling mode
    enum SamplingMode : int {
        FIRST_ROWS = 1,
        LAST_ROWS = 2,
        RANDOM_ROWS = 4,
        FIRST_FILE = 8,
        LAST_FILE = 16,
        RANDOM_FILE = 32,
        ALL_FILES = 64,
    };

    static const SamplingMode DEFAULT_SAMPLING_MODE = static_cast<SamplingMode>(SamplingMode::FIRST_ROWS | SamplingMode::LAST_ROWS | SamplingMode::FIRST_FILE);

    inline SamplingMode operator | (const SamplingMode& lhs, const SamplingMode& rhs) {
        return static_cast<SamplingMode>(lhs | rhs);
    }
}

#endif //TUPLEX_DEFS_H