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


namespace tuplex {
    enum class FileFormat {
        OUTFMT_UNKNOWN=0,
        OUTFMT_TUPLEX,
        OUTFMT_CSV,
        OUTFMT_TEXT,
        OUTFMT_ORC
    };
}

#endif //TUPLEX_DEFS_H