//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FILEINPUTREADER_H
#define TUPLEX_FILEINPUTREADER_H

#include <URI.h>
#include <VirtualFileSystem.h>

namespace tuplex {

    class FileInputReader {
    public:
        virtual ~FileInputReader() {}
        virtual void read(const URI& inputFilePath) = 0;
        virtual size_t inputRowCount() const = 0;
    };
}

#endif //TUPLEX_FILEINPUTREADER_H