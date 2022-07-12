//
// Created by Leonhard Spiegelberg on 1/31/22.
//

#ifndef TUPLEX_FILEPART_H
#define TUPLEX_FILEPART_H

#include <string>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <stdexcept>

#include "FileUtils.h"
#include "URI.h"

namespace tuplex {
    struct FilePart {
        URI uri;
        size_t partNo; // when trying to restore in order, select here partNo
        size_t rangeStart; // rangeStart == rangeEnd == 0 indicate full file!
        size_t rangeEnd;
        size_t size; // file size

        FilePart() : uri(URI::INVALID), partNo(0), rangeStart(0), rangeEnd(0), size(0) {}

        FilePart(const FilePart& other) : uri(other.uri), partNo(other.partNo),
        rangeStart(other.rangeStart), rangeEnd(other.rangeEnd), size(other.size) {}

        inline FilePart& operator = (const FilePart& other) {
            uri = other.uri;
            partNo = other.partNo;
            rangeStart = other.rangeStart;
            rangeEnd = other.rangeEnd;
            size = other.size;
            return *this;
        }

        inline size_t part_size() const {
            if(0 == rangeStart && 0 == rangeEnd)
                return size;
            return rangeEnd - rangeStart;
        }
    };

    /*!
     * helper function to help distribute file processing into multiple parts across different threads
     * @param numThreads on how many threads to split file processing
     * @param uris which uris
     * @param file_sizes which file sizes
     * @param minimumPartSize minimum part size, helpful to avoid tiny parts (default: 4KB)
     * @return vector<vector<FileParts>>
     */
    extern std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads,
                                                                  const std::vector<URI>& uris,
                                                                  const std::vector<size_t>& file_sizes,
                                                                  size_t minimumPartSize=1024 * 4);

    /*!
     * helper function to distribute file parts equally according to size across multiple threads
     * @param numThreads across how many threads to split
     * @param parts full files or parts of files
     * @param minimumPartSize minimum part size to acknowledge while splitting. Note, that parts are assumed to be larger than that.
     */
    extern std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads,
                                                                  const std::vector<FilePart>& parts,
                                                                  size_t minimumPartSize);
}

#endif //TUPLEX_FILEPART_H
