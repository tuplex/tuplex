//
// Created by Leonhard Spiegelberg on 1/31/22.
//

#include <FilePart.h>

namespace tuplex {
    std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads,
                                                           const std::vector<FilePart>& parts,
                                                           size_t minimumPartSize) {
        using namespace std;

        size_t partNo = 0;

        auto vv = vector<vector<FilePart>>(numThreads, vector<FilePart>{});

        // check how many bytes each vector should get
        size_t totalBytes = 0;
        for(auto part : parts) {
            if(part.rangeStart == 0 && part.rangeEnd == 0) {
                totalBytes += part.size;
            } else {
                totalBytes += part.rangeEnd - part.rangeStart;
            }
        }

        if(0 == totalBytes)
            return vv;

        auto bytesPerThread = totalBytes / numThreads;

        // set bytes per thread to a minimum
        bytesPerThread = std::max(bytesPerThread, minimumPartSize);

        assert(bytesPerThread != 0);

        // do not process empty files...
        size_t curThreadSize = 0;
        unsigned curThread = 0;
        for(unsigned i = 0; i < parts.size(); ++i) {

            auto file_size = parts[i].size;
            assert(parts[i].rangeEnd >= parts[i].rangeStart);
            auto part_size = (parts[i].rangeStart == 0 && parts[i].rangeEnd == 0) ? file_size : parts[i].rangeEnd - parts[i].rangeStart;
            auto uri = parts[i].uri;

            // skip empty files...
            if(0 == file_size)
                continue;

            // does current thread get the whole file?
            if(curThreadSize + part_size <= bytesPerThread && part_size > 0) {
                FilePart fp;
                fp.uri = uri;
                fp.rangeStart = parts[i].rangeStart;
                fp.rangeEnd = parts[i].rangeEnd;
                fp.size = file_size;
                fp.partNo = partNo++; // new part number!
                vv[curThread].emplace_back(fp);
                curThreadSize += part_size;
            } else {
                // no, current thread gets part of the file only
                // rest of file needs to get distributed among other threads!

                // split into parts & inc curThread
                size_t remaining_bytes = part_size;
                size_t offset = parts[i].rangeStart;
                size_t cur_start = 0; // byte counter

                size_t bytes_this_thread_gets = bytesPerThread > curThreadSize ? bytesPerThread - curThreadSize : 0;

                if(bytes_this_thread_gets > 0) {
                    // split into part (depending on how many bytes remain)
                    FilePart fp;
                    fp.uri = uri;
                    fp.rangeStart = offset + cur_start;
                    fp.rangeEnd = offset + cur_start + std::min(bytes_this_thread_gets, remaining_bytes);
                    fp.partNo = partNo++;
                    fp.size = file_size;

                    // correction to avoid tiny parts...
                    auto this_part_size = fp.rangeEnd - fp.rangeStart;
                    if(remaining_bytes < minimumPartSize + cur_start + this_part_size) {
                        // add remaining bytes to this part!
                        fp.rangeEnd = offset + part_size;
                    }

                    cur_start += fp.rangeEnd - fp.rangeStart;
                    vv[curThread].emplace_back(fp);
                }

                // go to next thread
                // check how many bytes are left, distribute large file...
                curThread = (curThread + 1) % numThreads;
                curThreadSize = 0;

                // split into parts...
                while(cur_start < remaining_bytes) {

                    bytes_this_thread_gets = remaining_bytes - cur_start;

                    // now current thread gets the current file (or a part of it)
                    if(curThreadSize + bytes_this_thread_gets <= bytesPerThread && bytes_this_thread_gets > 0) {
                        FilePart fp;
                        fp.uri = uri;
                        fp.rangeStart = offset + cur_start;
                        fp.rangeEnd = offset + cur_start + bytes_this_thread_gets;
                        fp.partNo = partNo++;
                        fp.size = file_size;

                        // correction to avoid tiny parts...
                        auto this_part_size = fp.rangeEnd - fp.rangeStart;
                        if(remaining_bytes - cur_start - this_part_size < minimumPartSize) {
                            // add remaining bytes to this part!
                            fp.rangeEnd = offset + part_size;
                        }

                        curThreadSize += fp.rangeEnd - fp.rangeStart;
                        cur_start += fp.rangeEnd - fp.rangeStart;

                        // full file? -> use 0,0 as special value pair
                        if(fp.rangeStart == 0 && fp.rangeEnd == file_size)
                            fp.rangeEnd = 0;

                        vv[curThread].emplace_back(fp);
                    } else {
                        // thread only gets a part, inc thread!
                        bytes_this_thread_gets = std::min(bytes_this_thread_gets, bytesPerThread);
                        FilePart fp;
                        fp.uri = uri;
                        fp.rangeStart = offset + cur_start;
                        fp.rangeEnd = offset + cur_start + bytes_this_thread_gets;
                        fp.partNo = partNo++;
                        fp.size = file_size;

                        // correction to avoid tiny parts...
                        auto this_part_size = fp.rangeEnd - fp.rangeStart;
                        if(remaining_bytes - cur_start - this_part_size < minimumPartSize) {
                            // add remaining bytes to this part!
                            fp.rangeEnd = offset + part_size;
                        }

                        curThreadSize += fp.rangeEnd - fp.rangeStart;
                        cur_start += fp.rangeEnd - fp.rangeStart;

                        // full file? -> use 0,0 as special value pair
                        if(fp.rangeStart == 0 && fp.rangeEnd == file_size)
                            fp.rangeEnd = 0;

                        vv[curThread].emplace_back(fp);

                        // next thread!
                        curThread = (curThread + 1) % numThreads;
                        curThreadSize = 0;
                    }
                }
            }
        }

        return vv;
    }

    extern std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads,
                                                                  const std::vector<URI>& uris,
                                                                  const std::vector<size_t>& file_sizes,
                                                                  size_t minimumPartSize) {
        using namespace std;
        vector<FilePart> parts;

        if(uris.size() != file_sizes.size())
            throw std::runtime_error("invalid number of uris/file sizes.");

        parts.reserve(uris.size());
        for(unsigned i = 0; i < uris.size(); ++i) {
            FilePart fp;
            fp.uri = uris[i];
            fp.rangeStart = 0;
            fp.rangeEnd = 0;
            fp.partNo = i;
            fp.size = file_sizes[i];
            parts.push_back(fp);
        }

        return splitIntoEqualParts(numThreads, parts, minimumPartSize);
    }
}