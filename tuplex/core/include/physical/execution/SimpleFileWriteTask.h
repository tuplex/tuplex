//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SIMPLEFILEWRITETASK_H
#define TUPLEX_SIMPLEFILEWRITETASK_H

#include "physical/execution/IExecutorTask.h"

namespace tuplex {

/*!
 * simple class to write several blocks of memory to file. Helpful i.e. for CSV/JSON output.
 */
class SimpleFileWriteTask : public IExecutorTask {
public:
    SimpleFileWriteTask() = delete;
    SimpleFileWriteTask(const SimpleFileWriteTask& other) = default;
    SimpleFileWriteTask(const URI& uri, uint8_t *header, size_t header_length, const std::vector<Partition *> &partitions) : _uri(uri), _header(
            header), _headerLength(header_length), _partitions(partitions.begin(), partitions.end()) {
    }

    void execute() override {
        auto& logger = Logger::instance().defaultLogger();

        Timer timer;

        // open file, go through partitions and write data
        if(_uri == URI::INVALID) {
            abort("invalid URI to writeToFile Task given");
            return;
        }

        // something to write?
        if(_partitions.empty())
            return;

        auto outFile = VirtualFileSystem::open_file(_uri, VirtualFileMode::VFS_WRITE);
        if(!outFile) {
            abort("could not open " + _uri.toPath() + " in write mode.");
            return;
        }

        // write prefix
        if(_header && _headerLength > 0) {
            outFile->write(_header, _headerLength);
        }

        // write all bytes from partitions
        size_t totalBytes = 0;
        size_t totalRows = 0;
        for(auto p : _partitions) {
            auto numBytes = p->bytesWritten();
            totalBytes += numBytes;
            totalRows += p->getNumRows();
            auto dataptr = p->lock();
            // write to file
            outFile->write(dataptr, numBytes);
            p->unlock();
            p->invalidate(); // free partition
        }

        outFile->close();

        // done.
        std::stringstream ss;
        ss<<"[Task Finished] write to file in "
          <<std::to_string(timer.time())<<"s (";
        ss<<pluralize(totalRows, "row")<<", "<<sizeToMemString(totalBytes)<<")";
        owner()->info(ss.str());
    }

    TaskType type() const override { return TaskType::SIMPLEFILEWRITE; }
    std::vector<Partition*> getOutputPartitions() const override { return std::vector<Partition*>{}; }

    void releaseAllLocks() override {
        for(auto p : _partitions)
            p->unlock();
    }

private:
    URI _uri;
    std::vector<Partition *> _partitions;
    uint8_t *_header;
    size_t _headerLength;

    void abort(const std::string& message) {}
};

}
#endif //TUPLEX_SIMPLEFILEWRITETASK_H