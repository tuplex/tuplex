//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ee/IBackend.h>

namespace tuplex {
    // @TODO: add here common backend functions when multiple backends are supported...

    std::vector<std::tuple<std::string, size_t> > decodeFileURIs(const std::vector<Partition *> &partitions, bool invalidate) {
        using namespace std;
        vector<std::tuple<std::string, size_t> > infos;

        auto fileSchema = Schema(Schema::MemoryLayout::ROW,
                                 python::Type::makeTupleType({python::Type::STRING, python::Type::I64}));

        for (auto partition: partitions) {
            // get num
            auto numFiles = partition->getNumRows();
            const uint8_t *ptr = partition->lock();
            size_t bytesRead = 0;
            // found
            for (int i = 0; i < numFiles; ++i) {
                // found file -> create task / split into multiple tasks
                Row row = Row::fromMemory(fileSchema, ptr, partition->capacity() - bytesRead);
                auto path = row.getString(0);
                size_t file_size = row.getInt(1);

                infos.push_back(make_tuple(path, file_size));
                ptr += row.serializedLength();
                bytesRead += row.serializedLength();
            }

            partition->unlock();

            if (invalidate)
                partition->invalidate();
        }

        return infos;
    }
}