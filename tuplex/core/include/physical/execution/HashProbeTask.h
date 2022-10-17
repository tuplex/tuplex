//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HASHPROBETASK_H
#define TUPLEX_HASHPROBETASK_H

#include "IExecutorTask.h"
#include "physical/execution/TransformTask.h"
#include <hashmap.h>

namespace tuplex {

    class HashProbeTask : public IExecutorTask {
    private:
        Partition* _inputPartition;
        map_t _hmap; // the hashmap (pointer)
        void(*_functor)(void*, map_t, const uint8_t*);
        MemorySink _output;
        Schema _outputSchema;
        int64_t _outputDataSetID;
        int64_t _contextID;
    public:
        HashProbeTask(Partition *partition, map_t hmap,
                      void(*functor)(void *, map_t, const uint8_t *), const python::Type &joinedRowType,
                      int64_t outputDataSetID,
                      int64_t contextID) : _inputPartition(partition), _hmap(hmap),
                      _functor(functor), _outputSchema(Schema(Schema::MemoryLayout::ROW, joinedRowType)),
                      _outputDataSetID(outputDataSetID), _contextID(contextID) {

        }

        static codegen::write_row_f writeRowCallback();

        std::vector<Partition*> getOutputPartitions() const override { return _output.partitions; }

        int64_t writeRowToMemory(uint8_t* buf, int64_t bufSize);

        void execute() override;
        TaskType type() const override { return TaskType::HASHPROBE; }

        void releaseAllLocks() override;
    };
}

#endif //TUPLEX_HASHPROBETASK_H