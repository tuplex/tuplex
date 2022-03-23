//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/execution/HashProbeTask.h>
#include <jit/RuntimeInterface.h>

extern "C" {
    static int64_t writeJoinedRow(tuplex::HashProbeTask* task, uint8_t* buf, int64_t bufSize) {
        assert(task);
        assert(dynamic_cast<tuplex::HashProbeTask*>(task));
        return task->writeRowToMemory(buf, bufSize);
    }
}
namespace tuplex {

    // TODO...

    codegen::write_row_f HashProbeTask::writeRowCallback() {
        return reinterpret_cast<codegen::write_row_f>(writeJoinedRow);
    }

    int64_t HashProbeTask::writeRowToMemory(uint8_t *buf, int64_t bufSize) {
        return rowToMemorySink(owner(), _output, _outputSchema, _outputDataSetID, _contextID, buf, bufSize);
    }

    void HashProbeTask::execute() {
        assert(owner());
        assert(_functor);

        Timer timer;

        runtime::rtfree_all();
        _output.reset();

        auto data_ptr = _inputPartition->lockRaw(); // important, in order to extract incl. numrows counter

        // call functor
        _functor(this, _hmap, data_ptr);

        // unlock sink
        _output.unlock();

        // unlock input partition
        _inputPartition->unlock();

        runtime::rtfree_all();

        // invalidate input partition!
        _inputPartition->invalidate();

        std::stringstream ss;
        ss<<"[Task Finished] Hash in "
          <<std::to_string(timer.time())<<"s ("
          <<pluralize(getNumOutputRows(), "normal row")<<")";
        owner()->info(ss.str());

        // TODO: history server notification!
    }

}