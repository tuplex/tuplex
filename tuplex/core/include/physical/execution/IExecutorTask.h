//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IEXECUTORTASK_H
#define TUPLEX_IEXECUTORTASK_H

#include "physical/memory/Partition.h"
#include "ee/local/Executor.h"
#include "TaskTypes.h"

namespace tuplex {

    class Executor;
    class IExecutorTask;
    class Partition;

    /*!
     * special class used for task to be delivered to executors
     */
    class IExecutorTask : public ITask {
    private:
        Executor* _owner; //! executor to which this task belongs to
        size_t _threadNumber; //! a number to use for thread-local indexing. == 0 for main-thread/driver.
    public:
        IExecutorTask() : _owner(nullptr), _threadNumber(-1)   {}
        IExecutorTask(const IExecutorTask& other) : _owner(other._owner), _threadNumber(other._threadNumber)    {}

        ~IExecutorTask() override { _owner = nullptr; }

        void setOwner(Executor *owner) { assert(owner); _owner = owner; }
        Executor* owner() { assert(_owner); return _owner; }

        size_t threadNumber() const { return _threadNumber; }
        void setThreadNumber(size_t threadNumber) { _threadNumber = threadNumber; }

        virtual std::vector<Partition*> getOutputPartitions() const = 0;

        virtual size_t getNumOutputRows() const;
        virtual size_t getNumInputRows() const { return 0; }

        /*!
         * how much wall-time this task took.
         * @return
         */
        virtual double wallTime() const { return 0.0; }

        virtual TaskType type() const = 0;

        /*!
         * used when an exception is thrown to release all pending locks. -> else, deadlock.
         */
        virtual void releaseAllLocks() = 0;
    };


    /*!
     * sort tasks after their ord
     * @param tasks
     */
    extern void sortTasks(std::vector<IExecutorTask*>& tasks);

    /*!
     * compare two orders ascending
     * @param ordA
     * @param ordB
     * @return
     */
    inline bool compareOrders(const std::vector<size_t> &ordA, const std::vector<size_t> &ordB) {
        assert(ordA.size() > 0 && ordB.size() > 0);

        assert(ordA.size() == ordB.size());

        if(ordA.empty() && ordB.empty())
            return true;

        // order first after first entry, the  after second entry, ...
        for(int j = 0; j < ordB.size(); j++) {
            if(ordA[j] < ordB[j])
                return true;
            else if(ordA[j] > ordB[j])
                return false;
        }

        return false;
    }
}

#endif //TUPLEX_IEXECUTORTASK_H