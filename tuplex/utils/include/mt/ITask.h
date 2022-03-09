//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ITASK_H
#define TUPLEX_ITASK_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>
#include <thread>
#include <cassert>

namespace tuplex {

/*!
 * interface for defining tasks that can be run via a threadpool
 */
class ITask {
private:
    std::thread::id _id; //! the id of the thread that executed the task. Used to specifically execute a task on a specific thread.
//! Per default object is constructed that does not represent a thread

    std::vector<size_t> _orderNumbers; //! for sorting tasks when doing async processing, allows for multiple stages

public:
    ITask() {};
    ITask(const ITask& other) : _id(other._id), _orderNumbers(other._orderNumbers)  {}
    virtual ~ITask() = default;
    ITask(ITask&& other) = default;
    ITask& operator = (ITask&& other) = default;

    /*!
     * interface to run a task
     */
    virtual void execute() = 0;

    std::thread::id getID() {
        return _id;
    }

    void setID(const std::thread::id& id) {
        _id = id;
    }

    void setOrder(size_t order) { _orderNumbers = std::vector<size_t>{order}; }

    size_t getOrder(const size_t nth) const {
        return _orderNumbers[nth];
    }

    std::vector<size_t> getOrder() const { return _orderNumbers; }

    void setOrder(const std::vector<size_t>& order) {
        _orderNumbers = order;
    }

    /*!
     * compare the ordering numbers of two tasks to restore initial data order after processing multiple ones
     * @param other
     * @return
     */
    bool compareAscOrder(const ITask& other) const {
        // make sure they have the same length
        assert(_orderNumbers.size() == other._orderNumbers.size());

        // this < other?
        // compare one by one
        for(int i = 0; i < other._orderNumbers.size(); ++i) {
            if(_orderNumbers[i] >= other._orderNumbers[i])
                return false;
        }
        return true;
    }
};
}
#endif //TUPLEX_ITASK_H