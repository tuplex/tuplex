//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TASKFUTURE_H
#define TUPLEX_TASKFUTURE_H

#include <future>

namespace tuplex {
    template<typename T> class TaskFuture {
    private:
        std::future<T> _future;
    public:
        TaskFuture(std::future<T>&& future): _future(std::move(future)) {}

        TaskFuture(const TaskFuture& rhs) = delete;
        TaskFuture& operator=(const TaskFuture& rhs) = delete;
        TaskFuture(TaskFuture&& other) = default;
        TaskFuture& operator=(TaskFuture&& other) = default;

        ~TaskFuture() {
            if(_future.valid())
                _future.get(); // blocking call
        }

        T get() { return _future.get(); }
    };
}

#endif //TUPLEX_TASKFUTURE_H