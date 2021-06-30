//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_THREADTASK_H
#define TUPLEX_THREADTASK_H

#include "mt/ITask.h"

namespace tuplex {

    /*!
     * Wrapper around a single function
     * @tparam Func
     */
    template<typename Func> class ThreadTask : public ITask {
    private:
        Func _func;
    public:
        ThreadTask(std::thread::id id, Func&& func) : _func(std::move(func))    { setID(id); }
        ThreadTask(Func&& func) : ThreadTask(std::thread::id(), std::move(func))    {}

        ~ThreadTask() override = default;

        ThreadTask(const ThreadTask& other) = delete;
        ThreadTask& operator = (const ThreadTask& other) = delete;
        ThreadTask(ThreadTask&& other) = default;
        ThreadTask&operator = (ThreadTask&& other) = default;

        void execute() override {
            _func();
        }
    };
}
#endif //TUPLEX_THREADTASK_H