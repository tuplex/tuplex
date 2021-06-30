//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "mt/ThreadPool.h"
#include <Logger.h>

namespace tuplex {

    // helper function to assign each thread a number
    static std::mutex g_thread_lookupMutex;
    static std::unordered_map<std::thread::id, uint64_t> g_thread_lookup;
    uint64_t getThreadNumber() {
        std::lock_guard<std::mutex> lock_guard(g_thread_lookupMutex);
        if(g_thread_lookup.find(std::this_thread::get_id()) == g_thread_lookup.end())
            g_thread_lookup[std::this_thread::get_id()] = g_thread_lookup.size();
        return g_thread_lookup[std::this_thread::get_id()];
        return 0;
    }

    void resetThreadNumbers() {
        std::lock_guard<std::mutex> lock_guard(g_thread_lookupMutex);
        g_thread_lookup.clear();
    }

    void ThreadPool::release() {
        _done = true;

        // @Todo: should probably call on remaining tasks abort

        // join all threads
        for(auto& thread : _threads) {
            if(thread.joinable())
                thread.join();
        }
    }

    void ThreadPool::worker(worker_init_f init_f, worker_release_f release_f) {
        auto this_id = std::this_thread::get_id();

        // call init func if != nullptr
        if(init_f)
            init_f(this_id);

        while(!_done) {
            std::unique_ptr<ITask> task(nullptr);

            // dequeue from general working queue
            if(_workQueue.try_dequeue(task)) {
                // process task
                task->execute();
                // save which thread executed this task
                task->setID(this_id);

                _numPendingTasks.fetch_add(-1, std::memory_order_release);

                // add task to done list
                _completedTasksMutex.lock();
                _completedTasks.push_back(std::move(task));
                _completedTasksMutex.unlock();
            }
        }

        if(release_f)
            release_f(this_id);
    }

    void ThreadPool::start() {

        // should not been started
        if(_threads.size() > 0) {
            Logger::instance().defaultLogger().warn("threadpool already started, should not be done twice");
            return;
        }

        // create threads
        try {
            for(auto i = 0u; i < _numThreads; ++i) {
                _threads.emplace_back(&ThreadPool::worker, this, _init_f, _release_f);
            }
        } catch(...) {
            release();
            throw;
        }
    }

    // @ Todo: deferred start of threads...
    ThreadPool::ThreadPool(const uint32_t numThreads, worker_init_f init_f, worker_release_f release_f, bool autoStart) : _done(false),
                                                        _workQueue{},
                                                        _numThreads(numThreads),
                                                        _threads{},
                                                        _numPendingTasks(0),
                                                        _completedTasks{} {
        _init_f = init_f;
        _release_f = release_f;

        if(autoStart)
            start();

    }

    // @Todo: allow for interrupt handler (may be external or via Python)
    // should note when python is run in standalone mode
    // i.e. a lambda that if returns true, will kill the threadpool
    void ThreadPool::waitForAll(bool helpOut) {
        if(helpOut) {
            // use this thread to also process tasks
            while (_numPendingTasks.load(std::memory_order_acquire) != 0) {
                std::unique_ptr<ITask> task(nullptr);
                if (!_workQueue.try_dequeue(task))
                    continue;

                // process task
                task->execute();
                _numPendingTasks.fetch_add(-1, std::memory_order_release);
            }
        } else {
            // simply wait for all outstanding tasks to finish
            while(_numPendingTasks.load(std::memory_order_acquire) != 0)
                continue;
        }
    }

    std::vector<std::shared_ptr<ITask>> ThreadPool::popCompletedTasks() {
        std::lock_guard<std::mutex> lock(_completedTasksMutex);
        std::vector<std::shared_ptr<ITask>> res;
        std::copy(_completedTasks.begin(), _completedTasks.end(), std::back_inserter(res));
        _completedTasks.clear();
        return res;
    }
}