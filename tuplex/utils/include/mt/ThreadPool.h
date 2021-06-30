//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_THREADPOOL_H
#define TUPLEX_THREADPOOL_H

#include "mt/blockingconcurrentqueue.h"
#include "mt/ITask.h"
#include "ThreadTask.h"
#include "mt/TaskFuture.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <unordered_map>



namespace tuplex {

    using TaskQueueType=moodycamel::BlockingConcurrentQueue<std::unique_ptr<ITask>>;

    // based on http://roar11.com/2016/01/a-platform-independent-thread-pool-using-c14/
    //! a class to submit tasks
    class ThreadPool {
    public:
        // user defined functions to call at start & exit of each thread by the thread
//        typedef void(*worker_init_f)(std::thread::id);
//        typedef void(*worker_release_f)(std::thread::id);
        using worker_init_f=std::function<void(std::thread::id)>;
        using worker_release_f=std::function<void(std::thread::id)>;
    private:
        std::atomic_bool _done;
        std::vector<std::thread> _threads;
        std::atomic_int _numThreads;
        TaskQueueType _workQueue;

        std::mutex _completedTasksMutex;
        std::vector<std::shared_ptr<ITask>> _completedTasks;
        std::atomic_int _numPendingTasks;


        worker_init_f _init_f;
        worker_release_f _release_f;

        /*!
         * helper function for each thread to fetch the next available task
         */
        void worker(worker_init_f init_f, worker_release_f release_f);

    public:
        explicit ThreadPool(const uint32_t numThreads,
                worker_init_f init_f=nullptr,
                worker_release_f release_f=nullptr,
                bool autoStart = true);


        ThreadPool() : ThreadPool(std::max(std::thread::hardware_concurrency(), 2u) - 1) {
            // hardware concurrency may return 0 or 1, so to be safe yield at least 1 thread.
        }

        ~ThreadPool() { release(); }

        // not copyable & assignable
        ThreadPool(const ThreadPool& rhs) = delete;
        ThreadPool& operator = (const ThreadPool& rhs) = delete;

        /*!
         * starts threadpool
         */
        void start();

        /*!
         * info on whether threadpool terminated or not
         * @return
         */
        bool done() const { return _done; }

        /*!
         * waits until all tasks in the thread pool are finished. (Calling thread will help to consume tasks).
         */
        void waitForAll(bool helpOut=true);

        /*!
         * helper function to execute an arbitrary function via the threadpool
         * @tparam Func type of the function
         * @tparam Args arguments types of the function
         * @param func function to execute
         * @param args arguments to pass to the function
         * @return result of the supplied function
         */
        template<typename Func, typename... Args> auto submit(Func&& func, Args&&... args) {

            auto boundTask = std::bind(std::forward<Func>(func), std::forward<Args>(args)...);
            using ResultType = std::result_of_t<decltype(boundTask)()>;
            using PackagedTask = std::packaged_task<ResultType()>;
            using TaskType = ThreadTask<PackagedTask>;

            PackagedTask task{std::move(boundTask)};
            TaskFuture<ResultType> result{task.get_future()};

            _workQueue.enqueue(std::make_unique<TaskType>(std::move(task)));

            _numPendingTasks.fetch_add(1, std::memory_order_release);
            return result;
        }


        void addTask(std::unique_ptr<ITask>&& task) {
            _workQueue.enqueue(std::move(task));
            _numPendingTasks.fetch_add(1, std::memory_order_release);
        }

        /*!
         * status of tasks queued.
         * @return true if there are no tasks left to complete.
         */
        bool completed() { return _numPendingTasks.load(std::memory_order_acquire) == 0; }

        /*!
         * pops all completed tasks at this stage. This is a blocking operation.
         * @return vector of pointers to completed tasks.
         */
        std::vector<std::shared_ptr<ITask>> popCompletedTasks();

        /*!
         * get thread ids of threads in pool
         * @param includeCaller also returns id of the calling thread
         * @return vector of thread ids
         */
        std::vector<std::thread::id> getThreadIds(bool includeCaller=false) const {
            std::vector<std::thread::id> ids;
            for(auto i = 0u; i < _threads.size(); ++i)
                ids.push_back(_threads[i].get_id());

            // if true, include ID of caller (search that not already contained)
            if(includeCaller) {
                auto this_id = std::this_thread::get_id();
                if(std::find(ids.begin(), ids.end(), this_id) == ids.end())
                    ids.push_back(this_id);
            }
            return ids;
        }

        void release();
    };

    /*!
     * assigns lazy numbers to thr eads
     * @return
     */
    extern uint64_t getThreadNumber();

    extern void resetThreadNumbers();
}


#endif //TUPLEX_THREADPOOL_H