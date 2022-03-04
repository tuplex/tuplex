//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_EXECUTOR_H
#define TUPLEX_EXECUTOR_H

#include <thread>
#include <deque>
#include <Utils.h>
#include <Logger.h>
#include <VirtualFileSystem.h>
#include <Timer.h>
#include <list>
#include <boost/thread/shared_mutex.hpp>
#include <mt/ThreadPool.h>
#include <BitmapAllocator.h>
#include <Schema.h>
#include "Context.h"
#include "HistoryServerConnector.h"
#include "physical/IExecutorTask.h"

namespace tuplex {

    // this here is a redesign of the current MemoryManager implementation

    class Executor;
    class IExecutorTask;
    class Partition;
    class WorkQueue;
    class HistoryServerConnector;
    class Context;

    using ExecutorTaskQueueType=moodycamel::BlockingConcurrentQueue<IExecutorTask*>;

    /*!
     * helper class to attach Tasks to
     */
    class WorkQueue {
    private:
        std::atomic_bool _done{}; // protects against data races
        ExecutorTaskQueueType _queue;
        std::mutex _completedTasksMutex;
        std::vector<IExecutorTask*> _completedTasks;
        std::atomic_int _numPendingTasks{};
        std::atomic_int _numCompletedTasks{};

        // mapping from order number -> row count if the task is finished
        std::mutex _rowsDoneMutex;
        std::map<size_t, size_t> _rowsDone;

        std::atomic_int _frontRowsLimit{};
        std::atomic_int _bottomRowsLimit{};
    public:

        WorkQueue();

        ~WorkQueue() {
        }

        /*!
         * MT safe function to add a task to the working queue
         * @param task
         */
        void addTask(IExecutorTask* task) {
            if(!task)
                return;
            _queue.enqueue(task);
            _numPendingTasks.fetch_add(1, std::memory_order_release);
        }

        size_t numPendingTasks() const {
            return _numPendingTasks;
        }

        size_t numCompletedTasks() const { return _numCompletedTasks; }

        size_t frontRowsLimit() const {
            return _frontRowsLimit;
        };

        size_t bottomRowsLimit() const {
            return _bottomRowsLimit;
        };

        /*!
         * stop working on this queue & dump all tasks
         */
        void cancel();

        /*!
         * blocking work on one task. To be called from any worker thread
         * @param Executor the executor who works on this task. (I.e. the caller)
         * @param nonBlocking if true, may not work at all at a task. Only if dequeuing worked.
         * @return true if task was worked on, false else
         */
        bool workTask(Executor& executor, bool nonBlocking=true);

        /*!
         * blocking call until all tasks on this queue are worked on
         */
        void waitUntilAllTasksFinished();


        /*!
         * use executor in current thread to also work on tasks.
         * @param executor i.e., the driver
         * @param flushPeriodicallyToPython whether to invoke the GIL and call Logger::flushToPython after each task the driver finished.
         */
        void workUntilAllTasksFinished(Executor& executor, bool flushPeriodicallyToPython=false);


        std::vector<IExecutorTask*> popCompletedTasks();

        /*!
         * removes all tasks from the queue & resets counters
         */
        void clear();
    };


    /*!
     * helper class to make Mutexes traceable
     */
    class Mutex : public std::mutex {
    private:
#ifndef NDEBUG
    std::thread::id _holder;
#endif
    public:
#ifndef NDEBUG
        void lock() {
            std::mutex::lock();
            _holder = std::this_thread::get_id();
        }
#endif

#ifndef NDEBUG
        void unlock() {
            _holder = std::thread::id();
            std::mutex::unlock();
        }
#endif

#ifndef NDEBUG
        bool locked_by_caller() const {
            return _holder == std::this_thread::get_id();
        }
#endif
    };



    // one executor == one thread (may be also one process)
    class Executor {
    private:

        // name to identify this executor
        std::string _name;

        size_t maxMemory() { return _allocator.size(); }

        // currently used memory
        size_t usedMemory();

        // make this maybe abstract.. --> i.e. throw out partitions based on some strategy...
        BitmapAllocator _allocator;

         // mutex for modifying this list
        boost::shared_mutex _listMutex;

        // LRU based list
        std::list<Partition*> _partitions; //! active, currently used partitions
        std::list<Partition*> _storedPartitions; //! cached/stored partitions on disk

        // which thread does this executor belong too?
        std::thread::id         _threadID;
        size_t                  _threadNumber;

        // unique id of this executor
        uniqueid_t              _uuid;

        URI _cache_path;

        URI getPartitionURI(Partition* partition) const;

        // no locks used within
        void evictLRUPartition();

        // perform this in separate thread
        // TaskQueue
        std::atomic_bool _done;
        //ExecutorTaskQueueType _workQueue;


        // atomic workqueue (note: executor may be attached or not!)
        std::atomic<WorkQueue*> _workQueue;

        std::thread _thread;

        /*!
         * helper function for each thread to fetch the next available task
         */
        void worker();

        // another memory pool for python runtime memory as needed by the compiled UDFs
        size_t _runTimeMemory;
        size_t _runTimeMemoryDefaultBlockSize;

        // weak reference to history server
        // C++20 will intro atomic smart ptr, maybe change then...
        std::atomic<HistoryServerConnector*> _historyServer;
    public:

        Executor(const size_t size,
                const size_t blockSize,
                const size_t runTimeMemory,
                const size_t runTimeMemoryDefaultBlockSize,
                URI cache_path,
                const std::string& name = "");

        Executor(const Executor& other) = delete;
        Executor& operator = (const Executor& other) = delete;

        virtual ~Executor() {
            release();
        }

        std::string name() const { return _name; }

        inline void info(const std::string& message) {
            Logger::instance().logger(_name).info(message);
        }

        inline void error(const std::string& message) {
            Logger::instance().logger(_name).error(message);
        }

        // for multiple stages, there should be an acquire function
        // based on thread id.
        // this function may be only called by the thread owning this partition
        Partition* allocWritablePartition(const size_t minRequired, const Schema& schema, const int dataSetID, const int contextID);

        // two things can happen
        // (1) own partition --> release
        // (2) other executor

        // beware! this is not multithreading safe!
        // only call carefully...
        void freePartition(Partition* partition);

        /*!
         * frees all partitions belogning to context ctx
         * @param ctx a Context object
         */
        void freeAllPartitionsOfContext(const Context* ctx=nullptr);

        void setThreadNumber(size_t threadNumber) { _threadNumber = threadNumber; }

        /*!
         * load partition from disk
         * @param partition
         */
        void recoverPartition(Partition* partition);

        /*!
         * @return thread ID of the thread belonging to this particular executor
         */
        std::thread::id getThreadID() const { return _threadID; }
        size_t threadNumber() const { return _threadNumber; }


        /*!
         * BLOCKING sets partition as the most recently used one
         * @param partition
         */
        void makeRecentlyUsed(Partition *partition);


        // @ Todo: Later, attach each Executor to a scheduler. Tasks are then submitted to the Scheduler.
        // the Scheduler basically manages which executor gets which task...

        /*!
         * start processing queue (infinite loop)
         * @param detached if true, then a separate thread is created that runs infinitely until waitForAllTasksFinished is called.
         * Else, simply in the current thread all tasks currently in the queue are processed.
         */
        void processQueue(bool detached=true);


        // attach a new working queue to this executor
        /*!
         * attach Executor to a working queue
         * @param queue where to attach Executor to, will automatically start pulling & executing tasks
         */
        void attachWorkQueue(WorkQueue* queue);

        /*!
         * removes this executor from any working queue
         */
        void removeFromQueue();

        /*!
         * stops executor
         */
        void release();

        size_t memorySize() const { return _allocator.size(); }
        size_t blockSize() const { return _allocator.blockSize(); }
        size_t runTimeMemorySize() const { return _runTimeMemory; }
        size_t runTimeMemoryDefaultBlockSize() const { return _runTimeMemoryDefaultBlockSize; }

        /*!
         * is worker still running his process queue?
         * @return
         */
        bool isRunning() const {return !_done;}


        // don't inline this because of memory constraints
        HistoryServerConnector* historyServer() const {
            HistoryServerConnector* hs = _historyServer.load(std::memory_order::memory_order_acquire);
            return hs;
        }


        /*!
         * updates internal history server connector reference. thread safe
         * @param hs
         */
        void setHistoryServer(HistoryServerConnector *hs=nullptr);
    };

}

#endif //TUPLEX_EXECUTOR_H