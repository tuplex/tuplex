//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "Executor.h"
#include <VirtualFileSystem.h>
#include <RuntimeInterface.h>
#include <Partition.h>
#include <atomic>
#include <unistd.h>
#include <Signals.h>


// Notes on Multithreaded programming:
// to avoid deadlocks, the locks should be obtained always in the same order
// i.e.
// FIRST lock listMutex (Executor)
// then lock partition mutex

namespace tuplex {

    WorkQueue::WorkQueue() {
        _numPendingTasks = 0;
        _numCompletedTasks = 0;
    }

    std::vector<IExecutorTask*> WorkQueue::popCompletedTasks() {
        TRACE_LOCK("workQueue");
        std::lock_guard<std::mutex> lock(_completedTasksMutex);

        // move leads to circular dependency in gcc and thus a bug on travis-ci. Therefore, just
        // use the below hack to fool the compiler into actually copying the vectors
        // // move to reset completed tasks and return array
        // return std::move(_completedTasks);

        // no tasks? optimize! // => use here the if because of bugs in gcc when it comes to rvo
        if(_completedTasks.empty()) {
            TRACE_UNLOCK("workQueue");
            return std::vector<IExecutorTask*>{};
        }
        else {
            std::vector<IExecutorTask*> res;
            using namespace std;
            // cout<<"*** retrieving "<<_completedTasks.size()<<" tasks from work queue ***"<<endl;
            std::copy(_completedTasks.cbegin(), _completedTasks.cend(), std::back_inserter(res));
            _numCompletedTasks.fetch_add(-_completedTasks.size(), std::memory_order_release);
            _completedTasks.clear();
            TRACE_UNLOCK("workQueue");
            // cout<<"*** returning "<<res.size()<<" tasks / "<<_completedTasks.size()<<" left ***"<<endl;
            return res;
        }
    }

    void WorkQueue::clear() {
        // simply wait for all outstanding tasks to finish
        size_t pendingTasks = 0;
        while((pendingTasks = _numPendingTasks.load(std::memory_order_acquire)) != 0) {
            IExecutorTask *task = nullptr;
            if(_queue.try_dequeue(task)) {
                _numPendingTasks.fetch_add(-1, std::memory_order_release);
                _numCompletedTasks.fetch_add(1, std::memory_order_release);
            }
        }

        _completedTasksMutex.lock();
        _completedTasks.clear();
        _completedTasksMutex.unlock();
        _numPendingTasks = 0;
        _numCompletedTasks = 0;
    }

    bool WorkQueue::workTask(Executor& executor, bool nonBlocking) {

        IExecutorTask *task = nullptr;
        if(nonBlocking) {
            // @Todo: This should be put into a function "work" on the workQueue...
            // dequeue from general working queue
            if(_queue.try_dequeue(task)) {
                if(!task)
                    return false;

                task->setOwner(&executor);
                task->setThreadNumber(executor.threadNumber()); // redundant?

                //executor.logger().info("started task...");
                // process task
                task->execute();
                // save which thread executed this task
                task->setID(std::this_thread::get_id());

                _numPendingTasks.fetch_add(-1, std::memory_order_release);

                // add task to done list
                TRACE_LOCK("completedTasks");
                _completedTasksMutex.lock();
                _completedTasks.push_back(std::move(task));
                _completedTasksMutex.unlock();
                _numCompletedTasks.fetch_add(1, std::memory_order_release);
                TRACE_UNLOCK("completedTasks");
                return true;
            }
        } else {
            _queue.wait_dequeue(task);

            if(!task)
                return false;

            task->setOwner(&executor);
            task->setThreadNumber(executor.threadNumber()); // redundant?

            // process task
            task->execute();
            // save which thread executed this task
            task->setID(std::this_thread::get_id());

            // add task to done list
            TRACE_LOCK("completedTasks");
            _completedTasksMutex.lock();
            _completedTasks.push_back(std::move(task));
            _completedTasksMutex.unlock();
            _numCompletedTasks.fetch_add(1, std::memory_order_release);
            TRACE_UNLOCK("completedTasks");

            _numPendingTasks.fetch_add(-1, std::memory_order_release);
            return true;
        }
        return false;
    }

    void WorkQueue::workUntilAllTasksFinished(tuplex::Executor &executor) {
        int pendingTasks = 0;
        while((pendingTasks = _numPendingTasks.load(std::memory_order_acquire)) != 0) {

            // check for interrupt, if so clear queue!
            if(check_interrupted()) {
                clear();
                while(_numPendingTasks.load(std::memory_order_acquire) != 0) {
                    Logger::instance().defaultLogger().info("Waiting for tasks to end...");
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
                // leave loop & function
                return;
            }

            // work on task
            workTask(executor, true);
        }
    }

    void WorkQueue::waitUntilAllTasksFinished() {
        // simply wait for all outstanding tasks to finish
        int pendingTasks = 0;
        while((pendingTasks = _numPendingTasks.load(std::memory_order_acquire)) != 0) {

#ifndef NDEBUG
            Logger::instance().defaultLogger().info("there are " + std::to_string(pendingTasks) + " task(s) left to do...");
#endif

            // sleep for 5ms or do some other work...
            std::this_thread::sleep_for(std::chrono::milliseconds{5});
        }
    }

    void Executor::attachWorkQueue(tuplex::WorkQueue *queue) {
        assert(queue);
        assert(isRunning());

        // valid queue currently?
        WorkQueue* old = _workQueue.exchange(queue, std::memory_order_acquire);
        if(old) {
            // deregister executor from this queue...
        }
    }

    void Executor::removeFromQueue() {
        // valid queue currently?
        WorkQueue* old = _workQueue.exchange(nullptr, std::memory_order_acquire);
        if(old) {
            // deregister executor from this queue...
        }
    }


    static size_t g_execNumbers = 1;
    std::string makeExecutorName(const std::string& name) {
        if(0 == name.length()) {
            return "E/" + std::to_string(g_execNumbers++);
        } else return name;
    }

    Executor::Executor(const size_t  size,
                       const size_t blockSize,
                       const size_t runTimeMemory,
                       const size_t  runTimeMemoryDefaultBlockSize,
                       URI cache_path,
                       const std::string& name) : _allocator(size, blockSize),
                                                  _runTimeMemory(runTimeMemory),
                                                  _runTimeMemoryDefaultBlockSize(runTimeMemoryDefaultBlockSize),
                                                  _uuid(getUniqueID()),
                                                  _cache_path(cache_path),
                                                  _name(makeExecutorName(name)),
                                                  _historyServer(nullptr),
                                                  _threadNumber(0) {

        _threadID = std::this_thread::get_id();
        _workQueue = nullptr;
        _done = true; // per default, if worker(...) is executed it should not run through.

        // @TODO: what about non-existing S3 path? to make more user-friendly fix this!
        if(cache_path.isLocal() && !cache_path.exists()) {
            info("provided cache path " + cache_path.toString() + " does not exist. Attempting to create it.");
            auto vfs = VirtualFileSystem::fromURI(cache_path);
            if(VirtualFileSystemStatus::VFS_OK != vfs.create_dir(cache_path)) {
                std::stringstream ss;
                ss<<"Could not create cache path "<<cache_path.toString()<<". FATAL ERROR.";
                error(ss.str());
                throw std::runtime_error(ss.str());
            } else {
                info("created cache directory " + cache_path.toString());
            }
        }
    }

    Partition* Executor::allocWritablePartition(const size_t minRequired, const Schema& schema, const int dataSetID) {

        // memory is a valid pointer!
        // --> add to list as recently used
        // this is modifying operation, so lock globally
        //lockListMutex();

        std::unique_lock<boost::shared_mutex> lock(_listMutex);

        // fatal error?
        if(minRequired > maxMemory()) {
            error("Executor required " + sizeToMemString(minRequired) + " but maximum available memory is " + sizeToMemString(maxMemory()));
            return nullptr;
        }

        uint8_t* memory = nullptr;
        // try to get memory, as long as it fails evict partitions
        while(!(memory = reinterpret_cast<uint8_t*>(_allocator.alloc(minRequired)))) {
            evictLRUPartition();
        }

        Partition *p = new Partition(this, memory, _allocator.allocatedSize(memory), schema, dataSetID);

        // print out info:
        //info("new partition at addr: " + hexAddr(p) + " uuid: " + uuidToString(p->uuid()));

        _partitions.push_front(p);
        return p;
    }

    void Executor::makeRecentlyUsed(tuplex::Partition *partition) {

        //lockListMutex();

        std::this_thread::yield();
        std::unique_lock<boost::shared_mutex> lock(_listMutex);

        auto num_before = _partitions.size();

        // Note:
        // DO NOT USE iter_swap here! I spend 2 days debugging for it.
        // it basically is flawed since when using 2 or more threads
        // it corrupts the internal state of the vector.
        //  // this here may be a bad idea!!!
        //  auto from = _normalCasePartitions.begin();
        //  auto to = std::find(_normalCasePartitions.begin(), _normalCasePartitions.end(), partition);
        //  std::iter_swap(from, to);

        // make sure element is within list.
        assert(!_partitions.empty());

        // make sure partition is already recovered
        assert(partition->_arena);

        // this here is very important!
        // storedPartitions are not checked. i.e. the partition needs to be already recovered!
        assert(_partitions.end() != std::find(_partitions.begin(), _partitions.end(), partition));

        // Basic swap (for vector this could be done more efficient)
        _partitions.remove(partition);
        _partitions.push_front(partition);

        auto num_after = _partitions.size();
        assert(num_before == num_after);

        //unlockListMutex();
    }

    void Executor::freePartition(tuplex::Partition *partition) {

        assert(partition);

        // partitions is not needed anymore. I.e. remove from either stored or non-stored partitions
        // before all, acquire list lock because we are going to modify lists!
        //lockListMutex();
        {
            std::unique_lock<boost::shared_mutex> lock(_listMutex);
            if(std::find(_partitions.begin(), _partitions.end(), partition) != _partitions.end()) {
                // free the memory from this partition
                partition->free(_allocator);

                // remove from list
                _partitions.remove(partition);
            } else if(std::find(_storedPartitions.begin(), _storedPartitions.end(), partition) != _storedPartitions.end()) {

                // remove from list
                _storedPartitions.remove(partition);
            } else {

                error("INTERNAL ERROR: Could not find partition " + uuidToString(partition->uuid())
                               + " belonging to operator " + std::to_string(partition->getDataSetID()) + " and type " + partition->schema().getRowType().desc() + "");
                std::abort();
                exit(1);
            }
        }

        //unlockListMutex();

        // free memory
        partition->free(_allocator);
        delete partition;
        partition = nullptr;
    }

    size_t Executor::usedMemory() {

        size_t totalUsed = 0;
        {
            boost::shared_lock<boost::shared_mutex> lock(_listMutex);
            for(auto& p : _partitions)
                totalUsed += p->size();
        }
        return totalUsed;
    }


    void Executor::recoverPartition(tuplex::Partition *partition) {

        // must be in stored, must not be in _normalCasePartitions
        std::unique_lock<boost::shared_mutex> lock(_listMutex);

        // make sure partition is locked
        assert(partition->isLocked());

        // make sure partition is not having a valid memory pointer
        assert(!partition->_arena);

        // very important to check in order to make sure that below loop won't end in an infinity loop
        assert(partition->size() > 0);
        assert(partition->owner());

        assert(std::find(_storedPartitions.begin(), _storedPartitions.end(), partition) != _storedPartitions.end());
        assert(std::find(_partitions.begin(), _partitions.end(), partition) == _partitions.end());

        auto partitionPath = getPartitionURI(partition);

        // get from bitmap allocator free memory region, if this fails --> need to throw out partitions until it succeeds
        uint8_t *memory = nullptr;
        while(!(memory = (uint8_t*)_allocator.alloc(partition->size())))
            evictLRUPartition();

        // change partition
        // assign memory first, then load from disk
        partition->swapIn(memory, partitionPath);

        // the partition is now filled again with valid data
        // --> add it first to the LRU list, remove it from the stored list

        // make sure partition is not contained
        assert(std::find(_partitions.begin(), _partitions.end(), partition) == _partitions.end());

        _partitions.push_front(partition);
        _storedPartitions.remove(partition);

        std::stringstream ss;
        ss <<"recovered partition "+ uuidToString(partition->uuid()) + " from " + partitionPath.toString();
        info(ss.str());
    }

    void Executor::evictLRUPartition() {

        // function used exclusively by allocWritablePartition & recoverPartition

        // function should be only executed IFF _listmutex is locked!

        if(_partitions.empty()) {
            error("there is no partition to evict, fatal error!");
            std::abort();
            return;
        }

        // save last list item to disk & remove from partitions (should be added to global remvoe list?)
        Partition* last = _partitions.back();
        assert(last->owner() == this);
        last->swapOut(_allocator, getPartitionURI(last));

        assert(_partitions.back() == last);

        // threads may now access this partitions internals.
        // However, restore is blocked still through the list lock
        _partitions.remove(last);
        assert(std::find(_partitions.begin(), _partitions.end(), last) == _partitions.end());

        // exclusive push
        // make sure last is not contained within stored partitions yet
        assert(std::find(_storedPartitions.begin(), _storedPartitions.end(), last) == _storedPartitions.end());
        _storedPartitions.push_back(last);

        std::stringstream ss;
        ss<<"evicted partition " + uuidToString(last->uuid()) + " to " + getPartitionURI(last).toString();
        info(ss.str());
    }

    void Executor::worker() {

        info("starting detached process queue");

        auto this_id = std::this_thread::get_id();
        _threadID = this_id; //! needs to come first!!!

        // init runtime memory
        runtime::setRunTimeMemory(_runTimeMemory, _runTimeMemoryDefaultBlockSize);
        info("initialized runtime memory (" + sizeToMemString(runtime::runTimeMemorySize()) + ")" );
        // flush out
        Logger::instance().flushAll();

        bool done = _done.load(std::memory_order_acquire);
        while(!done) {
            done = _done.load(std::memory_order_acquire);

            // check
            WorkQueue *queue = nullptr;

            if((queue = _workQueue.load(std::memory_order_acquire))) {

                // work in non-blocking way 0 or 1 task away
                bool taskDone = queue->workTask(*this);

#ifndef NDEBUG
                // TODO: find out why so much runtime memory is used...used
                // i.e. print it out here or better, add to stats
#endif

                // acquire atomic reference and check if not null,
                // if not null then update!
                // only update when task is done, to avoid expensive calls.
                HistoryServerConnector* hs = historyServer();
                if(taskDone && hs) {
                    hs->sendStatus(JobStatus::RUNNING,
                                   queue->numPendingTasks(),
                                   queue->numCompletedTasks());
                }
            }
        }

        // release here runtime memory...
        runtime::releaseRunTimeMemory();

        // can't use logger here anymore because of python3 issues
        // logger().info("stopped detached work queue");
        // // flush out
        // Logger::instance().flushAll();
    }

    void Executor::release() {

        //@Todo: improve logging system for python3 interop

        using namespace std;

        // stops detached queue.
        _done = true;

        if(_thread.joinable())
            _thread.join();

        //@Todo: release memory allocated for ManagedPartitions
        //lockListMutex();
        {
            std::unique_lock<boost::shared_mutex> lock(_listMutex);
            if(!_partitions.empty()) {
               cout<<"[GLOBAL] releasing " + std::to_string(_partitions.size()) + " active partitions"<<endl;
                for(auto& p : _partitions) {
                    if(p)
                        delete p;
                    p = nullptr;
                }

                _partitions.clear();
            }

            if(!_storedPartitions.empty()) {
                cout<<"[GLOBAL] releasing " + std::to_string(_storedPartitions.size()) + " stored partitions"<<endl;
                for(auto& p : _storedPartitions) {
                    if(p)
                        delete p;
                    p = nullptr;
                }

                _storedPartitions.clear();
            }
        }

        //unlockListMutex();
    }

    URI Executor::getPartitionURI(Partition* partition) const {
        return URI(_cache_path.toPath() + "/" + uuidToString(partition->uuid()) + ".prt");
    }

    void Executor::processQueue(bool detached) {

        // if already running, return
        if(isRunning())
            return;

        // make sure it is not running yet
        assert(!isRunning());

        // reset signal variable for thread to end
        _done = false;

        // process all tasks in the queue
        if(!detached) {
//            while(_numPendingTasks.load(std::memory_order_acquire) != 0) {
//                std::unique_ptr<IExecutorTask> task(nullptr);
//
//                // dequeue from general working queue
//                if(_workQueue.try_dequeue(task)) {
//                    // process task
//                    task->execute();
//                    // save which thread executed this task
//                    task->setID(_threadID);
//
//                    _numPendingTasks.fetch_add(-1, std::memory_order_release);
//
//                    // add task to done list
//                    _completedTasksMutex.lock();
//                    _completedTasks.push_back(std::move(task));
//                    _completedTasksMutex.unlock();
//                }
//            }

            EXCEPTION("nyimpl");

        } else {
            // create threads
            try {
                _thread = std::thread(&Executor::worker, this);
            } catch(...) {

                // @Todo...
                // get rid of memory...

                throw;
            }
        }
    }


    void Executor::setHistoryServer(HistoryServerConnector *hs) {
        // make sure not attached to queue...
        assert(!_workQueue);

        // update pointer
        _historyServer.store(hs, std::memory_order::memory_order_release);

        // make sure nothing bad happened in the meantime...
        assert(!_workQueue);
    }
}