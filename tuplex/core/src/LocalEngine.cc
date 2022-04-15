//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 9/5/18                                                                   //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <memory>

//
// Created by Leonhard Spiegelberg on 9/5/18.
//

#include <LocalEngine.h>
#include <Signals.h>

namespace tuplex {
    void LocalEngine::release() {
        // release all
        for(auto& exec : _executors) {
            // this here is very stupid:
            // basically, the issue is that when exiting the program
            // python doesn't allow anymore PySys_StdWriteout to be called...
            // ==> i.e. for this resort to std::cout...
            std::cout<<"[GLOBAL] releasing executor: "<<exec->name()<<std::endl;
            exec->release();
            std::stringstream ss;
            ss<<"[GLOBAL] local executor "<<exec->name()<<" terminated.";
            std::cout<<ss.str()<<std::endl;
        }
    }

    std::vector<Executor*> LocalEngine::getExecutors(const size_t num,
            const size_t size,
            const size_t blockSize,
             const size_t runTimeMemory,
             const size_t runTimeMemoryDefaultBlockSize,
            const URI& cache_path) {

        if(0 == num) // no execs
            return std::vector<Executor*>();

        std::vector<Executor*> execs; // the array to return

        // check whether existing executors (threads) may be reused
        std::vector<Executor*> candidates;
        for(auto& e : _executors)
            candidates.push_back(e.get());

        for(int i = 0; i < num; ++i) {
            // best match, exact match
            Executor* bestMatch = nullptr;
            Executor* exactMatch = nullptr;

            for(auto& e : candidates) {
                // exact match?
                if(e->memorySize() == size && e->blockSize() == blockSize && e->runTimeMemorySize() == runTimeMemory && e->runTimeMemoryDefaultBlockSize() == runTimeMemoryDefaultBlockSize)
                    exactMatch = e;
                // best match?
                if(e->memorySize() >= size && e->blockSize() >= blockSize && e->runTimeMemorySize() >= runTimeMemory && e->runTimeMemoryDefaultBlockSize() >= runTimeMemoryDefaultBlockSize)
                    bestMatch = e;
            }

            // match found?
            if(exactMatch) {
                execs.push_back(exactMatch);
                _refCounts[exactMatch]++;
            } else {
                if(bestMatch) {
                    execs.push_back(bestMatch);
                    _refCounts[bestMatch]++;
                }
            }
        }

        // above is for reusing execs. Now, create difference of newly needed execs
        auto numStillNeeded = num - execs.size();
        auto num_current = _executors.size();
        auto& logger = Logger::instance().logger("local execution engine");
        for(int i = 0; i < numStillNeeded; ++i) {
            URI uri = URI(cache_path.toString() + "/" + "E" + std::to_string(num_current + 1 + i));
            _executors.push_back(std::make_unique<Executor>(size, blockSize, runTimeMemory, runTimeMemoryDefaultBlockSize, uri, "E/" + std::to_string(num_current + 1 + i)));
            auto exec = _executors.back().get();
            _refCounts[exec] = 1;
            execs.push_back(exec);
        }

        // to avoid race conflicts, start executors AFTER putting them into the array
        unsigned threadNum = 1; // driver has thread-number 0
        for(auto exec : execs) {
            // set thread number, inc
            exec->setThreadNumber(threadNum++);

            // start process queue on this executor
            exec->processQueue(true);

            std::stringstream ss;
            ss << "started local executor " << exec->name() << " (" << sizeToMemString(size) << ", "
               << sizeToMemString(blockSize) << " default partition size)";
            logger.info(ss.str());
        }

        // check if executors can be reused.
        assert(execs.size() == num);
        return execs;
    }

    std::shared_ptr<Executor>
    LocalEngine::getDriver(const size_t size, const size_t blockSize, const size_t runTimeMemory,
                           const size_t runTimeMemoryDefaultBlockSize, const tuplex::URI &cache_path) {
        ExecutorConfig new_cfg = ExecutorConfig{
                ._size = size,
                ._blockSize = blockSize,
                ._runTimeMemory = runTimeMemory,
                ._runTimeMemoryDefaultBlockSize = runTimeMemoryDefaultBlockSize,
                ._cache_path = cache_path
        };

        if (!_driver || _driver_cfg != new_cfg) {
            if (_driver) {
                Logger::instance().logger("local execution engine").info(
                        "driver already exist, starting new driver with updated config");
            }

            // lazy start driver
            URI uri = URI(cache_path.toString() + "/" + "driver");
            _driver = std::make_shared<Executor>(size, blockSize, runTimeMemory, runTimeMemoryDefaultBlockSize, uri,
                                                 "driver");
            _driver_cfg = new_cfg;

            // driver always has thread number 0!
            // Note: this could be a potential issue if the config change and the old driver is still running
            // due to external reference. Then there could be two executors with the same number
            _driver->setThreadNumber(0);

            std::stringstream ss;
            ss << "started driver (" << sizeToMemString(size) << ", " << sizeToMemString(blockSize)
               << " default partition size)";
            //  <<"overflow will be cached at "<<uri.toString();
            Logger::instance().logger("local execution engine").info(ss.str());
        }

        return _driver;
    }

    void LocalEngine::freeExecutors(const std::vector<tuplex::Executor *> & executors, const Context* ctx) {

        for(auto& e : executors) {
            // check that they are contained!
            assert(_refCounts.find(e) != _refCounts.end());

            // free partitions belonging to ctx
            if(e && ctx)
                e->freeAllPartitionsOfContext(ctx);

            // decrease ref count, if ref count is zero, then stop executor!
            _refCounts[e]--;

            if(0 == _refCounts[e]) {
                auto it = std::find_if(_executors.begin(), _executors.end(), [&](const std::unique_ptr<Executor>& p) {
                    return p.get() == e;
                });
                assert(it != _executors.end());

                // stop executor
                e->release();
                Logger::instance().logger("local execution engine").info("Stopped local executor " + e->name());

                _executors.erase(it);
                _refCounts.erase(e);
            }
        }
    }

    LocalEngine::LocalEngine() {
        // init signal handlers
        // => this is a global init. Should we do that?
        // @TODO: what about lambda backend? we should not use local engine there...!
        install_signal_handlers();
    }
}