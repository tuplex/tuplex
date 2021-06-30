//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LOCALENGINE_H
#define TUPLEX_LOCALENGINE_H

#include <Executor.h>
#include <vector>
#include <TSingleton.h>
#include "RESTInterface.h"

namespace tuplex {
    /*!
     * local execution engine. Provides local executors for a context
     * THIS IS NOT THREADSAFE. Should be only accessed by driver thread.
     */
    class LocalEngine {

    private:
        // non-detached executor that serves as the driver
        std::unique_ptr<Executor> _driver;

        std::vector<std::unique_ptr<Executor>> _executors;
        std::map<Executor*, size_t> _refCounts; //! reference counts for each executor

        LocalEngine(const LocalEngine&);
        void operator = (const LocalEngine&);

        // The local task queue
        WorkQueue  _queue;

    protected:
        LocalEngine();

    public:

        ~LocalEngine() {

            // Note that current version of the WorkQueue has not a thread safe destructor.
            // Hence, need to destroy all threads before calling destructor on WorkQueue
            release();

            std::cout<<"[GLOBAL] Local engine terminated."<<std::endl;
        }

        static LocalEngine& instance() {
            static LocalEngine theoneandonly;
            return theoneandonly;
        }

        /*!
         * retrieves a number of executors. Lazily starts them if not available.
         * @param num number of executors requested
         * @param size size in bytes that each executor should have
         * @param blockSize size of individual blocks used (can be used for coarse or fine grained parallelism)
         * @param cache_path directory where subfolders will be created for all executors to be started
         * @return array of executor references
         */
        std::vector<Executor*> getExecutors(const size_t num,
                                            const size_t size,
                                            const size_t blockSize,
                                            const size_t runTimeMemory,
                                            const size_t runTimeMemoryDefaultBlockSize,
                const URI& cache_path);

        /*!
         * releases executors (invoked by context)
         */
        void freeExecutors(const std::vector<Executor*>&);

        Executor* getDriver(const size_t size,
                            const size_t blockSize,
                            const size_t runTimeMemory,
                            const size_t runTimeMemoryDefaultBlockSize,
                            const URI& cache_path);

        void release();

        /*!
         * retrieves the global work queue for local executors
         * @return
         */
        WorkQueue& getQueue() { return _queue; }
    };
}
#endif //TUPLEX_LOCALENGINE_H