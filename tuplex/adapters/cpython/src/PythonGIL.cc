//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonHelpers.h>
#include <thread>
#include <iostream>
#include <Base.h>
#include <mutex>

namespace python {

    // GIL details:
    // ==> thread management is actually a mess in Python
    // There is PyEval_SaveThread AND PyGILState_Ensure

    // hack: because thread::id can't be atomic yet -.-
    inline int64_t thisThreadID() {
        std::stringstream ss;
        ss<<std::this_thread::get_id();
        ss.flush();
        auto thread_id = ss.str();
        int64_t id = -1;
        sscanf(thread_id.c_str(), "%lld", &id);
        return id;
    }

    // GIL management here
    static std::atomic_bool gil(false); // true if a thread holds the gil, false else
    static std::mutex gilMutex; // access to all the properties below


    // Note: thread::id can't be atomic yet, this is an ongoing proposal
    // ==> convert to uint64_t and use this for thread safe access
    static std::atomic_int64_t gilID(-1); // id of thread who holds gil
    static std::atomic_int64_t interpreterID(-1); // thread which holds the interpreter
    static std::atomic_bool interpreterInitialized(false); // checks whether interpreter is initialized or not

    // vars for python management
    static std::atomic<PyThreadState*> gilState(nullptr);

    void lockGIL() {
        gilMutex.lock();
        assert(gilState);
        PyEval_RestoreThread(gilState); // acquires GIL!
        gil = true;
        gilID = thisThreadID();
    }

    void unlockGIL() {
        gilMutex.unlock();
        gil = false;
        gilState = PyEval_SaveThread();
        gilID = thisThreadID();
    }

    bool holdsGIL() {
        return gil;
    }

    void acquireGIL() {
        gilMutex.lock();
        PyEval_AcquireLock();
        PyEval_AcquireThread(gilState); // acquires GIL!
        gil = true;
        gilID = thisThreadID();
    }

    void initInterpreter() {
        if(interpreterInitialized)
            throw std::runtime_error("interpreter was already initialized, abort");

        // check if this function is called within a python interpreter or not
        if(!Py_IsInitialized()) {

            Py_InitializeEx(0); // 0 to skip initialization of signal handlers, 1 would register them.

#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION < 7)
            // init threads (not necessary from Python 3.7 onwards)
            PyEval_InitThreads();
            assert(PyEval_ThreadsInitialized());
#endif
            // assume we are calling from python process/shared object
            gilMutex.lock();
            gil = true;
            gilID = interpreterID = thisThreadID();
        } else {

            // make sure this thread rn holds the GIL!
            if(!PyGILState_Check())
                throw std::runtime_error("when initializing the thread, initInterpreter MUST hold the GIL");

            // assume we are calling from python process/shared object
            gilMutex.lock();
            gil = true;
            gilID = interpreterID = thisThreadID();
        }

        interpreterInitialized = true;
    }

    void closeInterpreter() {

        if(!PyGILState_Check() || !holdsGIL())
            throw std::runtime_error("to shutdown interpreter, GIL must be hold the calling thread...");

        if(PyErr_Occurred()) {
            PyErr_Print();
            PyErr_Clear();
        }

        if(PyErr_CheckSignals() < 0) {
            PyErr_Print();
            PyErr_Clear();
        }
        Py_FinalizeEx();

        interpreterInitialized = false;

        // unlock
        if(gil)
            gilMutex.unlock();
    }
}