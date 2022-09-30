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

#include <pythread.h>

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
#ifndef LINUX
        sscanf(thread_id.c_str(), "%lld", &id);
#else
        sscanf(thread_id.c_str(), "%ld", &id);
#endif
        return id;
    }

    // GIL management here
    static std::atomic_bool gil(false); // true if a thread holds the gil, false else
    static std::mutex gilMutex; // access to all the properties below
    PyGILState_STATE gstate; // for non-main thread lock

    // cf. https://pythonextensionpatterns.readthedocs.io/en/latest/thread_safety.html#f1
    static PyThread_type_lock gil_lock(nullptr);

    static void acquire_lock() {
        // lazy init lock -> called on first entry.
        if(!gil_lock) {
            gil_lock = PyThread_allocate_lock();
            if(!gil_lock) {
                std::cerr<<"failed to initialize lock"<<std::endl;
            }
        }

        if (! PyThread_acquire_lock(gil_lock, NOWAIT_LOCK)) {
            {
                PyThreadState *_save;
                _save = PyEval_SaveThread();
                PyThread_acquire_lock(gil_lock, WAIT_LOCK);
                PyEval_RestoreThread(_save);
            }
        }
    }

    static void release_lock() {
        PyThread_release_lock(gil_lock);
    }

    // Note: thread::id can't be atomic yet, this is an ongoing proposal
    // ==> convert to uint64_t and use this for thread safe access
    static std::atomic_int64_t gilID(-1); // id of thread who holds gil
    static std::atomic_int64_t interpreterID(-1); // thread which holds the interpreter
    static std::atomic_bool interpreterInitialized(false); // checks whether interpreter is initialized or not
    std::thread::id gil_main_thread_id;

    // vars for python management
    static std::atomic<PyThreadState*> gilState(nullptr);

    void lockGIL() {
        gilMutex.lock(); // <-- acquire the managing lock. No other thread can lock the gil! => what if another thread tries to unlock? -> security concern...

        // what is the current thread id? is it the main thread? => then lock the gil via restore thread etc.
        // if not, need to use GILState_Ensure
        if(std::this_thread::get_id() == gil_main_thread_id) {
            if(!gilState)
                gilState = PyGILState_GetThisThreadState();
            assert(gilState);
            PyEval_RestoreThread(gilState); // acquires GIL!
        } else {
            assert(interpreterInitialized);
            gstate = PyGILState_Ensure();
        }
        assert(PyGILState_Check());
        gil = true;
        gilState = nullptr;
        gilID = thisThreadID();
    }

    void unlockGIL() {
        // is it the main thread? and does it hold the manipulation lock?
        if(std::this_thread::get_id() == gil_main_thread_id) {
            gilState = PyEval_SaveThread();
        } else {
            assert(interpreterInitialized);
            PyGILState_Release(gstate);
            gstate = PyGILState_UNLOCKED;
        }
        gil = false;
        gilID = thisThreadID();
        gilMutex.unlock();
    }

    bool holdsGIL() {
        return gil;
    }

    void acquireGIL() {
//        gilMutex.lock();
//        // PyEval_AcquireLock();
//        PyEval_AcquireThread(gilState); // acquires GIL!
//        gil = true;
//        gilID = thisThreadID();
        std::cerr<<"acquire GIL is deprecated"<<std::endl;
    }

    void initInterpreter() {
        gil_main_thread_id = std::this_thread::get_id();

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
            //gilMutex.lock();
            gil_lock = nullptr; // this is the start, we're in the interpreter...

            gil = true;
            gilID = interpreterID = thisThreadID();
        } else {

            // make sure this thread rn holds the GIL!
            if(!PyGILState_Check())
                throw std::runtime_error("when initializing the thread, initInterpreter MUST hold the GIL");

            // assume we are calling from python process/shared object
            //gilMutex.lock();

            gil_lock = nullptr; // this is the start, we're in the interpreter...
            // acquire and release to initialize, works b.c. single-threaded interpreter...
            acquire_lock();
            release_lock();

            gil = true;
            gilID = interpreterID = thisThreadID();
        }
        gilMutex.lock();
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
//        if(gil)
//            gilMutex.unlock();
        if (gil_lock) {
            PyThread_free_lock(gil_lock);
            gil_lock = NULL;
        }
        gilMutex.unlock();
    }
}