//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Signals.h>
#include <thread>
#include <atomic>
#include <Logger.h>
#include <PythonHelpers.h>

namespace tuplex {

    volatile sig_atomic_t do_shutdown = 0;
    std::atomic<bool> shutdown_requested(false);
    std::atomic_int sig_received(-1);
    // C++ 17
    //static_assert(std::atomic<bool>::is_always_lock_free);

    void tplx_signal_handler(int signum) {
        do_shutdown = 1;
        sig_received = signum;
        shutdown_requested = true;
#ifndef NDEBUG
        const char str[] = "\n => received signal SIGINT in tplx_signal_handler, aborting.\n";
        write(STDERR_FILENO, str, sizeof(str) - 1); // write is signal safe, the others not.
#endif
    }

    bool install_signal_handlers() {

        // cf. https://www.man7.org/linux/man-pages/man2/sigaction.2.html
        // reset vars
        do_shutdown = 0;
        shutdown_requested = false;

        struct sigaction action;
        action.sa_handler = tplx_signal_handler;
        sigemptyset(&action.sa_mask);

        // for now only install on sigint, this effectively disables
        // all other python handlers. That's ok though...

        if(0 == sigaction(SIGINT, &action, NULL))
            return true;
        else {
            // errno has description
            Logger::instance().defaultLogger().error("Failed to install custom signal handlers, details: " +
            std::string(strerror(errno)));
            return false;
        }
    }

    bool check_interrupted() {
        return do_shutdown && shutdown_requested.load();
    }

    bool check_and_forward_signals(bool within_GIL) {
        if(check_interrupted()) {
            // @TODO: could improve this but artifically tripping signals
            // to python! cf. https://github.com/python/cpython/blob/fb5db7ec58624cab0797b4050735be865d380823/Modules/signalmodule.c#L1787

            if(!within_GIL)
                python::lockGIL();

            // Note: this will produce in the default handler a KeyboardInterrupt exception
            //       based on a SystemError.
            //       ==> to manipulate this, do not trip the signal but instead get signal handler + raise manually
            //           exception. However, the solution here seems more natural and more compatible.
            //
            // trip SIGINT to cpython interpreter
            PyErr_SetInterrupt();

            // call python handlers & Co.
            int rc = PyErr_CheckSignals();

#ifndef NDEBUG
            std::cout<<"Python signal forwarding: Result of PyErr_CheckSignals: "<<rc<<std::endl;
#endif
            if(!within_GIL)
                python::unlockGIL();

            // reset signals, else stuck e.g. in interactive shell.
            reset_signals();
            return true;
        }
        return false;
    }

    void reset_signals() {
        do_shutdown = 0;
        shutdown_requested = false;
    }
}