//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SIGNALS_H
#define TUPLEX_SIGNALS_H

#include <Python.h>
#include <cerrno>
#include <csignal>

// helper functions to use signal handling together with CPython signals...
namespace tuplex {

    /*!
     * install custom handlers to forward to python later
     * @return true if install succeeded
     */
    extern bool install_signal_handlers();

    /*!
     * returns true if SIGINT was received, can be used to leave control flow early.
     * @return true if SIGINT was received
     */
    extern bool check_interrupted();

    /*!
     * reset signal indicators
     */
    extern void reset_signals();

    /*!
     * true if any signal was received, forwards everything to cpython then. Should be called from main thread only.
     * @param within_GIL if true, no GIL acquiring calls are made.
     * @return true if any signal was received.
     */
    extern bool check_and_forward_signals(bool within_GIL=false);
}

#endif //TUPLEX_SIGNALS_H