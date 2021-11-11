//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LOGGER_H
#define TUPLEX_LOGGER_H

#include <map>
#include <MessageHandler.h>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/formatter.h>
#include <spdlog/details/null_mutex.h>

class Logger;
class MessageHandler;

template<typename Mutex> class python_sink : public spdlog::sinks::base_sink <Mutex> {
public:
    virtual void flushToPython(bool acquireGIL = false) = 0;
};

/*!
 * singleton class that handles logging (one per node...)
 * per default logs are printed to console and stored in files.
 * For multijob/multiuser this is not anymore sufficient. needs to be overhauled such that each context is associated.
 * Right now, logdir is not used...
 */

class Logger {
    friend class MessageHandler;
private:
    Logger();

    std::mutex _mutex;
    std::vector<spdlog::sink_ptr> _sinks;
    std::map<std::string, MessageHandler> _handlers;

    void warn(const std::string& name, const std::string& message);
    void error(const std::string& name, const std::string& message);
    void info(const std::string& name, const std::string& message);
    void debug(const std::string& name, const std::string& message);

    // to avoid deadlocks with spdlog, use lazy initialization
    bool _initialized;

    void initDefault();
public:

    static Logger& instance() {
        static Logger theoneandonly;
        return theoneandonly;
    }

    MessageHandler& logger(const std::string& name);


    MessageHandler& defaultLogger() { return logger("global"); }


    /*!
     * flushes all loggers.
     */
    void flushAll();

    /*!
     * flush specific python logger...
     * @param acquireGIL
     */
    void flushToPython(bool acquireGIL=false);

    // add here later functions to filter out certain messages etc.
    static void init(const std::vector<spdlog::sink_ptr >& sinks={std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>()});

    /*!
     * reset all internal + spdlog structures, i.e. init can be called afterwards.
     */
    void reset() {
        std::unique_lock<std::mutex> lock(_mutex);

        // remove all sinks
        spdlog::shutdown();

        _handlers.clear();
        _sinks.clear();
        _initialized = false;
    }
};

#endif //TUPLEX_LOGGER_H