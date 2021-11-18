//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <memory>
#include <Logger.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <sstream>

Logger::Logger() : _initialized(false) {
}

void Logger::initDefault() {
    if(!_initialized) {

        try {
            // add later here also an stderr sink...
            _sinks.push_back(std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>());
#ifndef NDEBUGe
            // disable slow log in release mode
            _sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>("log.txt"));
#endif
            _initialized = true;

            // create default logger
        }
        catch(const spdlog::spdlog_ex& ex) {
            std::cout<<"[FATAL] Initialization of logging system failed: "<<ex.what()<<std::endl;
            exit(1);
        }
    }
}

void Logger::init(const std::vector<spdlog::sink_ptr> &sinks) {
    Logger& log = Logger::instance();

    try {
        log.reset();
        log._sinks = sinks;
        log._initialized = true;
    }
    catch(const spdlog::spdlog_ex& ex) {
        std::cerr<<"[FATAL] Initialization of logging system failed: "<<ex.what()<<std::endl;
        exit(1);
    }
}

MessageHandler& Logger::logger(const std::string &name) {

    std::unique_lock<std::mutex> lock(_mutex);

    // setup sinks if required
    initDefault();

    // check if a message handler under this name is already registered
    // if not create, else return reference
    auto it = _handlers.find(name);
    if(it != _handlers.end())
        return it->second;
    else {
        _handlers[name] = MessageHandler().setName(name);

        // create the logger and register it
        auto spdlogger = std::make_shared<spdlog::logger>(name, _sinks.begin(), _sinks.end());
#ifndef NDEBUG
        spdlogger->set_level(spdlog::level::debug);
#endif
        spdlog::register_logger(spdlogger);

        return _handlers[name];
    }
}

void Logger::error(const std::string &name, const std::string &message) {
    auto log = spdlog::get(name);
    if(log)
        log.get()->error(message);
}

void Logger::debug(const std::string &name, const std::string &message) {
#ifndef NDEBUG
    auto log = spdlog::get(name);
    if(log)
        log.get()->debug(message);
#endif
}

void Logger::warn(const std::string &name, const std::string &message) {
    auto log = spdlog::get(name);
    if(log)
       log.get()->warn(message);
}

void Logger::info(const std::string &name, const std::string &message) {
    auto log = spdlog::get(name);
    if(log)
        log.get()->info(message);
}

void Logger::flushAll() {
    // iterate through all loggers & flush them
    for(auto it : this->_handlers) {
        auto name = it.first;
        auto log = spdlog::get(name);
        // log may be nullptr. Hence, only flush if valid.
        if(log)
            log.get()->flush();
    }
}

void Logger::flushToPython(bool acquireGIL) {

    // flush other sinks
    flushAll();

    // check for each sink whether it's a python sink, then call method
    for(auto& sink : _sinks) {
        auto py_sink = std::dynamic_pointer_cast<python_sink<std::mutex>>(sink);
        if(py_sink) {
            py_sink->flushToPython(acquireGIL);
        }
    }
}
