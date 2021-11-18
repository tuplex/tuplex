//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/9/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_PYTHONCOMMON_H
#define TUPLEX_PYTHONCOMMON_H

#include <Python.h>
#include <boost/python.hpp>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/formatter.h>
#include <spdlog/details/null_mutex.h>
#include <mutex>

#include <PythonHelpers.h>

namespace tuplex {

    inline int spdlog_level_to_number(const spdlog::level::level_enum& lvl) {
        switch(lvl) {
            case spdlog::level::level_enum::trace:
                return 1;
            case spdlog::level::level_enum::debug:
                return 2;
            case spdlog::level::level_enum::info:
                return 3;
            case spdlog::level::level_enum::warn:
                return 4;
            case spdlog::level::level_enum::err:
                return 5;
            case spdlog::level::level_enum::critical:
                return 6;
            default:
                return 0;
        }
    }

    template<typename Mutex> class nogil_python3_sink : public python_sink<Mutex> {
    public:
        nogil_python3_sink() = delete;
        explicit nogil_python3_sink(PyObject* pyFunctor) : _pyFunctor(pyFunctor) {}

        void flushToPython(bool acquireGIL=false) override {

            if(!_pyFunctor) {
                std::cerr<<"no functor found, early abort"<<std::endl;
                return;
            }

            if(acquireGIL)
                python::lockGIL();
            {
                std::lock_guard<std::mutex> lock(_bufMutex);


                // sort messages after time
                std::sort(_messageBuffer.begin(), _messageBuffer.end(), [](const LogMessage& a, const LogMessage& b) {
                    return a.timestamp < b.timestamp;
                });

                // now call for each message the python function!
                // => basically give as arg the message... (later pass the other information as well...)
                for (const auto &msg: _messageBuffer) {

                    // callback gets 4 params:
                    // 1. severity level (integer)
                    // 2. time (iso8601 string)
                    // 3. logger (string)
                    // 4. message (string)

                    // perform callback in python...
                    auto args = PyTuple_New(4);
                    auto py_lvl = PyLong_FromLong(spdlog_level_to_number(msg.level));
                    auto py_time = python::PyString_FromString(chronoToISO8601(msg.timestamp).c_str());
                    auto py_logger = python::PyString_FromString(msg.logger.c_str());
                    auto py_msg = python::PyString_FromString(msg.message.c_str());
                    PyTuple_SET_ITEM(args, 0, py_lvl);
                    PyTuple_SET_ITEM(args, 1, py_time);
                    PyTuple_SET_ITEM(args, 2, py_logger);
                    PyTuple_SET_ITEM(args, 3, py_msg);

                    Py_XINCREF(_pyFunctor);
                    Py_XINCREF(args);
                    Py_XINCREF(py_lvl);
                    Py_XINCREF(py_logger);
                    Py_XINCREF(py_msg);

                    PyObject_Call(_pyFunctor, args, nullptr);
                    if(PyErr_Occurred()) {
                        PyErr_Print();
                        std::cout<<std::endl;
                        PyErr_Clear();
                    }
                }

                _messageBuffer.clear();
            }

            if(acquireGIL)
                python::unlockGIL();
        }
    protected:
        virtual void sink_it_(const spdlog::details::log_msg& spdlog_msg) override {
            // invoke mutex
            std::lock_guard<std::mutex> lock(_bufMutex);

            // need to read&create copy of spdlog msg because at some point memory gets invalidated for the stringviews...
            LogMessage msg;
            msg.message = std::string(spdlog_msg.payload.data());
            msg.timestamp = spdlog_msg.time;
            msg.logger = *spdlog_msg.logger_name;
            msg.level = spdlog_msg.level;
            _messageBuffer.push_back(msg);
        }

        virtual void flush_() override {
           // don't do anything here... --> instead call the flushAll at strategoc places where the GIL state is known!
        }
    private:

        struct LogMessage {
            std::string message;
            std::chrono::time_point<std::chrono::system_clock> timestamp;
            std::string logger;
            spdlog::level::level_enum level;
        };

        std::vector<LogMessage> _messageBuffer;
        PyObject* _pyFunctor;
        std::mutex _bufMutex;
    };

    using no_gil_python3_sink_mt = nogil_python3_sink<std::mutex>;
    using no_gil_python3_sink_st = nogil_python3_sink<spdlog::details::null_mutex>;

    extern boost::python::object registerPythonLoggingCallback(boost::python::object callback_functor);
}
#endif //TUPLEX_PYTHONCOMMON_H
