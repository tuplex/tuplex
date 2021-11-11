//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/9/2021                                                                 //
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

    template<typename Mutex> class nogil_python3_sink : public python_sink<Mutex> {
    public:
        nogil_python3_sink() : _pyFunctor(nullptr) {}
        nogil_python3_sink(PyObject* pyFunctor) : _pyFunctor(nullptr) {}

        void flushToPython(bool acquireGIL=false) override {

            if(!_pyFunctor)
                return;

            assert(_pyFunctor->ob_refcnt > 0);

            if(acquireGIL)
                python::lockGIL();
            try {
                std::lock_guard<Mutex> lock(this->mutex_);


//                // sort messages after time
//                std::sort(_messageBuffer.begin(), _messageBuffer.end(), [](const spdlog::details::log_msg& a, const spdlog::details::log_msg& b) {
//                    return a.time < b.time;
//                });

                // now call for each message the python function!
                // => basically give as arg the message... (later pass the other information as well...)
                for(auto msg : _messageBuffer) {
                    auto args = PyTuple_New(1);
                    auto py_msg = python::PyString_FromString(std::string(msg.payload.data()).c_str());
                    PyTuple_SET_ITEM(args, 0, py_msg);

                    PyObject_Call(_pyFunctor, args, nullptr);
                    if(PyErr_Occurred()) {
                        PyErr_Print();
                        std::cout<<std::endl;
                        PyErr_Clear();
                    }
                }

                _messageBuffer.clear();
            } catch(...) {
                fprintf(stderr, "failed to communicate message buffer from python sink");
            }
            if(acquireGIL)
                python::unlockGIL();
        }
    protected:
        virtual void sink_it_(const spdlog::details::log_msg& msg) override {
//            fmt::memory_buffer formatted;
//            this->formatter_->format(msg, formatted);
//            std::string formatted_msg = fmt::to_string(formatted);



//            // make sure GIL is not hold when this function is triggered!
//            assert(!python::holdsGIL());
//
//            // logging should NEVER be called when python::lockGIL() has been done!
//            python::lockGIL();
//            PySys_FormatStdout("%s", formatted_msg.c_str());
//            python::unlockGIL();
            // invoke mutex
            std::lock_guard<Mutex> lock(this->mutex_);
            _messageBuffer.push_back(msg);
        }

        virtual void flush_() override {
           // don't do anything here...
        }
    private:
        std::vector<spdlog::details::log_msg> _messageBuffer;
        PyObject* _pyFunctor;
    };

    using no_gil_python3_sink_mt = nogil_python3_sink<std::mutex>;
    using no_gil_python3_sink_st = nogil_python3_sink<spdlog::details::null_mutex>;

    inline boost::python::object registerPythonLogger(boost::python::object log_functor) {
        // get object
        auto functor_obj = log_functor.ptr();
        Py_XINCREF(functor_obj);
        // make sure it's callable etc.
        if(!PyCallable_Check(functor_obj))
            throw std::runtime_error(python::PyString_AsString(functor_obj) + " is not callable. Can't register as logger.");


        // add new sink to loggers with this function
        python::unlockGIL();
        try {
            Logger::instance().init({std::make_shared<no_gil_python3_sink_mt>(functor_obj)});

//            Logger::instance().init(); ??
        } catch(std::exception& e) {
            // use C printing for the exception here
            std::cerr<<"while registering python logger, following error occurred: "<<e.what()<<std::endl;
        }
        python::lockGIL();

        // TODO: make sure Logger is never called while thread holds GIL!


        // return None
        return boost::python::object();
    }
}
#endif //TUPLEX_PYTHONCOMMON_H
