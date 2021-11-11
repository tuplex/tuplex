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

    // cf. e.g. https://gist.github.com/hensing/0db3f8e3a99590006368 ?
    enum logtypes {info, warning, error, debug};
    extern void log_msg_to_python_logging(int type, const char *msg);

    template<typename Mutex> class nogil_python3_sink : public python_sink<Mutex> {
    public:
        //nogil_python3_sink() : _pyFunctor(nullptr) {}
        nogil_python3_sink() = delete;
        explicit nogil_python3_sink(PyObject* pyFunctor) : _pyFunctor(pyFunctor) {}

        void flushToPython(bool acquireGIL=false) override {

            printf("calling flush to python in nogil_python3_sink\n");
            std::cout<<std::endl;
            printf("acquireGIL: %d\n", acquireGIL);
            std::cout<<std::endl;
            printf("pyFunctor is: %llX\n", reinterpret_cast<uint64_t>(_pyFunctor));

            if(!_pyFunctor) {
                std::cout<<"no functor found, early abort"<<std::endl;
                return;
            }

//            assert(_pyFunctor->ob_refcnt > 0);

            if(acquireGIL)
                python::lockGIL();
//            try {
                printf("acquiring bufmutex...\n");
            {
                std::lock_guard<std::mutex> lock(_bufMutex);


//                // sort messages after time
//                std::sort(_messageBuffer.begin(), _messageBuffer.end(), [](const spdlog::details::log_msg& a, const spdlog::details::log_msg& b) {
//                    return a.time < b.time;
//                });

                printf("bufmutex acquired, found % msg...", _messageBuffer.size());

                // now call for each message the python function!
                // => basically give as arg the message... (later pass the other information as well...)
                for (const auto &msg: _messageBuffer) {
//                    auto args = PyTuple_New(1);
//                    auto py_msg = python::PyString_FromString(std::string(msg.payload.data()).c_str());
//                    PyTuple_SET_ITEM(args, 0, py_msg);
//
//                    PyObject_Call(_pyFunctor, args, nullptr);
//                    if(PyErr_Occurred()) {
//                        PyErr_Print();
//                        std::cout<<std::endl;
//                        PyErr_Clear();
//                    }
                    std::cout<<"first message acquired..."<<std::endl;
                    printf("get first message...\n");

                    //std::string message(msg.payload.data());
                    std::string message = "test message";

//                    // get null-terminated C-string from string_view
//                    char *temp_str = new char[msg.payload.size() + 1];
//                    memset(temp_str, 0, msg.payload.size() + 1);
//                    memcpy(temp_str, msg.payload.data(), msg.payload.size());
//                    printf("message is: %s", temp_str);
//                    delete [] temp_str;
                    // use python logging helper...
                    log_msg_to_python_logging(logtypes::info, msg.message.c_str());

                    std::cout << "logged message: " << message << std::endl;

                }

                _messageBuffer.clear();
            }
//            } catch(...) {
//                fprintf(stderr, "failed to communicate message buffer from python sink");
//            }
            if(acquireGIL)
                python::unlockGIL();

            printf("flush to python done.");
        }
    protected:
        virtual void sink_it_(const spdlog::details::log_msg& spdlog_msg) override {
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

            printf("calling sink_it_ in pysink\n");
            // invoke mutex
            std::lock_guard<std::mutex> lock(_bufMutex);
            printf("mutex acquired, sinking msg\n");

            // need to read from msg because at some point memory gets invalidated

            LogMessage msg;
            msg.message = std::string(spdlog_msg.payload.data());
            std::cout<<"message is: "<<msg.message<<std::endl;
            _messageBuffer.push_back(msg);
            printf("message stored!\n");
        }

        virtual void flush_() override {
           // don't do anything here...
        }
    private:

        struct LogMessage {
            std::string message;
        };

        std::vector<LogMessage> _messageBuffer;
        PyObject* _pyFunctor;
        std::mutex _bufMutex;
    };

    using no_gil_python3_sink_mt = nogil_python3_sink<std::mutex>;
    using no_gil_python3_sink_st = nogil_python3_sink<spdlog::details::null_mutex>;

    extern boost::python::object registerPythonLogger(boost::python::object log_functor);
}
#endif //TUPLEX_PYTHONCOMMON_H
