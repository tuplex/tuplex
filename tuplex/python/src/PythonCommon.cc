//
// Created by Leonhard Spiegelberg on 11/9/21.
//

#include <PythonCommon.h>


/***********************************************************/
/* define logging function and logtypes for python.logging */
/* by H.Dickten 2014                                       */
/***********************************************************/
// from https://gist.github.com/hensing/0db3f8e3a99590006368

namespace tuplex {


    void log_msg_to_python_logging(int type, const char *msg) {
        static PyObject *logging = NULL;
        static PyObject *string = NULL;

        // import logging module on demand
        if (logging == NULL) {
            logging = PyImport_ImportModuleNoBlock("logging");
            if (logging == NULL)
                PyErr_SetString(PyExc_ImportError,
                                "Could not import module 'logging'");
        }

        // build msg-string
        string = Py_BuildValue("s", msg);

        // call function depending on loglevel
        switch (type) {
            case info:
                PyObject_CallMethod(logging, "info", "O", string);
                break;

            case warning:
                PyObject_CallMethod(logging, "warn", "O", string);
                break;

            case error:
                PyObject_CallMethod(logging, "error", "O", string);
                break;

            case debug:
                PyObject_CallMethod(logging, "debug", "O", string);
                break;
        }
        Py_DECREF(string);
    }

}
namespace tuplex {
    boost::python::object registerPythonLogger(boost::python::object log_functor) {

        printf("calling registerPythonLogger\n");

        // get object
        auto functor_obj = boost::python::incref(get_managed_object(log_functor, boost::python::tag));

        std::cout<<"got object from boost python"<<std::endl;

        if(!functor_obj) {
            std::cerr<<"invalid functor obj passed?"<<std::endl;
        }

        // make sure it's callable etc.
        if(!PyCallable_Check(functor_obj))
            throw std::runtime_error(python::PyString_AsString(functor_obj) + " is not callable. Can't register as logger.");

        std::cout<<"testing call to functor..."<<std::endl;

        // test passed logger object
        auto args = PyTuple_New(1);
        auto py_msg = python::PyString_FromString("test message");
        PyTuple_SET_ITEM(args, 0, py_msg);

        PyObject_Call(functor_obj, args, nullptr);
        if(PyErr_Occurred()) {
            std::cout<<"error occurred while calling functor obj..."<<std::endl;
            PyErr_Print();
            std::cout<<std::endl;
            PyErr_Clear();
        }

        std::cout<<"test call to functor completed..."<<std::endl;
        // test call to logging module after
        log_msg_to_python_logging(logtypes::info, "this is a test message from the C++ backend...");

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

        printf("pylogger added, all good\n");
        // TODO: make sure Logger is never called while thread holds GIL!


        // return None
        return boost::python::object();
    }
}