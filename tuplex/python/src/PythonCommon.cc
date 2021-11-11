//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/9/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PythonCommon.h>

namespace tuplex {
    boost::python::object registerPythonLoggingCallback(boost::python::object callback_functor) {
        // get object
        auto functor_obj = boost::python::incref(get_managed_object(callback_functor, boost::python::tag));

        if(!functor_obj) {
            std::cerr<<"invalid functor obj passed?"<<std::endl;
        }

        // make sure it's callable etc.
        if(!PyCallable_Check(functor_obj))
            throw std::runtime_error(python::PyString_AsString(functor_obj) + " is not callable. Can't register as logger.");

        // check that func takes exactly 4 args
        // add new sink to loggers with this function
        python::unlockGIL();
        try {
            // this replaces current logging scheme with python only redirect...
            Logger::instance().init({std::make_shared<no_gil_python3_sink_mt>(functor_obj)});
        } catch(const std::exception& e) {
            // use C printing for the exception here
            std::cerr<<"while registering python callback logging mechanism, following error occurred: "<<e.what()<<std::endl;
        }
        python::lockGIL();

        // return None
        return boost::python::object();
    }
}