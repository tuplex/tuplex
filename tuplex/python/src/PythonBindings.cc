//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <boost/python.hpp>
#include "../../core/include/Context.h"
#include <PythonWrappers.h>
#include <PythonException.h>
#include <PythonContext.h>
#include <PythonMetrics.h>

using namespace boost::python;

// PYMODULE is defined by cmake as BOOST_PYTHON_MODULE(name)
PYMODULE {


    // important to prevent weird errors in jupyter?
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION < 7)
    PyEval_InitThreads();
#endif

    // // debug output when issues arise whether the extension module was correctly initialized or not.
    // std::cout<<"initializing interpreter by acquiring gil state/thread state"<<std::endl;
    // PyEval_InitThreads();
    // PyThreadState *mainstate = PyThreadState_Get();
    // std::cout<<"main thread state is: "<<mainstate<<std::endl;
    // std::cout<<"Status of PyEval_ThreadsInitialized threads: "<<PyEval_ThreadsInitialized()<<std::endl;
    // std::cout<<"init done"<<std::endl;


    //register_exception_translator<tuplex::PythonException>(&tuplex::translateCCException);

    class_<tuplex::PythonDataSet>("_DataSet")
            .def("show", &tuplex::PythonDataSet::show)
            .def("collect", &tuplex::PythonDataSet::collect)
            .def("take", &tuplex::PythonDataSet::take)
            .def("map", &tuplex::PythonDataSet::map)
            .def("resolve", &tuplex::PythonDataSet::resolve)
            .def("ignore", &tuplex::PythonDataSet::ignore)
            .def("filter", &tuplex::PythonDataSet::filter)
            .def("mapColumn", &tuplex::PythonDataSet::mapColumn)
            .def("withColumn", &tuplex::PythonDataSet::withColumn)
            .def("selectColumns", &tuplex::PythonDataSet::selectColumns)
            .def("renameColumn", &tuplex::PythonDataSet::renameColumn)
            .def("join", &tuplex::PythonDataSet::join)
            .def("leftJoin", &tuplex::PythonDataSet::leftJoin)
            .def("columns", &tuplex::PythonDataSet::columns)
            .def("cache", &tuplex::PythonDataSet::cache)
            .def("tocsv", &tuplex::PythonDataSet::tocsv)
            .def("unique", &tuplex::PythonDataSet::unique)
            .def("aggregate", &tuplex::PythonDataSet::aggregate)
            .def("aggregateByKey", &tuplex::PythonDataSet::aggregateByKey)
            .def("types", &tuplex::PythonDataSet::types)
            .def("exception_counts", &tuplex::PythonDataSet::exception_counts);

    class_<tuplex::PythonContext>("_Context", init<std::string, std::string, std::string>())
            .def(init<std::string>()) // default C++ ctor
            .def("csv", &tuplex::PythonContext::csv)
            .def("text", &tuplex::PythonContext::text)
            .def("parallelize", &tuplex::PythonContext::parallelize)
            .def("options", &tuplex::PythonContext::options)
            .def("getMetrics", &tuplex::PythonContext::getMetrics)
            .def("ls", &tuplex::PythonContext::ls)
            .def("cp", &tuplex::PythonContext::cp)
            .def("rm", &tuplex::PythonContext::rm);

    class_<tuplex::PythonMetrics>("_Metrics")
            .def("getLogicalOptimizationTime", &tuplex::PythonMetrics::getLogicalOptimizationTime)
            .def("getLLVMOptimizationTime", &tuplex::PythonMetrics::getLLVMOptimizationTime)
            .def("getLLVMCompilationTime", &tuplex::PythonMetrics::getLLVMCompilationTime)
            .def("getTotalCompilationTime", &tuplex::PythonMetrics::getTotalCompilationTime)
            .def("getTotalExceptionCount", &tuplex::PythonMetrics::getTotalExceptionCount)
            .def("getJSONString", &tuplex::PythonMetrics::getJSONString);
}
