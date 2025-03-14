//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <pybind11/pybind11.h>
#include "../../core/include/Context.h"
#include <PythonWrappers.h>
#include <PythonException.h>
#include <PythonContext.h>
#include <PythonMetrics.h>
#include <PythonCommon.h>

// use pybind11 binding
namespace py = pybind11;

// Note: Cf. https://sceweb.sce.uhcl.edu/helm/WEBPAGE-Python/documentation/python_tutorial/api/refcountDetails.html for refguide
// and https://pybind11.readthedocs.io/en/stable/advanced/functions.html
// wrong refcounts easily can lead to segfaults/corruption for tests.

// PYMODULE is defined by cmake as PYBIND11_MODULE(name, m)
PYMODULE {
    m.doc() = R"pbdoc(
            TUPLEX C-extension
            ------------------
            .. currentmodule:: tuplex
            .. autosummary::
               :toctree: _generate
        )pbdoc";

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif

    // Perform cleanup (e.g., AWS SDK shutdown if necessary to await endless loop)
    // Register a callback function that is invoked when the BaseClass object is collected
    // cf. https://pybind11.readthedocs.io/en/stable/advanced/misc.html
    auto cleanup_callback = []() {
        // perform cleanup here -- this function is called with the GIL held
        // std::cout<<"Pybind11 clean up call here."<<std::endl;

        // When using AWS SDK, important to explicitly call sdk shutdown. AWS SDK creates a threadpool,
        // if shutdown is not issued an endless loop will occur at shutdown due to the SDK waiting on mutexes.
#ifdef BUILD_WITH_AWS
        // std::cout<<"Shutting down AWS SDK."<<std::endl;
        tuplex::shutdownAWS();
        // std::cout<<"AWS cleanup done."<<std::endl;
#endif
    };

    m.add_object("_cleanup", py::capsule(cleanup_callback));

    // Note: before constructing any object - call registerWithInterpreter to setup GIL properly!

    py::class_<tuplex::PythonDataSet>(m, "_DataSet")
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
            .def("renameColumnByPosition", &tuplex::PythonDataSet::renameColumnByPosition)
            .def("join", &tuplex::PythonDataSet::join)
            .def("leftJoin", &tuplex::PythonDataSet::leftJoin)
            .def("columns", &tuplex::PythonDataSet::columns)
            .def("cache", &tuplex::PythonDataSet::cache)
            .def("tocsv", &tuplex::PythonDataSet::tocsv)
            .def("toorc", &tuplex::PythonDataSet::toorc)
            .def("unique", &tuplex::PythonDataSet::unique)
            .def("aggregate", &tuplex::PythonDataSet::aggregate)
            .def("aggregateByKey", &tuplex::PythonDataSet::aggregateByKey)
            .def("types", &tuplex::PythonDataSet::types)
            .def("exception_counts", &tuplex::PythonDataSet::exception_counts);

    py::class_<tuplex::PythonContext>(m, "_Context")
            .def(py::init<std::string, std::string, std::string>()) // other constructor
            .def(py::init<std::string>()) // default C++ ctor
            .def("csv", &tuplex::PythonContext::csv)
            .def("text", &tuplex::PythonContext::text)
            .def("orc", &tuplex::PythonContext::orc)
            .def("parallelize", &tuplex::PythonContext::parallelize)
            .def("options", &tuplex::PythonContext::options)
            .def("getMetrics", &tuplex::PythonContext::getMetrics)
            .def("ls", &tuplex::PythonContext::ls)
            .def("cp", &tuplex::PythonContext::cp)
            .def("rm", &tuplex::PythonContext::rm);

    py::class_<tuplex::PythonMetrics>(m, "_Metrics")
            .def("getLogicalOptimizationTime", &tuplex::PythonMetrics::getLogicalOptimizationTime)
            .def("getLLVMOptimizationTime", &tuplex::PythonMetrics::getLLVMOptimizationTime)
            .def("getLLVMCompilationTime", &tuplex::PythonMetrics::getLLVMCompilationTime)
            .def("getTotalCompilationTime", &tuplex::PythonMetrics::getTotalCompilationTime)
            .def("getTotalExceptionCount", &tuplex::PythonMetrics::getTotalExceptionCount)
            .def("getJSONString", &tuplex::PythonMetrics::getJSONString);

    // global method to access default options as json
    m.def("getDefaultOptionsAsJSON", &tuplex::getDefaultOptionsAsJSON);

    // global method to register a new logging function
    m.def("registerLoggingCallback", &tuplex::registerPythonLoggingCallback);

    m.def("registerWithInterpreter", &python::registerWithInterpreter);

    m.def("getPythonVersion", &tuplex::getPythonVersion);

    m.def("setExternalAwssdk", &tuplex::setExternalAwssdk);
}