#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include "main.h"
#include "sighandler.h"

#include <iostream>
#include <sstream>
#include <cstdlib>

#include <Python.h>
#include <JITCompiler.h>
#include <Utils.h>
#include <Timer.h>
#include <VirtualFileSystem.h>
#include <RuntimeInterface.h>

#include <google/protobuf/util/json_util.h>

// EXPERIMENT CODE ------------------------
static uint64_t timestamp() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
}

std::string res;
struct Timestamps {
    uint64_t MAIN_TS;
    uint64_t READY_TS;
    uint64_t PYTHON_READY_TS;
    uint64_t FUNCTION_TS;
    bool reused;

    void setString(double func_res) {
        std::stringstream ss;
        ss << "{"
           << "\"func_res\":" << func_res
           << ", \"reused\":" << reused
           << ", \"body\": {"
             << "\"MAIN_TS\":" << MAIN_TS
             << ", \"READY_TS\":" << READY_TS
             << ", \"PYTHON_READY_TS\":" << PYTHON_READY_TS
             << ", \"FUNCTION_TS\":" << FUNCTION_TS
           << "}}";
        reused = true; // next time, it will be true

        res = ss.str();
    }

    Timestamps() : MAIN_TS(0), READY_TS(0), PYTHON_READY_TS(0), FUNCTION_TS(0), reused(false) {}
};

Timestamps timings;

static std::string decodeFunction(const std::string &func_str, int &r) {
    // do some stupid stuff to get around not having protobuf
    auto b64Module = PyImport_ImportModule("base64");
    if(!b64Module) {
        PyErr_Print();
        r = -1;
        return "Couldn't open module base64";
    }
    auto b64decode = PyObject_GetAttrString(b64Module, "b64decode");
    if (!b64decode) {
        PyErr_Print();
        r = -1;
        return "Couldn't get function base64.b64decode";
    }
    auto py_pickled_func_bytes = PyObject_CallFunctionObjArgs(b64decode, PyUnicode_FromString(func_str.c_str()), nullptr);
    if(!py_pickled_func_bytes) {
        PyErr_Print();
        r = -1;
        return "base64.b64decode failed!";
    }
    char *mem = PyBytes_AsString(py_pickled_func_bytes);
    if(!mem) {
        PyErr_Print();
        r = -1;
        return "decoded function wasn't bytes?";
    }
    size_t mem_size = PyBytes_Size(py_pickled_func_bytes);
    std::string res;
    res.reserve(mem_size);
    res.assign(mem, mem_size);
    return res;
}

const char* alloc_str() {
    //char* ret = new char[res.length() + 1];
    //res.copy(ret, res.length());
    //ret[res.length()] = 0;
    //return ret;
    return res.c_str();
}

const char *handler_worker(const char *payload, int &r) {
    using namespace Aws::Utils::Json;
    timings.READY_TS = timestamp();
    r = 0; // no error

    // extract + set python home
    auto task_root = std::getenv("LAMBDA_TASK_ROOT");
    // convert to wchar
    std::vector<wchar_t> vec;
    auto len = strlen(task_root);
    vec.resize(len + 1);
    mbstowcs(&vec[0], task_root, len);
    Py_SetPythonHome(&vec[0]);

    // initialize interpreter
    if (!Py_IsInitialized()) {
        PyErr_Print();
        r = -1;
        return "Couldn't initialize interpreter!";
    }
    timings.PYTHON_READY_TS = timestamp();

    // grab the payload
    JsonValue json(payload);
    auto v = json.View();
    auto func_str = v.GetString("function");
    auto pickled_func_str = decodeFunction(func_str, r);
    if (r == -1) {
        res = pickled_func_str;
	return alloc_str();
    }

    /*auto py_func_str = PyUnicode_FromString(func_str.c_str());

    // pass to lambda_handler
    auto sys_path = PySys_GetObject("path");
    PyList_Append(sys_path, PyUnicode_FromString(task_root));
    auto lambda_module = PyImport_ImportModule("lambda_function");
    if (!lambda_module) {
        PyErr_Print();
        return invocation_response::success("Couldn't open module lambda_function", "text/plain");
    }
    auto lambda_handler = PyObject_GetAttrString(lambda_module, "lambda_handler");
    if (!lambda_handler) {
        PyErr_Print();
        return invocation_response::success("Couldn't get function lambda_handler", "text/plain");
    }

    // call the handler
    auto py_func_res = PyObject_CallFunctionObjArgs(lambda_handler, py_func_str);

    // save function results
    auto func_res = PyLong_AsLong(py_func_res);
    timings.FUNCTION_TS = timestamp();

    // shut down the interpreter
    //Py_Finalize();
     */

    // use tuplex instead of native Python functions
    auto py_module = python::getMainModule();
    auto py_func = python::deserializePickledFunction(py_module, pickled_func_str.c_str(),
                                                      pickled_func_str.length());
    tuplex::ExceptionCode ec;
    auto py_func_res = python::callFunction(py_func, PyTuple_New(0), ec);
    auto func_res = PyFloat_AsDouble(py_func_res);
    timings.FUNCTION_TS = timestamp();

    timings.setString(func_res);
    return alloc_str();
}

extern "C" {
  PyObject* py_lambda_handler(const char* payload) {
      int r;
      return python::PyString_FromString(handler_worker(payload, r));
  }
}

