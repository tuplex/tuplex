// reference: https://aws.amazon.com/blogs/compute/introducing-the-c-lambda-runtime/
// https://gist.github.com/huihut/b4597d097123a8c8388c71b3f0ff21e5
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <aws/lambda-runtime/runtime.h>

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

using namespace aws::lambda_runtime;

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
//    std::cout << "pickled func addr: "<< (void*)py_pickled_func_bytes << std::endl;
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

const char* handler_worker(const char *payload, int &r) {
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
    // Py_Initialize();
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
        return res.c_str();
    }

    // use tuplex instead of native Python functions
    auto py_module = python::getMainModule();
    auto py_func = python::deserializePickledFunction(py_module, pickled_func_str.c_str(),
                                                      pickled_func_str.length());
    tuplex::ExceptionCode ec;
    auto py_func_res = python::callFunction(py_func, PyTuple_New(0), ec);
    auto func_res = PyFloat_AsDouble(py_func_res);
    timings.FUNCTION_TS = timestamp();

    timings.setString(func_res);
    return res.c_str();
}

static invocation_response my_handler(invocation_request const &req) {
    try {
        int r;
        handler_worker(req.payload.c_str(), r);

        if(r == -1) {
            return invocation_response::success(res, "plain/text");
        } else {
            return invocation_response::success(res, "application/json");
        }
    } catch(const std::exception& e) {
        return invocation_response::success(std::string("exception! ") + e.what(), "plain/text");
    } catch(...) {
        return invocation_response::success(std::string("unhandled exception!"), "plain/text");
    }
}
// --- BASICALLY THE SAME AS awslambda/ -------------------------------------------
int main() {
    timings.MAIN_TS = timestamp();

    // install sigsev handler to throw C++ exception which is caught in handler...
    struct sigaction sigact;
    sigact.sa_sigaction = sigsev_handler;
    sigact.sa_flags = SA_RESTART | SA_SIGINFO;
    // set sigabort too
    sigaction(SIGABRT, &sigact, nullptr);

    global_init();
    if(sigaction(SIGSEGV, &sigact, nullptr) != 0) {
        run_handler([](invocation_request const& req) {
            return invocation_response::success("could not add sigsev handler","text/plain");
        });
    } else {
        run_handler(my_handler);
    }

    std::cout.flush();
    std::cerr.flush();
    python::closeInterpreter();
    global_cleanup();
    return 0;
}
