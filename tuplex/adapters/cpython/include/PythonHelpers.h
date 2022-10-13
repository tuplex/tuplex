//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONHELPERS_H
#define TUPLEX_PYTHONHELPERS_H

#include "PythonSerializer.h"
#include <string>
#include <ExceptionCodes.h>
#include <Row.h>

namespace python {

    /*!
     * helper structure to capture extended call result for exception tracing.
     */
    struct PythonCallResult {
        PyObject *res;
        std::string exceptionClass;
        std::string exceptionMessage;
        long exceptionLineNo;
        std::string functionName;
        std::string file;
        long functionFirstLineNo;
        tuplex::ExceptionCode exceptionCode;

        PythonCallResult() : res(nullptr),
                             exceptionLineNo(-1),
                             functionFirstLineNo(-1), exceptionCode(tuplex::ExceptionCode::SUCCESS)    {}
    };

    /*!
     * helper function to construct a Python3 str from a char pointer.
     * @param str '\0' delimited string
     * @return Python UTF8 string holding str contents
     */
    extern PyObject* PyString_FromString(const char* str);

    /*!
     * helper function to convert a single char to a python string
     * @param c character
     * @return PyObject representing a UTF8 string
     */
    inline PyObject* PyString_FromChar(const char c) {
        std::string s = " ";
        s[0] = c;
        return PyString_FromString(s.c_str());
    }

    /*!
     * return PyUnicode object as C++ string
     * @param obj
     * @return get's the underlying string representation
     */
    extern std::string PyString_AsString(PyObject* obj);


    /*!
     * helper function to construct boolean object with ref counting
     * @param val value to convert to Python
     * @return Py_True or Py_False including updated refcounts.
     */
    inline PyObject* boolToPython(bool val) {
        if(val) {
            Py_INCREF(Py_True); // list needs a ref, so inc ref count
            return Py_True;
        } else {
            Py_INCREF(Py_False); // list needs a ref, so inc ref count
            return Py_False;
        }
    }

    inline PyObject* boolean(bool val) { return boolToPython(val); }

    /*!
     * returns Py_None after calling incref, similar to Py_RETURN_NONE
     * @return Py_None
     */
    inline PyObject* none() {
        Py_XINCREF(Py_None);
        return Py_None;
    }

    /*!
     * set python home to local directory
     * @param home_dir string pointing to local path where standard python libraries are stored.
     *                 cf. https://docs.python.org/3/using/cmdline.html#envvar-PYTHONHOME for meaning.
     */
    void python_home_setup(const std::string& home_dir);

    /*!
     * returns python version build against as major.minor.patch
     * @param minor whether to include minor
     * @param patch whether to include patch (minor needs to be true as well)
     * @return string
     */
    inline std::string python_version(bool minor=true, bool patch=false) {
        auto version = std::to_string(PY_MAJOR_VERSION);
        if(minor)
            version += "." + std::to_string(PY_MINOR_VERSION);
        if(minor && patch)
            version += "." + std::to_string(PY_MICRO_VERSION);
        return version;
    }

    /*!
     * retrieves main module and loads cloudpickle module. Exits program if cloudpickle is not found.
     * @return module object holding the main module
     */
    extern PyObject* getMainModule();

    /*!
     * deserializes a (cloud)pickled function using the cloudpickle module. Make sure cloudpickle is loaded in mainModule
     * @param mainModule the main module holding everything incl. cloudpickle module
     * @param mem pointer to memory region
     * @param size size of memory region
     * @return Python function object
     */
    extern PyObject* deserializePickledFunction(PyObject* mainModule, const char* mem, const size_t size);

    /*!
     * call pickle.load function on memory region
     * @param mainModule
     * @param mem
     * @param size
     * @return deserialized object or null
     */
    inline PyObject* deserializePickledObject(PyObject* mainModule, const char* mem, const size_t size) {
        assert(mem);

        auto pFunc = PyObject_GetAttrString(mainModule, "loads");
        assert(pFunc);

        auto pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, PyBytes_FromStringAndSize(mem, size));
        auto pObject = PyObject_CallObject(pFunc, pArgs);
        return pObject;
    }

    /*!
     * serialize object to memory
     * @param mainModule
     * @param obj
     * @return memory region (not necessarily zero-delimited)
     */
    inline std::string pickleObject(PyObject* mainModule, PyObject* obj) {

        using namespace std;

        // now serialize the damn thing
        auto pDumpsFunc = PyObject_GetAttrString(mainModule, "dumps");
        if(!pDumpsFunc) {
            Logger::instance().defaultLogger().info("dumps not found in mainModule!");
            return "";
        }
        assert(pDumpsFunc);
        auto pArgs = PyTuple_New(1);
        assert(pArgs);
        PyTuple_SetItem(pArgs, 0, obj);
        auto pBytesObj = PyObject_CallObject(pDumpsFunc, pArgs);
        if(!pBytesObj)
            return ""; // error string...
        assert(pBytesObj);

        std::string res;
        size_t mem_size = PyBytes_Size(pBytesObj);
#ifndef NDEBUG
        const char *mem = PyBytes_AsString(pBytesObj);
#else
        const char *mem = PyBytes_AS_STRING(pBytesObj);
#endif
        res.reserve(mem_size);
        res.assign(mem, mem_size); // important to add here size! Else, full code won't get copied (there may be '\0' bytes)

        assert(res.length() == mem_size);
        return res;
    }

    /*!
     * retrieves the (positional) argument names of the corresponding function object
     * @param pFunc Python function object
     * @return argument names as they appear in the function signature
     */
    extern std::vector<std::string> getArgNames(PyObject *pFunc);

    /*!
     * compiles code given as string and returns pickled version of it.
     * @param mainModule module where to add code to
     * @param code python code as C++ string
     * @return pickled version of the object submitted. Empty string if error occured.
     */
    extern std::string serializeFunction(PyObject* mainModule, const std::string& code);

    /*!
     * compiles code and returns callable
     * @param mainModule module where to add code to
     * @param code python code as C++ string
     * @return NULL if compilation failed, else callable object.
     */
    extern PyObject* compileFunction(PyObject* mainModule, const std::string& code);

    /*!
     * calls (depickled) function pFunc on pObj.
     * @param pFunc Function to be called
     * @param pArgs Object to be used as input (i.e. a tuple)
     * @param ec If anything goes wrong, the python->tuplex translated exception will be stored there
     * @return result of the function as tuple object. nullptr if exception was raised.
     */
    extern PyObject* callFunction(PyObject* pFunc, PyObject* pArgs, tuplex::ExceptionCode& ec);

    /*!
     * If signature of the function is def f(x): ... and fields are accessed via x[columnName], this functions calls it
     * by providing a dictionary as single parameter
     * @param pFunc funciton to be called
     * @param pArgs a tuple of arguments to use when calling the function
     * @param columns column names, used for dictionary constructionc
     * @param ec where to capture return code of function
     * @return return object of function call
     */
    extern PyObject* callFunctionWithDict(PyObject* pFunc, PyObject* pArgs, const std::vector<std::string>& columns, tuplex::ExceptionCode& ec);

    /*!
     * call python function and return extended call result incl. possible traceback/exception data
     * @param pFunc callable
     * @param pArgs tuple
     * @param kwargs dict
     * @return
     */
    extern PythonCallResult callFunctionEx(PyObject* pFunc, PyObject* pArgs, PyObject* kwargs=nullptr);

    /*!
     * call function with dictionary input, record exceptions
     * @param pFunc
     * @param pArgs
     * @param columns
     * @return
     */
    extern PythonCallResult callFunctionWithDictEx(PyObject* pFunc, PyObject* pArgs, const std::vector<std::string>& columns);

    /*!
     * converts a python object to a tuplex C++ object. Direct mapping. Use version with type to account for nullable types.
     * @param obj python object
     * @return C++ Tuplex object
     */
    extern tuplex::Row pythonToRow(PyObject* obj);

    /*!
     * converts a field to a python object
     * @param f
     * @return python object, nullptr if conversion failed
     */
    extern PyObject* fieldToPython(const tuplex::Field& f);

    /*!
     * converts a python object to tuplex object, obj must be not null.
     * @param obj if it can not be mapped to a tuplex type, stored as PYOBJECT (cloudpickled).
     * @param autoUpcast whether to upcast numeric types to a unified type when type conflicts, false by default
     * @return C++ Tuplex Field object
     */
    extern tuplex::Field pythonToField(PyObject *obj, bool autoUpcast=false);

    /*!
     * helper function to map a python dictionary object to a struct dict field.
     * Note that this function can easily get quite expensive... -> potentially should map
     * using pyobject?
     * @param dict_obj
     * @param autoUpcast
     * @return struct dict field.
     */
    extern tuplex::Field py_dict_to_field(PyObject* dict_obj, bool autoUpcast=false);

    /*!
     * converts python object to Row using row type supplied in type.
     * @param obj
     * @param type specify what type of objects should be serialized, may contain options.
     * @param autoUpcast whether to upcast numeric types to a unified type when type conflicts, false by default
     * @return Tuplex C++ row object.
     */
    extern tuplex::Row pythonToRow(PyObject *obj, const python::Type &type, bool autoUpcast=false);

    /*!
     * converts a Tuplex C++ object to a python object
     * @param row
     * @param unpackPrimitives if there is a single element
     * (i.e. single integer, string, float, tuple, ...), when true the primitive will be returned and not a tuple.
     * @return python object (i.e. a tuple or single value)
     */
    extern PyObject* rowToPython(const tuplex::Row& row, bool unpackPrimitives=false);

    /*!
     * performs type detection by applying python func pFunc on a number of sample rows
     * @param pFunc
     * @param vSample
     * @return detected type or UNKNOWN if detection failed
     */
    extern python::Type detectTypeWithSample(PyObject* pFunc, const std::vector<tuplex::Row>& vSample);

    /*!
     * returns the number of positional arguments of a python function object
     * @param func
     * @return number of positional arguments
     */
    extern size_t pythonFunctionPositionalArgCount(PyObject* func);

    /*!
     * returns name of the function. In case func is a lambda, it will return "lambda"
     * @param func PyObject holding the function
     * @return name of the function
     */
    extern std::string pythonFunctionGetName(PyObject* func);

    /*!
     * shutdown interpreter thread-safe
     */
    extern void closeInterpreter();

    /*!
     * creates and inits interpreter
     */
    extern void initInterpreter();

    /*!
     * force run a gc pass in python. Helps when returning python objects.
     */
    extern void runGC();

    /*!
     * check whether Python interpreter is running in/available to this process
     * @return bool when is running else false
     */
    extern bool isInterpreterRunning();

    /*!
     * get corresponding tuplex type for python object
     * @param o python object to map to Tuplex type
     * @param autoUpcast whether to upcast numeric types to a unified type when type conflicts, false by default
     * @return internal Tuplex type corresponding to given python object.
     */
    extern python::Type mapPythonClassToTuplexType(PyObject *o, bool autoUpcast=false);

    /*!
     * Tuplex's python API provides a paramter to (optionally) specify a schema, this functions decodes that PyObject
     * to an internal type representation
     */
    extern python::Type decodePythonSchema(PyObject *schemaObject);

    /*!
     * encode tuplex type as Python type objects. I.e. I64 -> int, F64 -> float, ...
     */
     extern PyObject* encodePythonSchema(const python::Type& t);

    /*!
     * acquire GIL(global interpreter lock), i.e. lock global python mutex
     */
    extern void lockGIL();

    /*!
     * release GIL(global interpreter lock)
     */
    extern void unlockGIL();

    /*!
     * needs to be called if using as C-extension to setup gil etc. MUST BE CALLED FROM MAIN THREAD
     */
    extern void registerWithInterpreter();

    /*!
     * check whether this thread holds the GIL or not
     * @return
     */
    extern bool holdsGIL();


    /*!
     * runs python code (throws runtime error if err occurred) and retrieves object with name
     * @param code
     * @param name
     * @return object or nullptr
     */
    extern PyObject* runAndGet(const std::string& code, const std::string& name="");


    /*!
     * returns the name of the type of a python object as C++ object
     * @param object
     * @return e.g. str for a string
     */
    inline std::string typeName(PyObject* object) {
        assert(object);
        auto typeObj = PyObject_Type(object);
        assert(typeObj);

        auto typeNameObj = PyObject_GetAttrString(typeObj, "__name__"); // name of type
        assert(typeNameObj);

        return PyString_AsString(typeNameObj);
    }

    /*!
     * translate exception object type to tuplex type
     * @param type
     * @return tuplex exception code
     */
    extern tuplex::ExceptionCode translatePythonExceptionType(PyObject* type);

    extern std::string platformExtensionSuffix();

    /*!
     * check cloudpickle version compatibility.
     * @param os optional error stream to output
     * @return true if ok, false else.
     */
    extern bool cloudpickleCompatibility(std::ostream* os=nullptr);

    extern PyObject* json_string_to_pyobject(const std::string& s, const python::Type& type);
}


#endif //TUPLEX_PYTHONHELPERS_H
