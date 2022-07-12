//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/codegen/PythonCallbacks.h>
#include <jit/RuntimeInterface.h>

#warning "TODO: create versions which allow for a) dictmode b) multiparam syntax... c) both?"

/*****************************************
 * helper functions to invoke fallback python code
 */
// helper function to call all the fun things
// Note: this function needs to be here because of the TypeFactory Singleton is not shared with the dynamic lib!
// in the future, refactor such that the conversion code gets auto generated.
extern "C" int32_t callPythonCodeSingleParam(PyObject* func, uint8_t** out, int64_t* out_size,
                                  uint8_t* input, int64_t input_size, int32_t in_typeHash, int32_t out_typeHash) {
    // make sure vars are setup correctly
    assert(out);
    assert(out_size);
    assert(input);

    // deserialize contents
    using namespace tuplex;

    // note: this here is wrong:
    // since, this is a dylib, the type factory is a separate object.
    // need to point to the TypeFactory object being used in the main program!
    //@Todo: solve this!
    // ==> i.e. register type with the runtime!!!
    // or, make sure this is in the main library as symbol...

    python::Type input_type = python::TypeFactory::instance().getByHash(in_typeHash);
    python::Type output_type = python::TypeFactory::instance().getByHash(out_typeHash);


    // propagate to tuple type, since row.getRowType() always returns a tuple type.
    output_type = python::Type::propagateToTupleType(output_type);

    assert(input_type != python::Type::UNKNOWN);
    assert(output_type != python::Type::UNKNOWN);

    Schema schema(Schema::MemoryLayout::ROW, input_type);
    Row in_row = Row::fromMemory(schema, input, input_size);
    Row out_row;

    // std::cout<<"type is: "<<input_type.desc()<<std::endl;
    // std::cout<<"input is "<<in_row.toPythonString()<<std::endl;

    // ensure GIL is locked for this thread
    python::lockGIL();

    PyObject* rowObj = python::rowToPython(in_row);

    // call the python function
    ExceptionCode ec;
    auto pyobj_res = python::callFunction(func, rowObj, ec);

    // decref pyobj_res?
    if(pyobj_res)
        out_row = python::pythonToRow(pyobj_res);

    python::unlockGIL();
    // release GIL

    // std::cout<<"output type is: "<<output_type.desc()<<std::endl;
    // std::cout<<"output is "<<out_row.toPythonString()<<std::endl;

    // now check result & exception code
    if(ec != ExceptionCode::SUCCESS) {
        *out = nullptr;
        *out_size = 0;
        return ecToI32(ec);
    } else {
        // check whether return type of row matches required type or not
        if(out_row.getRowType() != output_type) {
            // --> TypeError exception!
            *out = nullptr;
            *out_size = 0;
            return ecToI32(ExceptionCode::TYPEERROR);
        }


        // normal behaviour now
        size_t serializedSize = out_row.serializedLength();

        // allocate using runtime alloc,
        // memory for this is reused every iteration
        uint8_t* buffer = (uint8_t*)runtime::rtmalloc(serializedSize);

        out_row.serializeToMemory(buffer, serializedSize);

        *out = buffer;
        *out_size = serializedSize;
        return ecToI32(ExceptionCode::SUCCESS);
    }
}




extern "C" int32_t callPythonCodeMultiParam(PyObject* func, uint8_t** out, int64_t* out_size,
                                  uint8_t* input, int64_t input_size, int32_t in_typeHash, int32_t out_typeHash) {
    // make sure vars are setup correctly
    assert(out);
    assert(out_size);
    assert(input);

    // deserialize contents
    using namespace tuplex;

    // note: this here is wrong:
    // since, this is a dylib, the type factory is a separate object.
    // need to point to the TypeFactory object being used in the main program!
    //@Todo: solve this!
    // ==> i.e. register type with the runtime!!!
    // or, make sure this is in the main library as symbol...

    python::Type input_type = python::TypeFactory::instance().getByHash(in_typeHash);
    python::Type output_type = python::TypeFactory::instance().getByHash(out_typeHash);


    // propagate to tuple type, since row.getRowType() always returns a tuple type.
    output_type = python::Type::propagateToTupleType(output_type);

    assert(input_type != python::Type::UNKNOWN);
    assert(output_type != python::Type::UNKNOWN);

    Schema schema(Schema::MemoryLayout::ROW, input_type);
    Row in_row = Row::fromMemory(schema, input, input_size);
    Row out_row;

    // std::cout<<"type is: "<<input_type.desc()<<std::endl;
    // std::cout<<"input is "<<in_row.toPythonString()<<std::endl;

    // ensure GIL is locked for this thread
    python::lockGIL();

    PyObject* rowObj = python::rowToPython(in_row);

    // call the python function
    ExceptionCode ec;
    auto pyobj_res = python::callFunction(func, rowObj, ec);

    // decref pyobj_res?
    if(pyobj_res)
        out_row = python::pythonToRow(pyobj_res);

    python::unlockGIL();
    // release GIL

    // std::cout<<"output type is: "<<output_type.desc()<<std::endl;
    // std::cout<<"output is "<<out_row.toPythonString()<<std::endl;

    // now check result & exception code
    if(ec != ExceptionCode::SUCCESS) {
        *out = nullptr;
        *out_size = 0;
        return ecToI32(ec);
    } else {
        // check whether return type of row matches required type or not
        if(out_row.getRowType() != output_type) {
            // --> TypeError exception!
            *out = nullptr;
            *out_size = 0;
            return ecToI32(ExceptionCode::TYPEERROR);
        }


        // normal behaviour now
        size_t serializedSize = out_row.serializedLength();

        // allocate using runtime alloc,
        // memory for this is reused every iteration
        uint8_t* buffer = (uint8_t*)runtime::rtmalloc(serializedSize);

        out_row.serializeToMemory(buffer, serializedSize);

        *out = buffer;
        *out_size = serializedSize;
        return ecToI32(ExceptionCode::SUCCESS);
    }
}

// Python fallback functions
// mark as used to avoid LTO to optimize function away!
extern "C" uint8_t* deserializePythonFunction(char* pickledCode, int64_t codeLength) {

    // important to get GIL here
    python::lockGIL();
    auto mod = python::getMainModule();
    PyObject* pythonFunc = python::deserializePickledFunction(mod, pickledCode, codeLength);

    // release GIL here
    python::unlockGIL();

    assert(pythonFunc);
    return (uint8_t*)pythonFunc;
}

// mark as used to avoid LTO to optimize function away!
extern "C" void releasePythonFunction(uint8_t* pyobj) {
    assert(pyobj);
    auto obj = (PyObject*)pyobj;

    // important to get GIL here
    python::lockGIL();
    Py_XDECREF(obj);
    // release GIL here
    python::unlockGIL();
}