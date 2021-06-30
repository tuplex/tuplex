//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <JITCompiler.h>
#include "TestUtils.h"
#include <physical/PipelineBuilder.h>
#include <RuntimeInterface.h>
#include <physical/CodeDefs.h>

#define OUTPUT_BUFFER_SIZE 2048


std::unique_ptr<tuplex::Executor> testExecutor() {
    using namespace tuplex;
    // create a small executor for test purposes
    return std::make_unique<tuplex::Executor>(memStringToSize("8MB"), memStringToSize("256KB"), memStringToSize("1MB"), memStringToSize("256KB"), "./cache");
}

uint8_t* simpleRequester(void* userData, int64_t minRequired, int64_t* num) {
    *num = OUTPUT_BUFFER_SIZE;

    return (uint8_t*)userData;
}

//static g_deserializer
void execRow_writer(uint8_t** outBuf, uint8_t* buf, int64_t bufSize) {
    // copy runtime buffer to test buffer
    assert(outBuf);
    *outBuf = (uint8_t*)malloc(bufSize);
    memcpy(*outBuf, buf, bufSize);
}

// helper function to take the row & provide codegenerated deserialize/serialize code
// return deserialized row
tuplex::Row execRow(const tuplex::Row& input, tuplex::UDF udf) {
    using namespace tuplex;
    using namespace std;

    // load runtime
    tuplex::runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    // type hint udf
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, input.getRowType()));

    // compile function
    auto env = make_shared<codegen::LLVMEnvironment>();
    auto pip = make_shared<codegen::PipelineBuilder>(env, input.getRowType());

    // add simple map
    pip->mapOperation(0, udf, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);


    // create simple mapper
    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
    string funName = llvmFunc->getName();

    auto ir = env->getIR();

#ifndef NDEBUG
    std::cout<<"\n---\n"<<ir<<"\n---\n"<<std::endl;
#endif


    // init runtime
    auto co = microTestOptions();
    runtime::init(co.RUNTIME_LIBRARY().toPath());

    // execute row executor
    auto compiler = make_shared<JITCompiler>();

    // add symbol
    compiler->registerSymbol("execRow_writeback", execRow_writer);
    bool compilation_success = compiler->compile(ir);
    assert(compilation_success);

    uint8_t* outBuf = nullptr;

    auto functor = (codegen::process_row_f)compiler->getAddrOfSymbol(funName);
    assert(functor);

    // serialize row to buffer
    static const int safetyFactor = 5;
    uint8_t *buffer = new uint8_t[safetyFactor * input.serializedLength()];
    input.serializeToMemory(buffer, input.serializedLength() * safetyFactor / 2);
    // call over buf
    auto bytesWritten = functor(&outBuf, buffer, 0, 0);

    // check output
    Row outRow = Row::fromMemory(udf.getOutputSchema(), outBuf, bytesWritten);

    free(outBuf);

    tuplex::runtime::releaseRunTimeMemory();

    return outRow;
}