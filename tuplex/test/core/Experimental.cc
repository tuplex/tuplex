//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Python.h>
#include <gtest/gtest.h>
#include <PythonHelpers.h>
#include <RuntimeInterface.h>
#include <ContextOptions.h>
#include <vector>
#include <Utils.h>

PyMemAllocatorEx oldAllocRaw;
PyMemAllocatorEx oldAllocMem;
PyMemAllocatorEx oldAllocObj;

void* Custom_null = nullptr;
size_t Custom_memUsed = 0;
std::vector<void*> Custom_ptrs;

// hack: because python does weird cleanup stuff, need to hack after usage an allocator back:



void* Custom_malloc(void* ctx, size_t size) {
    if(0 == size)
        return Custom_null;
    Custom_memUsed += size;
    //return malloc(size);
    auto ptr = tuplex::runtime::rtmalloc(size);

    Custom_ptrs.emplace_back(ptr);
    std::cout<<"allocated "<<tuplex::hexaddr(ptr)<< " ("<<size<<" bytes)"<<std::endl;
    return ptr;
}

void* Custom_realloc(void *ctx, void* ptr, size_t size) {
    if(0 == size)
        return Custom_null;
    // doesn't work...
    auto new_ptr = tuplex::runtime::rtmalloc(size);
    if(ptr)
        memcpy(new_ptr, ptr, size);
    Custom_ptrs.emplace_back(new_ptr);
    std::cout<<"(re)allocated "<<tuplex::hexaddr(new_ptr)<< " ("<<size<<" bytes)"<<std::endl;
    return new_ptr;
}

void* Custom_calloc(void* ctx, size_t nelem, size_t size) {
    if(0 == size || nelem == 0)
        return Custom_null;
    Custom_memUsed += nelem * size;

    //return calloc(nelem, size);
    auto ptr = tuplex::runtime::rtmalloc(size * nelem);
    memset(ptr, 0, size * nelem);
    Custom_ptrs.emplace_back(ptr);
    std::cout<<"callocated "<<tuplex::hexaddr(ptr)<< " ("<<size<<" bytes)"<<std::endl;
    return ptr;
}

void Custom_free(void* ctx, void* ptr) {

    // need to count memory usage here...
    // Custom_memUsed -= ...
    // call to free of python objects...
    //free(ptr);

    // remove from list
    auto it = std::find(Custom_ptrs.begin(), Custom_ptrs.end(), ptr);
    if(it != Custom_ptrs.end()) {
        Custom_ptrs.erase(it);
        std::cout<<"freed "<<tuplex::hexaddr(ptr)<<std::endl;
    }
}


void Wrapped_free_Raw(void* ctx, void* ptr) {

    // check if block is managed by Tuplex
    auto it = std::find(Custom_ptrs.begin(), Custom_ptrs.end(), ptr);
    if(it != Custom_ptrs.end()) {
        Custom_ptrs.erase(it); // do nothing, memory freed via freeRunTimeMemory
        std::cout<<"raw free (internal) for "<<tuplex::hexAddr(ptr)<<std::endl;
    }
    else {
        // fallback to python default allocator
        oldAllocRaw.free(ctx, ptr);
        std::cout<<"raw free (cpython]) for "<<tuplex::hexAddr(ptr)<<std::endl;
    }
}

void Wrapped_free_Mem(void* ctx, void* ptr) {

    // check if block is managed by Tuplex
    auto it = std::find(Custom_ptrs.begin(), Custom_ptrs.end(), ptr);
    if(it != Custom_ptrs.end()) {
        std::cout<<"mem free (internal) for "<<tuplex::hexAddr(ptr)<<std::endl;
        Custom_ptrs.erase(it); // do nothing, memory freed via freeRunTimeMemory
    }
    else {
        // fallback to python default allocator
        oldAllocMem.free(ctx, ptr);
        std::cout<<"mem free (cpython) for "<<tuplex::hexAddr(ptr)<<std::endl;
    }
}

void Wrapped_free_Obj(void* ctx, void* ptr) {

    // check if block is managed by Tuplex
    auto it = std::find(Custom_ptrs.begin(), Custom_ptrs.end(), ptr);
    if(it != Custom_ptrs.end()) {
        Custom_ptrs.erase(it); // do nothing, memory freed via freeRunTimeMemory
        std::cout<<"obj free (internal) for "<<tuplex::hexAddr(ptr)<<std::endl;
    }
    else {
        // fallback to python default allocator
        oldAllocObj.free(ctx, ptr);
        std::cout<<"obj free (cpython) for "<<tuplex::hexAddr(ptr)<<std::endl;
    }
}

// confer https://github.com/clan-fd/cyphesis/blob/c15fa3e56456beff487398f2ec2dd064844838e0/rules/python/Python_API.cpp
TEST(PythonAlloc, CustomAllocator) {
    using namespace tuplex;
    ContextOptions co = ContextOptions::defaults();
    runtime::init(co.RUNTIME_LIBRARY().toPath());


    python::initInterpreter();

    // doesn't work in docker... to be fixed when debug works...
//    PyMemAllocatorEx alloc = {nullptr, Custom_malloc, Custom_calloc, Custom_realloc, Custom_free};
//
//    PyMem_GetAllocator(PYMEM_DOMAIN_RAW, &oldAllocRaw);
//    PyMem_GetAllocator(PYMEM_DOMAIN_MEM, &oldAllocMem);
//    PyMem_GetAllocator(PYMEM_DOMAIN_OBJ, &oldAllocObj);
//
//
//    // use custom allocator
//    PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &alloc);
//    PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &alloc);
//    PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &alloc);
//
//
//    // disable garbage collection
//
//    // run custom string, use custom allocator for this...
//    PyRun_SimpleString("a = 10 + 30");
//
//
//    // convert all the objects to PyObject* and set tp_dealloc to dummy func because no dealloc is necessary...
//    std::vector<PyObject*> dummies;
//    for(auto ptr : Custom_ptrs) {
//        dummies.emplace_back((PyObject*)ptr);
//    }
//
//
//    // free runtime memory (rtmalloced python objects??)
//    runtime::freeRunTimeMemory();
//
//
//    // restore python default allocator via wrappers
//    PyMemAllocatorEx allocRaw = oldAllocRaw;
//    allocRaw.free = Wrapped_free_Raw;
//    PyMemAllocatorEx allocMem = oldAllocMem;
//    allocMem.free = Wrapped_free_Mem;
//    PyMemAllocatorEx allocObj = oldAllocObj;
//    allocObj.free = Wrapped_free_Obj;
//
//    PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &allocRaw);
//    PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &allocMem);
//    PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &allocObj);

    python::closeInterpreter();
}