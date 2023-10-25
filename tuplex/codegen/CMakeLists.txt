CMAKE_MINIMUM_REQUIRED(VERSION 3.19 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

## libffi
#find_package(FFI 3.2.1 REQUIRED)
#include_directories(${FFI_INCLUDE_DIRS})

# BOOST libs
include_directories(${Boost_INCLUDE_DIR})

# ANTLR4 Runtime (installed e.g. via brew install antlr4-cpp-runtime)
find_package(ANTLR4Runtime REQUIRED)

# Source code & linking
file(GLOB_RECURSE SOURCES src/*.cc)

# antlr target
add_definitions(-DANTLR4CPP_STATIC)
set(ANTLR4_WITH_STATIC_CRT OFF)
include(ExternalAntlr4Cpp)
include_directories(${ANTLR4_INCLUDE_DIRS})
set(ANTLR_EXECUTABLE ${CMAKE_CURRENT_SOURCE_DIR}/tools/antlr-4.13.1-complete.jar)
find_package(ANTLR REQUIRED)

antlr_target(Python3Grammar ${CMAKE_CURRENT_SOURCE_DIR}/grammar/Python3.g4 OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/managed PACKAGE antlr4 LISTENER VISITOR)

# can be only static because of llvms
add_library(libcodegen ${LIBRARY_MODE}
        ${CMAKE_CURRENT_BINARY_DIR} ${SOURCES} ${ANTLR_Python3Grammar_CXX_OUTPUTS} include/codegen/ExceptionTrace.h)
set_target_properties(libcodegen PROPERTIES PREFIX "")

# find libraries for LLVM components that are intended to be used
#llvm_map_components_to_libnames(llvm_libs support core irreader)
# to get list overview, use llvm-config --components

# list to reduce size of shared object. Compared to linking against all LLVM components, this saves about ~10MB.
#llvm_map_components_to_libnames(llvm_libs core orcjit nativecodegen native scalaropts objcarcopts passes)
llvm_map_components_to_libnames(llvm_libs core native irreader target)
link_directories(${LLVM_LIBRARY_DIRS})
# for minimum JIT these components are recommended:
#    core
#    executionengine
#    native
#    object
#    orcjit
#    runtimedyld
#    support
# this may make it easier but increases size of shared object tremendously
#llvm_map_components_to_libnames(llvm_libs all)

#add_dependencies(libcodegen GeneratePython3Parser)

# use precompiled headers for this target to decrease the cereal template instantiation cost
if(USE_PRECOMPILED_HEADERS)
    file(GLOB_RECURSE VISITORS_HEADERS include/visitors/*.h)
    file(GLOB_RECURSE AST_HEADERS include/ast/*.h)
    file(GLOB_RECURSE SYMBOLS_HEADERS include/symbols/*.h)
    set(CODEGEN_HEADERS ${VISITORS_HEADERS} ${AST_HEADERS} ${SYMBOLS_HEADERS})
    message(STATUS "headers to precompile: ${CODEGEN_HEADERS}")
    target_precompile_headers(libcodegen PUBLIC ${CODEGEN_HEADERS})
endif()
# Specify here the include directories exported
# by this library
target_include_directories(libcodegen PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/managed
        ${LLVM_INCLUDE_DIRS}
        ${Boost_INCLUDE_DIR}
        ${ANTLR4Runtime_INCLUDE_DIR}
        ${PCRE2_INCLUDE_DIRS}
        )

# Declare the library
target_link_libraries(libcodegen
        PUBLIC
        libutils
        PRIVATE
        ${FFI_LIBRARIES}
        ${PCRE2_LIBRARIES}
        ${ANTLR4Runtime_LIB}
        INTERFACE
        ${llvm_libs}
        )