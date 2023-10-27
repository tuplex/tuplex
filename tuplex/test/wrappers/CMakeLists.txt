CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


FILE(GLOB SRCS *.cc)
FILE(GLOB PYSRCS ../../python/src/*.cc)

# exclude Boost module bindings
#list(REMOVE_ITEM PYSRCS "../../python/src/PythonBindings.cc")
list(FILTER PYSRCS EXCLUDE REGEX ".*PythonBindings.cc$")

## use pybind11
#CPMAddPackage(
#    NAME pybind11
#    VERSION 2.9.1
#    GITHUB_REPOSITORY pybind/pybind11
#    OPTIONS
#        "PYBIND11_NOPYTHON ON"
#        "PYBIND11_FINDPYTHON OFF"
#)

# fetch pybind11 (external project)
#iinclude(FetchContent)
#FetchContent_Declare(pybind11 GIT_REPOSITORY https://github.com/pybind/pybind11
#        GIT_TAG        v2.9.1)
#FetchContent_GetProperties(pybind11)
#if(NOT pybind11_POPULATED)
#    FetchContent_Populate(pybind11)
#    add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
#endif()

include(GoogleTest)

ADD_EXECUTABLE(testwrappers ${SRCS} ${PYSRCS})

target_include_directories(testwrappers PRIVATE "../../python/include/" ${Boost_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(testwrappers
        libcore
        libcodegen
        libutils
        libio
        ${GTest_LIBRARIES}
        libcpythonadapter
        ${Boost_LIBRARIES}
        pybind11::embed
        ${Protobuf_LIBRARIES}
        )

gtest_add_tests(TARGET testwrappers TEST_PREFIX "")
