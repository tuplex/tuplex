CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable c11
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

FILE(GLOB SRCS *.cc)
FILE(GLOB PYSRCS ../../python/src/*.cc)

# exclude Boost module bindings
#list(REMOVE_ITEM PYSRCS "../../python/src/PythonBindings.cc")
list(FILTER PYSRCS EXCLUDE REGEX ".*PythonBindings.cc$")

# use pybind11
# fetch pybind11 (external project)
include(FetchContent)
FetchContent_Declare(pybind11 GIT_REPOSITORY https://github.com/pybind/pybind11
        GIT_TAG        v2.9.1)
FetchContent_GetProperties(pybind11)
if(NOT pybind11_POPULATED)
    FetchContent_Populate(pybind11)
    add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
endif()

include(GoogleTest)

ADD_EXECUTABLE(testwrappers ${SRCS} ${PYSRCS})

target_include_directories(testwrappers PRIVATE "../../python/include/" ${Boost_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(testwrappers
        libcore
        ${GTest_LIBRARIES}
        libcpythonadapter
        ${Boost_LIBRARIES}
        pybind11::embed
        )

gtest_add_tests(TARGET testwrappers TEST_PREFIX "")
