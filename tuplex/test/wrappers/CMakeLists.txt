CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

FILE(GLOB SRCS *.cc)
FILE(GLOB PYSRCS ../../python/src/*.cc)

# exclude Boost module bindings
#list(REMOVE_ITEM PYSRCS "../../python/src/PythonBindings.cc")
list(FILTER PYSRCS EXCLUDE REGEX ".*PythonBindings.cc$")

include(GoogleTest)

ADD_EXECUTABLE(testwrappers ${SRCS} ${PYSRCS})

target_include_directories(testwrappers PRIVATE "../../python/include/" ${Boost_INCLUDE_DIR})


# use backward for problematic macos builds
if(APPLE)
    TARGET_LINK_LIBRARIES(testwrappers
            libcore
            libcodegen
            libutils
            libio
            ${GTest_LIBRARIES}
            libcpythonadapter
            ${Boost_LIBRARIES}
            ${CURSES_LIBRARY}
            pybind11::embed
             Backward::Interface
            )
else()
    TARGET_LINK_LIBRARIES(testwrappers
            libcore
            libcodegen
            libutils
            libio
            ${GTest_LIBRARIES}
            libcpythonadapter
            ${Boost_LIBRARIES}
            ${CURSES_LIBRARY}
            pybind11::embed
            )
endif()

gtest_add_tests(TARGET testwrappers TEST_PREFIX "")
