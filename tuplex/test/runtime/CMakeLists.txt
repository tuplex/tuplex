CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


file(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testruntime ${SRCS} ../../runtime/src/Runtime.cc ../../runtime/src/StringFunctions.cc)

TARGET_LINK_LIBRARIES(testruntime
        libio
        libcodegen
        libcore
        libutils
        libcpythonadapter
        runtime
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES}
        ${CURSES_LIBRARY}
)

gtest_add_tests(TARGET testruntime TEST_PREFIX "")