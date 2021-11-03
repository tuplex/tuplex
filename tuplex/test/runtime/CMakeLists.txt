CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable c11
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

file(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testruntime ${SRCS})


TARGET_LINK_LIBRARIES(testruntime
        libio
        libcodegen
        libcore
        runtime
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
)

gtest_add_tests(TARGET testruntime TEST_PREFIX "")