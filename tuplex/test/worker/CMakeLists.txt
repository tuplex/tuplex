CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
SET(CMAKE_CXX_STANDARD 14)

FILE(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testworker ${SRCS} ../../worker/src/WorkerApp.cc ../../worker/src/LambdaWorkerApp.cc)
target_include_directories(testworker PUBLIC "../../worker/include")
TARGET_LINK_LIBRARIES(testworker
        libio
        libcodegen
        libcore
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        )

message(STATUS "Building tests for worker ${WORKER_NAME}")
# mark worker & runtime as dependency so they get build
add_dependencies(testworker ${WORKER_NAME} runtime)

gtest_add_tests(TARGET testworker TEST_PREFIX "")