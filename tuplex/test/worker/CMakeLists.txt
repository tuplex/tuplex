CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
SET(CMAKE_CXX_STANDARD 14)

include(GoogleTest)

ADD_EXECUTABLE(testworker main.cc BasicInvocation.cc nextconf/FlightsQueryTest.cc nextconf/GithubQueryTest.cc nextconf/ServiceRequestsQueryTest.cc)
target_include_directories(testworker PUBLIC "../../worker/include")
# find_package(pybind11 REQUIRED)
TARGET_LINK_LIBRARIES(testworker
        libutils
        libio
        libcodegen
        libcore
        libcpythonadapter
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES}
        pybind11::embed
        )

message(STATUS "Building tests for worker ${WORKER_NAME}")
# mark worker & runtime as dependency so they get build
add_dependencies(testworker ${WORKER_NAME} runtime)

gtest_add_tests(TARGET testworker TEST_PREFIX "")