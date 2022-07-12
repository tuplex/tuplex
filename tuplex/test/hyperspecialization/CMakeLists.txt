CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 14)

file(GLOB SRCS *.cc)

include(GoogleTest)

add_executable(testhyper ${SRCS})
target_link_libraries(testhyper
        libutils
        libio
        libcodegen
        libcore
        libcpythonadapter
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES})

gtest_add_tests(TARGET testhyper TEST_PREFIX "")