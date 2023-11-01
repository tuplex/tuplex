CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
set(CMAKE_CXX_STANDARD 17)

file(GLOB SRCS *.cc)
file(GLOB STDLIB_SRCS stdlib/*.cc)

include(GoogleTest)

add_executable(testcore ${SRCS} ${STDLIB_SRCS})
target_link_libraries(testcore
        libutils
        libio
        libcodegen
        libcore
        libcpythonadapter
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Protobuf_LIBRARIES}
        ${Python3_LIBRARIES})

gtest_add_tests(TARGET testcore TEST_PREFIX "")