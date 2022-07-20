CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
SET(CMAKE_CXX_STANDARD 14)

FILE(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testio ${SRCS} main.cc)

message(STATUS "probutf (testio): ${Protobuf_LIBRARIES} ${Protobuf_LITE_LIBRARIES}")
TARGET_LINK_LIBRARIES(testio
        libio
        libutils
        libcodegen
        libcore
        libcpythonadapter
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES}
        ${Protobuf_LIBRARIES}
        ${Protobuf_LITE_LIBRARIES}
        ${Protobuf_LIBRARIES}
        protobuf::libprotobuf
        )

gtest_add_tests(TARGET testio TEST_PREFIX "")
