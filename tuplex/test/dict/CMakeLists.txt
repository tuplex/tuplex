CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
SET(CMAKE_CXX_STANDARD 14)

FILE(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testdict ${SRCS})

TARGET_LINK_LIBRARIES(testdict
        libutils
        libcodegen
        libcpythonadapter
        libio
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES}
        )

gtest_add_tests(TARGET testdict TEST_PREFIX "")