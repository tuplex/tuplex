CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++14
SET(CMAKE_CXX_STANDARD 17)

FILE(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testutils ${SRCS})

TARGET_LINK_LIBRARIES(testutils
        libutils
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        )

gtest_add_tests(TARGET testutils TEST_PREFIX "")