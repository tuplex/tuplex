CMAKE_MINIMUM_REQUIRED(VERSION 3.12 FATAL_ERROR)

# enable c++17
SET(CMAKE_CXX_STANDARD 17)

FILE(GLOB SRCS *.cc)

include(GoogleTest)

ADD_EXECUTABLE(testio ${SRCS} main.cc)

TARGET_LINK_LIBRARIES(testio
        libio
        libutils
        libcodegen
        libcore
        libcpythonadapter
        ${GTest_LIBRARIES}
        ${AWSSDK_LINK_LIBRARIES}
        ${Python3_LIBRARIES}
        ${CURSES_LIBRARY}
        )

# Add protobuf dependencies only if required
if(BUILD_WITH_AWS OR BUILD_WITH_ORC)
    TARGET_LINK_LIBRARIES(testio
            ${Protobuf_LIBRARIES}
            ${Protobuf_LITE_LIBRARIES}
            protobuf::libprotobuf
            )
endif()

gtest_add_tests(TARGET testio)
